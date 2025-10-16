package com.example.banking.infrastructure.kafka.event;

import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.eventsourcing.eventstore.DomainEventStream;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Implementación de EventStorageEngine que usa Kafka como backend.
 * 
 * Características:
 * - Events Topic (log-based) como source of truth
 * - Partitioning por aggregateId para garantizar orden
 * - State Store como caché para lecturas rápidas
 * - Snapshot Topic para optimización
 * - Versioning explícito para detectar conflictos
 */
public class KafkaEventStorageEngine implements EventStorageEngine {

    private static final Logger logger = LoggerFactory.getLogger(KafkaEventStorageEngine.class);
    
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaEventSerializer eventSerializer;
    private final KafkaEventStore eventStore;
    private final String eventsTopic;
    private final String snapshotsTopic;

    public KafkaEventStorageEngine(
            KafkaTemplate<String, String> kafkaTemplate,
            KafkaEventSerializer eventSerializer,
            KafkaEventStore eventStore,
            String eventsTopic,
            String snapshotsTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.eventSerializer = eventSerializer;
        this.eventStore = eventStore;
        this.eventsTopic = eventsTopic;
        this.snapshotsTopic = snapshotsTopic;
    }

    @Override
    public void appendEvents(List<? extends EventMessage<?>> events) {
        if (events == null || events.isEmpty()) {
            return;
        }

        for (EventMessage<?> event : events) {
            if (event instanceof DomainEventMessage) {
                appendDomainEvent((DomainEventMessage<?>) event);
            }
        }
    }

    /**
     * Agrega un evento de dominio al topic de Kafka
     */
    private void appendDomainEvent(DomainEventMessage<?> eventMessage) {
        try {
            String aggregateId = eventMessage.getAggregateIdentifier();
            long expectedSequence = eventMessage.getSequenceNumber();
            
            // Verificación optimista de concurrencia
            Long currentSequence = eventStore.getLastSequenceNumber(aggregateId);
            if (currentSequence != null && expectedSequence != currentSequence + 1) {
                throw new org.axonframework.eventsourcing.eventstore.EventStoreException(
                    String.format("Concurrency conflict: expected sequence %d but found %d for aggregate %s",
                        expectedSequence, currentSequence + 1, aggregateId)
                );
            }
            
            // Serializar el evento
            AxonEventMessage axonEvent = eventSerializer.serialize(eventMessage);
            String jsonPayload = eventSerializer.serializeToJson(axonEvent);
            
            // Publicar a Kafka usando aggregateId como key para garantizar particionamiento
            kafkaTemplate.send(eventsTopic, aggregateId, jsonPayload)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        logger.error("Error publishing event to Kafka for aggregate {}", aggregateId, ex);
                    } else {
                        logger.debug("Published event seq={} for aggregate {} to partition {}", 
                            expectedSequence, 
                            aggregateId, 
                            result.getRecordMetadata().partition());
                        
                        // Actualizar el state store local
                        eventStore.storeEvent(aggregateId, axonEvent);
                    }
                });
            
        } catch (Exception e) {
            logger.error("Error appending event", e);
            throw new org.axonframework.eventsourcing.eventstore.EventStoreException(
                "Failed to append event", e);
        }
    }

    @Override
    public void storeSnapshot(DomainEventMessage<?> snapshot) {
        try {
            String aggregateId = snapshot.getAggregateIdentifier();
            
            logger.info("Storing snapshot for aggregate {} at sequence {}", 
                aggregateId, snapshot.getSequenceNumber());
            
            // Serializar el snapshot
            AxonEventMessage axonSnapshot = eventSerializer.serialize(snapshot);
            String jsonPayload = eventSerializer.serializeToJson(axonSnapshot);
            
            // Publicar al topic de snapshots
            kafkaTemplate.send(snapshotsTopic, aggregateId, jsonPayload)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        logger.error("Error storing snapshot for aggregate {}", aggregateId, ex);
                    } else {
                        logger.debug("Stored snapshot for aggregate {} to partition {}", 
                            aggregateId, 
                            result.getRecordMetadata().partition());
                        
                        // Actualizar el state store
                        eventStore.storeSnapshot(aggregateId, axonSnapshot);
                    }
                });
            
        } catch (Exception e) {
            logger.error("Error storing snapshot", e);
            throw new org.axonframework.eventsourcing.eventstore.EventStoreException(
                "Failed to store snapshot", e);
        }
    }

    @Override
    public DomainEventStream readEvents(String aggregateIdentifier) {
        logger.debug("Reading events for aggregate: {}", aggregateIdentifier);
        
        // Primero intentar obtener del state store (caché)
        List<AxonEventMessage> events = eventStore.readEvents(aggregateIdentifier);
        
        if (events.isEmpty()) {
            logger.warn("No events found for aggregate: {}", aggregateIdentifier);
            return DomainEventStream.empty();
        }
        
        // Convertir AxonEventMessage a DomainEventMessage
        Stream<DomainEventMessage<?>> eventStream = events.stream()
            .map(this::toDomainEventMessage);
        
        return DomainEventStream.of(eventStream);
    }

    /**
     * Lee eventos desde un número de secuencia específico
     */
    @Override
    public DomainEventStream readEvents(String aggregateIdentifier, long firstSequenceNumber) {
        logger.debug("Reading events for aggregate: {} from sequence: {}", 
            aggregateIdentifier, firstSequenceNumber);
        
        List<AxonEventMessage> events = eventStore.readEvents(aggregateIdentifier);
        
        // Filtrar eventos desde el número de secuencia solicitado
        Stream<DomainEventMessage<?>> eventStream = events.stream()
            .filter(e -> e.getSequenceNumber() >= firstSequenceNumber)
            .map(this::toDomainEventMessage);
        
        return DomainEventStream.of(eventStream);
    }

    @Override
    public Optional<DomainEventMessage<?>> readSnapshot(String aggregateIdentifier) {
        logger.debug("Reading snapshot for aggregate: {}", aggregateIdentifier);
        
        Optional<AxonEventMessage> snapshot = eventStore.readSnapshot(aggregateIdentifier);
        
        // Convertir AxonEventMessage a DomainEventMessage
        return snapshot.map(this::toDomainEventMessage);
    }

    /**
     * Convierte AxonEventMessage a DomainEventMessage para Axon Framework
     */
    private DomainEventMessage<?> toDomainEventMessage(AxonEventMessage axonEvent) {
        try {
            Object payload = eventSerializer.deserializePayload(axonEvent);
            
            return new org.axonframework.eventhandling.GenericDomainEventMessage<>(
                axonEvent.getAggregateType(),
                axonEvent.getAggregateIdentifier(),
                axonEvent.getSequenceNumber(),
                payload,
                org.axonframework.messaging.MetaData.from(axonEvent.getMetaData()),
                axonEvent.getEventIdentifier(),
                Instant.ofEpochMilli(axonEvent.getTimestamp())
            );
        } catch (Exception e) {
            throw new org.axonframework.eventsourcing.eventstore.EventStoreException(
                "Failed to deserialize event", e);
        }
    }

    // Método no implementado para event streaming (no usado por ahora)
    @Override
    public Stream<? extends TrackedEventMessage<?>> readEvents(TrackingToken trackingToken, boolean mayBlock) {
        throw new UnsupportedOperationException("Event streaming not implemented yet. Use DomainEventStream instead.");
    }
}