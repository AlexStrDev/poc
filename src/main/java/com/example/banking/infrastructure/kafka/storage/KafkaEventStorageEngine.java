package com.example.banking.infrastructure.kafka.storage;

import com.example.banking.infrastructure.kafka.bus.KafkaEventBus;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.common.jpa.EntityManagerProvider;
import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.eventhandling.DomainEventData;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventsourcing.eventstore.jpa.JpaEventStorageEngine;
import org.axonframework.serialization.Serializer;

import java.util.List;
import java.util.stream.Stream;

/**
 * EventStorageEngine híbrido con Kafka como source of truth:
 * - Kafka: Event store principal (durabilidad)
 * - PostgreSQL: Cache materializado (performance, lazy-load)
 * 
 * Flujo:
 * 1. appendEvents() → Kafka PRIMERO → PostgreSQL después (async)
 * 2. readEventData() → Verificar PG → Si no existe, reconstruir desde Kafka
 */
@Slf4j
public class KafkaEventStorageEngine extends JpaEventStorageEngine {

    private final KafkaEventBus kafkaEventBus;
    private final EventStoreMaterializer materializer;

    protected KafkaEventStorageEngine(Builder builder) {
        super(builder);
        this.kafkaEventBus = builder.kafkaEventBus;
        this.materializer = builder.materializer;
    }

    @Override
    protected void appendEvents(List<? extends EventMessage<?>> events, Serializer serializer) {
        log.debug("Persistiendo {} eventos - Kafka PRIMERO", events.size());
        
        events.stream()
            .filter(event -> event instanceof DomainEventMessage)
            .map(event -> (DomainEventMessage<?>) event)
            .forEach(event -> {
                try {
                    kafkaEventBus.publish(event);
                    log.debug("Evento publicado en Kafka (source of truth): {}", 
                        event.getPayloadType().getSimpleName());
                } catch (Exception e) {
                    log.error("CRÍTICO: Error publicando evento a Kafka", e);
                    throw new RuntimeException("No se pudo persistir en Kafka (source of truth)", e);
                }
            });
        
        try {
            super.appendEvents(events, serializer);
            
            if (!events.isEmpty() && events.get(0) instanceof DomainEventMessage) {
                DomainEventMessage<?> firstEvent = (DomainEventMessage<?>) events.get(0);
                materializer.markAsMaterialized(firstEvent.getAggregateIdentifier());
            }
            
            log.debug("Eventos persistidos en PostgreSQL (cache)");
        } catch (Exception e) {
            log.warn("Error persistiendo en PostgreSQL (no crítico, está en Kafka): {}", e.getMessage());
        }
    }

    @Override
    protected Stream<? extends DomainEventData<?>> readEventData(String aggregateIdentifier, long firstSequenceNumber) {
        log.debug("Leyendo eventos para aggregate: {}", aggregateIdentifier);
        
        if (materializer.isMaterialized(aggregateIdentifier)) {
            log.debug("Aggregate {} encontrado en PG (cache hit)", aggregateIdentifier);
            return super.readEventData(aggregateIdentifier, firstSequenceNumber);
        }
        
        log.info("Aggregate {} NO encontrado en PG - Reconstruyendo desde Kafka (lazy-load)", 
            aggregateIdentifier);
        
        try {
            materializer.materializeFromKafka(aggregateIdentifier);
            
            return super.readEventData(aggregateIdentifier, firstSequenceNumber);
            
        } catch (Exception e) {
            log.error("Error materializando aggregate desde Kafka", e);
            throw new RuntimeException("No se pudo reconstruir aggregate desde Kafka", e);
        }
    }

    @Override
    protected void storeSnapshot(DomainEventMessage<?> snapshot, Serializer serializer) {
        log.debug("Guardando snapshot para agregado: {}", snapshot.getAggregateIdentifier());
        super.storeSnapshot(snapshot, serializer);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends JpaEventStorageEngine.Builder {
        private KafkaEventBus kafkaEventBus;
        private EventStoreMaterializer materializer;

        @Override
        public Builder snapshotSerializer(Serializer snapshotSerializer) {
            super.snapshotSerializer(snapshotSerializer);
            return this;
        }

        @Override
        public Builder eventSerializer(Serializer eventSerializer) {
            super.eventSerializer(eventSerializer);
            return this;
        }

        @Override
        public Builder upcasterChain(org.axonframework.serialization.upcasting.event.EventUpcaster upcasterChain) {
            super.upcasterChain(upcasterChain);
            return this;
        }

        @Override
        public Builder persistenceExceptionResolver(org.axonframework.common.jdbc.PersistenceExceptionResolver persistenceExceptionResolver) {
            super.persistenceExceptionResolver(persistenceExceptionResolver);
            return this;
        }

        @Override
        public Builder entityManagerProvider(EntityManagerProvider entityManagerProvider) {
            super.entityManagerProvider(entityManagerProvider);
            return this;
        }

        @Override
        public Builder transactionManager(TransactionManager transactionManager) {
            super.transactionManager(transactionManager);
            return this;
        }

        @Override
        public Builder batchSize(int batchSize) {
            super.batchSize(batchSize);
            return this;
        }

        public Builder kafkaEventBus(KafkaEventBus kafkaEventBus) {
            this.kafkaEventBus = kafkaEventBus;
            return this;
        }

        public Builder materializer(EventStoreMaterializer materializer) {
            this.materializer = materializer;
            return this;
        }

        @Override
        public KafkaEventStorageEngine build() {
            return new KafkaEventStorageEngine(this);
        }
    }
}