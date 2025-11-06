package com.example.banking.infrastructure.kafka.consumer;

import com.example.banking.infrastructure.kafka.serializer.KafkaEventSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.axonframework.common.jpa.EntityManagerProvider;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventsourcing.eventstore.jpa.DomainEventEntry;
import org.axonframework.serialization.Serializer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import jakarta.persistence.EntityManager;

/**
 * Consumer dedicado para materializar eventos de Kafka a PostgreSQL de forma asíncrona.
 * 
 * Ventajas:
 * - Desacopla la persistencia en Kafka (rápida) de la materialización en PG
 * - Permite procesamiento paralelo con múltiples consumers
 * - Si PG falla, los eventos permanecen en Kafka para reprocesar
 */
@Slf4j
@Component
public class EventMaterializationConsumer {

    private final EntityManagerProvider entityManagerProvider;
    private final KafkaEventSerializer eventSerializer;
    private final Serializer axonSerializer;

    public EventMaterializationConsumer(
            EntityManagerProvider entityManagerProvider,
            KafkaEventSerializer eventSerializer,
            Serializer defaultSerializer) {
        this.entityManagerProvider = entityManagerProvider;
        this.eventSerializer = eventSerializer;
        this.axonSerializer = defaultSerializer;
    }

    /**
     * Consume eventos del tópico y los materializa en PostgreSQL.
     * 
     * Características:
     * - Transaccional: garantiza atomicidad de la escritura
     * - Manual acknowledgment: solo confirma si se persiste exitosamente
     * - Concurrency: múltiples threads procesan en paralelo
     */
    @KafkaListener(
        topics = "${kafka.events.topic}",
        groupId = "${kafka.events.materializer.group-id}",
        concurrency = "${kafka.events.materializer.concurrency:3}",
        containerFactory = "materializerKafkaListenerContainerFactory"
    )
    @Transactional
    public void materializeEvent(
            ConsumerRecord<String, String> record, 
            Acknowledgment acknowledgment) {
        
        try {
            log.debug("Materializando evento - Offset: {}, Partition: {}", 
                record.offset(), record.partition());
            
            // Deserializar evento
            DomainEventMessage<?> event = eventSerializer.deserialize(record.value());
            
            EntityManager em = entityManagerProvider.getEntityManager();
            
            // Verificar si ya existe (idempotencia)
            Long count = em.createQuery(
                "SELECT COUNT(e) FROM DomainEventEntry e " +
                "WHERE e.aggregateIdentifier = :aggId AND e.sequenceNumber = :seq",
                Long.class)
                .setParameter("aggId", event.getAggregateIdentifier())
                .setParameter("seq", event.getSequenceNumber())
                .getSingleResult();
            
            if (count > 0) {
                log.debug("Evento ya materializado (idempotente): {} seq={}", 
                    event.getAggregateIdentifier(), event.getSequenceNumber());
                acknowledgment.acknowledge();
                return;
            }
            
            // Persistir en PostgreSQL
            DomainEventEntry entry = new DomainEventEntry(event, axonSerializer);
            em.persist(entry);
            em.flush();
            
            log.debug("Evento materializado en PG: aggregate={}, seq={}", 
                event.getAggregateIdentifier(), event.getSequenceNumber());
            
            // Confirmar procesamiento
            acknowledgment.acknowledge();
            
        } catch (Exception e) {
            log.error("Error materializando evento - NO se hace acknowledge para reintentar", e);
            // NO hacer acknowledge para que Kafka reintente
            throw new RuntimeException("Error en materialización", e);
        }
    }
}