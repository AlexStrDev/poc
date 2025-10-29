package com.example.banking.infrastructure.kafka.storage;

import com.example.banking.infrastructure.kafka.bus.KafkaEventBus;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.common.jpa.EntityManagerProvider;
import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventsourcing.eventstore.jpa.JpaEventStorageEngine;
import org.axonframework.serialization.Serializer;

import java.util.List;

/**
 * EventStorageEngine híbrido:
 * - Persiste eventos en PostgreSQL usando JPA
 * - Publica eventos en Kafka para procesamiento distribuido
 */
@Slf4j
public class KafkaEventStorageEngine extends JpaEventStorageEngine {

    private final KafkaEventBus kafkaEventBus;

    protected KafkaEventStorageEngine(Builder builder) {
        super(builder);
        this.kafkaEventBus = builder.kafkaEventBus;
    }

    @Override
    protected void appendEvents(List<? extends EventMessage<?>> events, Serializer serializer) {
        log.debug("Guardando {} eventos en PostgreSQL", events.size());
        
        // 1. Persistir en PostgreSQL
        super.appendEvents(events, serializer);
        
        // 2. Publicar en Kafka
        events.stream()
            .filter(event -> event instanceof DomainEventMessage)
            .map(event -> (DomainEventMessage<?>) event)
            .forEach(event -> {
                try {
                    kafkaEventBus.publish(event);
                    log.debug("Evento publicado en Kafka: {}", event.getPayloadType().getSimpleName());
                } catch (Exception e) {
                    log.error("Error publicando evento en Kafka (ya persistido en DB)", e);
                }
            });
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

        @Override
        public KafkaEventStorageEngine build() {
            return new KafkaEventStorageEngine(this);
        }
    }
}