package com.example.banking.infrastructure.axon.config;

import com.example.banking.infrastructure.kafka.bus.KafkaEventBus;
import com.example.banking.infrastructure.kafka.gateway.KafkaCommandGateway;
import com.example.banking.infrastructure.kafka.storage.EventStoreMaterializer;
import com.example.banking.infrastructure.kafka.storage.KafkaEventStorageEngine;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.commandhandling.CommandBus;
import org.axonframework.commandhandling.SimpleCommandBus;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.common.jpa.EntityManagerProvider;
import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.config.EventProcessingConfigurer;
import org.axonframework.eventsourcing.EventSourcingRepository;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.messaging.interceptors.BeanValidationInterceptor;
import org.axonframework.messaging.interceptors.LoggingInterceptor;
import org.axonframework.modelling.command.Repository;
import org.axonframework.serialization.Serializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * Configuración de Axon Framework con:
 * - CommandBus usando Kafka
 * - EventStore híbrido (Kafka source of truth + PostgreSQL cache lazy-load)
 */
@Slf4j
@Configuration
public class AxonConfig {

    @Bean
    @Primary
    public CommandBus localCommandBus() {
        SimpleCommandBus commandBus = SimpleCommandBus.builder().build();
        
        commandBus.registerDispatchInterceptor(new BeanValidationInterceptor<>());
        commandBus.registerDispatchInterceptor(new LoggingInterceptor<>());
        commandBus.registerHandlerInterceptor(new LoggingInterceptor<>());
        
        log.info("CommandBus local configurado");
        return commandBus;
    }

    @Bean
    @Primary
    public CommandGateway commandGateway(KafkaCommandGateway kafkaCommandGateway) {
        log.info("Usando KafkaCommandGateway como CommandGateway principal");
        return kafkaCommandGateway;
    }

    /**
     * EventStorageEngine híbrido: Kafka (source of truth) + PostgreSQL (cache lazy-load)
     */
    @Bean
    public KafkaEventStorageEngine eventStorageEngine(
            Serializer defaultSerializer,
            EntityManagerProvider entityManagerProvider,
            TransactionManager transactionManager,
            KafkaEventBus kafkaEventBus,
            EventStoreMaterializer materializer) {
        
        log.info("Configurando KafkaEventStorageEngine (Kafka source of truth + PG lazy-load)");
        
        return KafkaEventStorageEngine.builder()
                .snapshotSerializer(defaultSerializer)
                .eventSerializer(defaultSerializer)
                .entityManagerProvider(entityManagerProvider)
                .transactionManager(transactionManager)
                .kafkaEventBus(kafkaEventBus)
                .materializer(materializer)
                .build();
    }

    @Autowired
    public void configureEventProcessing(EventProcessingConfigurer configurer) {
        configurer.registerDefaultListenerInvocationErrorHandler(
                configuration -> (exception, event, eventHandler) -> {
                    log.error("Error procesando evento: {}", event, exception);
                }
        );
        
        log.info("Configuración de procesamiento de eventos completada");
    }

    @Bean
    public Repository<com.example.banking.aggregate.BankAccountAggregate> bankAccountRepository(
            EventStore eventStore) {
        
        return EventSourcingRepository.builder(com.example.banking.aggregate.BankAccountAggregate.class)
                .eventStore(eventStore)
                .build();
    }
}