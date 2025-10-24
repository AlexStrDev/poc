package com.example.banking.infrastructure.axon.config;

import com.example.banking.infrastructure.kafka.gateway.KafkaCommandGateway;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.commandhandling.CommandBus;
import org.axonframework.commandhandling.SimpleCommandBus;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.common.jpa.EntityManagerProvider;
import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.config.EventProcessingConfigurer;
import org.axonframework.eventsourcing.EventSourcingRepository;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.jpa.JpaEventStorageEngine;
import org.axonframework.messaging.interceptors.BeanValidationInterceptor;
import org.axonframework.messaging.interceptors.LoggingInterceptor;
import org.axonframework.modelling.command.Repository;
import org.axonframework.serialization.Serializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * Configuración de Axon Framework con CommandBus personalizado usando Kafka
 * y EventStore con PostgreSQL
 */
@Slf4j
@Configuration
public class AxonConfig {

    /**
     * CommandBus local que procesa los comandos en esta instancia
     */
    @Bean
    @Primary
    public CommandBus localCommandBus() {
        SimpleCommandBus commandBus = SimpleCommandBus.builder().build();
        
        // Registrar interceptores
        commandBus.registerDispatchInterceptor(new BeanValidationInterceptor<>());
        commandBus.registerDispatchInterceptor(new LoggingInterceptor<>());
        commandBus.registerHandlerInterceptor(new LoggingInterceptor<>());
        
        log.info("CommandBus local configurado con interceptores");
        return commandBus;
    }

    /**
     * CommandGateway principal que usa Kafka para enviar comandos
     */
    @Bean
    @Primary
    public CommandGateway commandGateway(KafkaCommandGateway kafkaCommandGateway) {
        log.info("Usando KafkaCommandGateway como CommandGateway principal");
        return kafkaCommandGateway;
    }

    /**
     * EventStorageEngine con JPA y PostgreSQL
     */
    @Bean
    public JpaEventStorageEngine eventStorageEngine(
            Serializer serializer,
            EntityManagerProvider entityManagerProvider,
            TransactionManager transactionManager) {
        
        log.info("Configurando JpaEventStorageEngine con PostgreSQL");
        
        return JpaEventStorageEngine.builder()
                .snapshotSerializer(serializer)
                .eventSerializer(serializer)
                .entityManagerProvider(entityManagerProvider)
                .transactionManager(transactionManager)
                .build();
    }

    /**
     * Configuración de procesamiento de eventos
     */
    @Autowired
    public void configureEventProcessing(EventProcessingConfigurer configurer) {
        // Configurar procesadores de eventos si es necesario
        configurer.registerDefaultListenerInvocationErrorHandler(
                configuration -> (exception, event, eventHandler) -> {
                    log.error("Error procesando evento: {}", event, exception);
                }
        );
        
        log.info("Configuración de procesamiento de eventos completada");
    }

    /**
     * Repository para BankAccountAggregate
     */
    @Bean
    public Repository<com.example.banking.aggregate.BankAccountAggregate> bankAccountRepository(
            EventStore eventStore) {
        
        return EventSourcingRepository.builder(com.example.banking.aggregate.BankAccountAggregate.class)
                .eventStore(eventStore)
                .build();
    }
}