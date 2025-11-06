package com.example.banking.infrastructure.axon.config;

import com.example.banking.infrastructure.kafka.bus.KafkaEventBus;
import com.example.banking.infrastructure.kafka.gateway.KafkaCommandGateway;
import com.example.banking.infrastructure.kafka.storage.EventStoreMaterializer;
import com.example.banking.infrastructure.kafka.storage.KafkaEventStorageEngine;
import com.example.banking.infrastructure.lock.DistributedLockService;
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
 * Configuración mejorada de Axon Framework con:
 * 
 * ✅ CommandBus usando Kafka (sin Axon Server)
 * ✅ EventStore híbrido mejorado:
 *    - Kafka como source of truth (escritura rápida)
 *    - PostgreSQL como cache (materialización asíncrona)
 * ✅ Lock distribuido para prevenir condiciones de carrera
 * ✅ Idempotencia en comandos
 */
@Slf4j
@Configuration
public class AxonConfig {

    /**
     * CommandBus local para procesamiento interno
     */
    @Bean
    @Primary
    public CommandBus localCommandBus() {
        SimpleCommandBus commandBus = SimpleCommandBus.builder().build();
        
        // Interceptors para validación y logging
        commandBus.registerDispatchInterceptor(new BeanValidationInterceptor<>());
        commandBus.registerDispatchInterceptor(new LoggingInterceptor<>());
        commandBus.registerHandlerInterceptor(new LoggingInterceptor<>());
        
        log.info("✅ CommandBus local configurado con interceptors");
        return commandBus;
    }

    /**
     * CommandGateway usando Kafka (reemplaza Axon Server)
     */
    @Bean
    @Primary
    public CommandGateway commandGateway(KafkaCommandGateway kafkaCommandGateway) {
        log.info("✅ Usando KafkaCommandGateway como CommandGateway principal");
        return kafkaCommandGateway;
    }

    /**
     * EventStorageEngine híbrido mejorado:
     * - Kafka: Source of truth (escritura ÚNICAMENTE)
     * - PostgreSQL: Cache lazy-load (materialización asíncrona)
     */
    @Bean
    public KafkaEventStorageEngine eventStorageEngine(
            Serializer defaultSerializer,
            EntityManagerProvider entityManagerProvider,
            TransactionManager transactionManager,
            KafkaEventBus kafkaEventBus,
            EventStoreMaterializer materializer,
            DistributedLockService lockService) {
        
        log.info("🔧 Configurando KafkaEventStorageEngine híbrido mejorado:");
        log.info("   📝 Escritura: Kafka ÚNICAMENTE (source of truth)");
        log.info("   📖 Lectura: PostgreSQL con lazy-load desde Kafka");
        log.info("   🔒 Lock distribuido: Previene materialización concurrente");
        
        return KafkaEventStorageEngine.builder()
                .snapshotSerializer(defaultSerializer)
                .eventSerializer(defaultSerializer)
                .entityManagerProvider(entityManagerProvider)
                .transactionManager(transactionManager)
                .kafkaEventBus(kafkaEventBus)
                .materializer(materializer)
                .lockService(lockService)
                .build();
    }

    /**
     * Configuración de procesamiento de eventos
     */
    @Autowired
    public void configureEventProcessing(EventProcessingConfigurer configurer) {
        configurer.registerDefaultListenerInvocationErrorHandler(
                configuration -> (exception, event, eventHandler) -> {
                    log.error("❌ Error procesando evento: {}", event, exception);
                }
        );
        
        log.info("✅ Configuración de procesamiento de eventos completada");
    }

    /**
     * Repository para BankAccountAggregate
     */
    @Bean
    public Repository<com.example.banking.aggregate.BankAccountAggregate> bankAccountRepository(
            EventStore eventStore) {
        
        log.info("✅ Configurando repository para BankAccountAggregate");
        
        return EventSourcingRepository.builder(com.example.banking.aggregate.BankAccountAggregate.class)
                .eventStore(eventStore)
                .build();
    }
}