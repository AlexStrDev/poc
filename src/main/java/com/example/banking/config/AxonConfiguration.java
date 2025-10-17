package com.example.banking.config;

import com.example.banking.infrastructure.kafka.CommandSerializer;
import com.example.banking.infrastructure.kafka.KafkaCommandBus;
import com.example.banking.infrastructure.kafka.event.KafkaEventSerializer;
import com.example.banking.infrastructure.kafka.event.KafkaEventStorageEngine;
import com.example.banking.infrastructure.kafka.event.KafkaEventStore;
import org.axonframework.commandhandling.CommandBus;
import org.axonframework.commandhandling.SimpleCommandBus;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.commandhandling.gateway.DefaultCommandGateway;
import org.axonframework.config.EventProcessingConfigurer;
import org.axonframework.eventsourcing.eventstore.EmbeddedEventStore;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Configuración de Axon Framework con EventStore basado en Kafka
 */
@Configuration
public class AxonConfiguration {

    @Value("${kafka.events.topic:axon-events}")
    private String eventsTopic;
    
    @Value("${kafka.snapshots.topic:axon-snapshots}")
    private String snapshotsTopic;

    /**
     * Crea el segmento local del CommandBus
     */
    @Bean
    public CommandBus localCommandBus() {
        return SimpleCommandBus.builder().build();
    }

    /**
     * Crea nuestro KafkaCommandBus personalizado
     */
    @Bean
    @Primary
    public KafkaCommandBus kafkaCommandBus(
            CommandBus localCommandBus,
            KafkaTemplate<String, String> kafkaTemplate,
            CommandSerializer commandSerializer) {
        return new KafkaCommandBus(localCommandBus, kafkaTemplate, commandSerializer);
    }

    /**
     * CommandGateway que usa nuestro KafkaCommandBus
     */
    @Bean
    @Primary
    public CommandGateway commandGateway(KafkaCommandBus kafkaCommandBus) {
        return DefaultCommandGateway.builder()
                .commandBus(kafkaCommandBus)
                .build();
    }

    /**
     * EventStore con Kafka como backend
     * 
     * Características:
     * - Events Topic (log-based) como source of truth
     * - Partitioning por aggregateId para garantizar orden
     * - State Store como caché para lecturas rápidas
     * - Snapshot Topic para optimización
     * - Versioning explícito para detectar conflictos
     */
    @Bean
    public EventStore eventStore(
            KafkaTemplate<String, String> kafkaTemplate,
            KafkaEventSerializer kafkaEventSerializer,
            KafkaEventStore kafkaEventStore) {
        
        KafkaEventStorageEngine storageEngine = new KafkaEventStorageEngine(
            kafkaTemplate,
            kafkaEventSerializer,
            kafkaEventStore,
            eventsTopic,
            snapshotsTopic
        );
        
        return EmbeddedEventStore.builder()
                .storageEngine(storageEngine)
                .build();
    }

    /**
     * Configuración del procesamiento de eventos
     * 
     * CAMBIO IMPORTANTE:
     * - Usamos SUBSCRIBING processors en lugar de TRACKING
     * - Los Subscribing processors son más apropiados para integraciones con Kafka
     * - Kafka ya maneja el tracking de offsets
     * - No requieren TrackingToken implementation
     */
    @org.springframework.beans.factory.annotation.Autowired
    public void configureEventProcessing(EventProcessingConfigurer configurer) {
        // Cambiar a Subscribing Event Processors para todos los procesadores
        configurer.usingSubscribingEventProcessors();
        
        // Opcionalmente, puedes configurar procesadores específicos:
        // configurer.registerSubscribingEventProcessor("com.example.banking.query.projection");
        // configurer.registerSubscribingEventProcessor("com.example.banking.infrastructure.logging");
    }
}