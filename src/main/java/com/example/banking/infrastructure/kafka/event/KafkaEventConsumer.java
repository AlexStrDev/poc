package com.example.banking.infrastructure.kafka.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Consumidor de eventos de Kafka que puebla el State Store local.
 * 
 * Este consumer reconstruye el estado local leyendo todos los eventos
 * del tópico de eventos al iniciar, y luego mantiene el estado actualizado
 * escuchando nuevos eventos.
 */
@Component
public class KafkaEventConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaEventConsumer.class);
    
    private final KafkaEventStore eventStore;
    private final KafkaEventSerializer eventSerializer;

    public KafkaEventConsumer(KafkaEventStore eventStore, KafkaEventSerializer eventSerializer) {
        this.eventStore = eventStore;
        this.eventSerializer = eventSerializer;
    }

    /**
     * Consume eventos del tópico de eventos
     * Usa auto.offset.reset=earliest para leer desde el principio al iniciar
     */
    @KafkaListener(
        topics = "${kafka.events.topic:axon-events}",
        groupId = "${kafka.events.group-id:banking-event-store}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeEvent(
            @Payload String jsonPayload,
            @Header(KafkaHeaders.RECEIVED_KEY) String aggregateId,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {
        
        try {
            logger.debug("Received event for aggregate {} from partition {} offset {}", 
                aggregateId, partition, offset);
            
            // Deserializar el evento
            AxonEventMessage axonEvent = eventSerializer.deserializeFromJson(jsonPayload);
            
            // Almacenar en el state store local
            eventStore.storeEvent(aggregateId, axonEvent);
            
            logger.debug("Stored event seq={} for aggregate {} in local store", 
                axonEvent.getSequenceNumber(), aggregateId);
            
        } catch (Exception e) {
            logger.error("Error consuming event for aggregate {}: {}", 
                aggregateId, e.getMessage(), e);
        }
    }

    /**
     * Consume snapshots del tópico de snapshots
     */
    @KafkaListener(
        topics = "${kafka.snapshots.topic:axon-snapshots}",
        groupId = "${kafka.snapshots.group-id:banking-snapshot-store}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeSnapshot(
            @Payload String jsonPayload,
            @Header(KafkaHeaders.RECEIVED_KEY) String aggregateId,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {
        
        try {
            logger.debug("Received snapshot for aggregate {} from partition {} offset {}", 
                aggregateId, partition, offset);
            
            // Deserializar el snapshot
            AxonEventMessage axonSnapshot = eventSerializer.deserializeFromJson(jsonPayload);
            
            // Almacenar en el state store local
            eventStore.storeSnapshot(aggregateId, axonSnapshot);
            
            logger.info("Stored snapshot for aggregate {} at sequence {}", 
                aggregateId, axonSnapshot.getSequenceNumber());
            
        } catch (Exception e) {
            logger.error("Error consuming snapshot for aggregate {}: {}", 
                aggregateId, e.getMessage(), e);
        }
    }

    /**
     * Muestra estadísticas al iniciar la aplicación
     */
    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        // Dar tiempo para que el consumer se conecte y consuma
        new Thread(() -> {
            try {
                Thread.sleep(2000);
                Map<String, Object> stats = eventStore.getStatistics();
                logger.info("Event Store initialized: {}", stats);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).start();
    }
}