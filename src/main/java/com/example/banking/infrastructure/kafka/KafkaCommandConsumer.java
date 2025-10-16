package com.example.banking.infrastructure.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Consumidor de Kafka que escucha comandos entrantes
 * y los despacha al KafkaCommandBus para su procesamiento.
 * Replica el comportamiento del CommandProcessingTask de Axon Server.
 */
@Component
public class KafkaCommandConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaCommandConsumer.class);
    
    private final KafkaCommandBus commandBus;
    private final ExecutorService executorService;

    public KafkaCommandConsumer(KafkaCommandBus commandBus) {
        this.commandBus = commandBus;
        // Pool de threads para procesar comandos en paralelo
        // Similar al ExecutorService usado por AxonServerCommandBus
        this.executorService = Executors.newFixedThreadPool(10);
    }

    /**
     * Escucha comandos del tópico de Kafka
     * Similar al método run() de CommandProcessingTask en Axon Server
     */
    @KafkaListener(
        topics = "${kafka.command.topic:axon-commands}",
        groupId = "${kafka.command.group-id:banking-command-handlers}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeCommand(
            @Payload String jsonPayload,
            @Header(KafkaHeaders.RECEIVED_KEY) String commandName,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {
        
        logger.debug("Received command [{}] from topic [{}] partition [{}] offset [{}]", 
            commandName, topic, partition, offset);
        
        // Procesar el comando en un thread del pool
        // Esto permite procesamiento paralelo de comandos, similar a Axon Server
        executorService.submit(() -> processCommand(jsonPayload, commandName));
    }

    /**
     * Procesa el comando deserializándolo y ejecutándolo
     * Este método es equivalente al run() de CommandProcessingTask
     */
    private void processCommand(String jsonPayload, String commandName) {
        try {
            logger.debug("Processing command: {}", commandName);
            
            // Despachar al command bus para ejecución
            // El KafkaCommandBus deserializará el mensaje completo
            // y reconstruirá el CommandMessage original
            commandBus.handleIncomingCommand(jsonPayload);
            
        } catch (Exception e) {
            logger.error("Error processing command [{}]: {}", commandName, e.getMessage(), e);
            // Aquí podrías implementar retry logic o dead letter queue
        }
    }

    /**
     * Limpieza al destruir el bean
     */
    public void shutdown() {
        logger.info("Shutting down command consumer executor service");
        executorService.shutdown();
    }
}