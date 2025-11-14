package com.example.banking.infrastructure.kafka.bus;

import com.example.banking.infrastructure.cache.CommandDeduplicationService;
import com.example.banking.infrastructure.kafka.handler.CommandReplyHandler;
import com.example.banking.infrastructure.kafka.serializer.CommandSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.axonframework.commandhandling.CommandBus;
import org.axonframework.commandhandling.CommandCallback;
import org.axonframework.commandhandling.CommandMessage;
import org.axonframework.commandhandling.GenericCommandResultMessage;
import org.axonframework.messaging.MessageHandler;
import org.axonframework.messaging.MessageHandlerInterceptor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * CommandBus mejorado que consume comandos desde Kafka con:
 * 
 * ✅ Idempotencia: Previene procesamiento duplicado
 * ✅ DLQ: Envía comandos fallidos a Dead Letter Queue
 * ✅ Reply Pattern: Envía respuestas al tópico de respuestas
 * ✅ Metrics: Tracking de comandos procesados/fallidos
 */
@Slf4j
@Component
public class KafkaCommandBus {

    private final CommandBus localCommandBus;
    private final CommandSerializer commandSerializer;
    private final CommandDeduplicationService deduplicationService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final Map<String, List<MessageHandler<? super CommandMessage<?>>>> handlers;
    private final List<MessageHandlerInterceptor<? super CommandMessage<?>>> handlerInterceptors;
    
    private final String dlqTopic;
    private final String replyTopic;

    // Métricas
    private long processedCommands = 0;
    private long failedCommands = 0;
    private long duplicateCommands = 0;

    public KafkaCommandBus(
            CommandBus localCommandBus,
            CommandSerializer commandSerializer,
            CommandDeduplicationService deduplicationService,
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${kafka.command.dlq.topic}") String dlqTopic,
            @Value("${kafka.command.reply.topic}") String replyTopic) {
        
        this.localCommandBus = localCommandBus;
        this.commandSerializer = commandSerializer;
        this.deduplicationService = deduplicationService;
        this.kafkaTemplate = kafkaTemplate;
        this.dlqTopic = dlqTopic;
        this.replyTopic = replyTopic;
        this.handlers = new ConcurrentHashMap<>();
        this.handlerInterceptors = new CopyOnWriteArrayList<>();
    }

    /**
     * Registra un handler para un tipo específico de comando.
     */
    public org.axonframework.common.Registration subscribe(
            @Nonnull String commandName,
            @Nonnull MessageHandler<? super CommandMessage<?>> handler) {
        
        log.info("Registrando handler para comando: {}", commandName);
        
        handlers.computeIfAbsent(commandName, k -> new CopyOnWriteArrayList<>()).add(handler);
        
        org.axonframework.common.Registration localRegistration = 
            localCommandBus.subscribe(commandName, handler);
        
        return () -> {
            List<MessageHandler<? super CommandMessage<?>>> commandHandlers = handlers.get(commandName);
            if (commandHandlers != null) {
                commandHandlers.remove(handler);
                if (commandHandlers.isEmpty()) {
                    handlers.remove(commandName);
                }
            }
            return localRegistration.cancel();
        };
    }

    /**
     * Listener de Kafka que consume comandos con idempotencia y DLQ
     */
    @KafkaListener(
            topics = "${kafka.command.topic}",
            groupId = "${kafka.command.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeCommand(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        String messageId = null;
        String correlationId = null;
        
        try {
            log.info("📨 Comando recibido - Key: {}, Partition: {}, Offset: {}", 
                    record.key(), record.partition(), record.offset());
            
            String commandJson = record.value();
            CommandMessage<?> commandMessage = commandSerializer.deserialize(commandJson);
            
            // Extraer IDs del metadata
            messageId = extractMessageId(commandMessage);
            correlationId = extractCorrelationId(commandMessage);
            
            // ✅ IDEMPOTENCIA: Verificar si ya fue procesado
            if (!deduplicationService.markAsProcessed(messageId)) {
                log.warn("⚠️ Comando duplicado bloqueado: {}", messageId);
                duplicateCommands++;
                acknowledgment.acknowledge();
                
                // Enviar respuesta de duplicado
                if (correlationId != null) {
                    sendReply(correlationId, true, "Comando duplicado (ya procesado)");
                }
                return;
            }
            
            log.debug("Comando deserializado: {}", commandMessage.getPayloadType().getSimpleName());
            
            // Procesar usando el CommandBus local
            processCommandWithReply(commandMessage, correlationId);
            
            processedCommands++;
            acknowledgment.acknowledge();
            log.debug("✅ Comando confirmado en Kafka");
            
        } catch (BusinessException e) {
            // Error de negocio: enviar a DLQ y confirmar
            log.error("❌ Error de negocio procesando comando: {}", e.getMessage());
            failedCommands++;
            
            sendToDLQ(record, e.getMessage());
            sendReply(correlationId, false, "Error de negocio: " + e.getMessage());
            
            acknowledgment.acknowledge();
            
        } catch (Exception e) {
            // Error técnico: NO confirmar para reintentar
            log.error("💥 Error técnico procesando comando (se reintentará): {}", 
                record.value(), e);
            failedCommands++;
            
            sendReply(correlationId, false, "Error técnico: " + e.getMessage());
            
            // NO hacer acknowledge para que Kafka reintente
            // El comando volverá a la cola
        }
    }

    /**
     * Procesa el comando y envía respuesta
     */
    private void processCommandWithReply(CommandMessage<?> commandMessage, String correlationId) {
        try {
            log.info("⚙️ Procesando comando: {}", commandMessage.getPayloadType().getSimpleName());
            
            localCommandBus.dispatch(commandMessage, new CommandCallback<Object, Object>() {
                @Override
                public void onResult(@Nonnull CommandMessage<?> commandMessage, 
                                     @Nonnull org.axonframework.commandhandling.CommandResultMessage<?> result) {
                    if (result.isExceptional()) {
                        Throwable exception = result.exceptionResult();
                        log.error("❌ Error procesando comando: {}", 
                                commandMessage.getPayloadType().getSimpleName(), exception);
                        
                        sendReply(correlationId, false, exception.getMessage());
                        
                        if (exception instanceof BusinessException) {
                            throw (BusinessException) exception;
                        }
                    } else {
                        log.info("✅ Comando procesado exitosamente: {}", 
                                commandMessage.getPayloadType().getSimpleName());
                        
                        String resultStr = result.getPayload() != null ? 
                            result.getPayload().toString() : "OK";
                        sendReply(correlationId, true, resultStr);
                    }
                }
            });
            
        } catch (Exception e) {
            log.error("Error despachando comando al CommandBus local", e);
            throw e;
        }
    }

    /**
     * Envía comando fallido a Dead Letter Queue
     */
    private void sendToDLQ(ConsumerRecord<String, String> record, String errorMessage) {
        try {
            String dlqMessage = String.format(
                "{\"originalKey\":\"%s\",\"originalValue\":%s,\"error\":\"%s\",\"timestamp\":%d}",
                record.key(), record.value(), errorMessage, System.currentTimeMillis()
            );
            
            kafkaTemplate.send(dlqTopic, record.key(), dlqMessage);
            log.info("📮 Comando enviado a DLQ: {}", dlqTopic);
            
        } catch (Exception e) {
            log.error("Error enviando comando a DLQ", e);
        }
    }

    /**
     * Envía respuesta al tópico de respuestas
     */
    private void sendReply(String correlationId, boolean success, String message) {
        if (correlationId == null) {
            log.warn("⚠️ No hay correlationId para enviar respuesta");
            return;
        }
        
        try {
            CommandReplyHandler.CommandResult result = success ?
                CommandReplyHandler.CommandResult.success(correlationId, message) :
                CommandReplyHandler.CommandResult.error(correlationId, message);
            
            String resultJson = new com.fasterxml.jackson.databind.ObjectMapper()
                .writeValueAsString(result);
            
            log.info("📤 Preparando envío de respuesta: correlationId={}, success={}, tópico='{}'", 
                correlationId, success, replyTopic);
            log.debug("📤 JSON de respuesta: {}", resultJson);
            
            // ✅ IMPORTANTE: Esperar confirmación de envío para debugging
            kafkaTemplate.send(replyTopic, correlationId, resultJson)
                .whenComplete((sendResult, exception) -> {
                    if (exception != null) {
                        log.error("❌ CRÍTICO: Error enviando respuesta a Kafka", exception);
                    } else {
                        log.info("✅ Respuesta enviada exitosamente a Kafka - Offset: {}, Partition: {}", 
                            sendResult.getRecordMetadata().offset(),
                            sendResult.getRecordMetadata().partition());
                    }
                });
            
        } catch (Exception e) {
            log.error("💥 Error serializando o enviando respuesta de comando", e);
        }
    }

    /**
     * Extrae messageId del comando
     */
    private String extractMessageId(CommandMessage<?> commandMessage) {
        Object messageId = commandMessage.getMetaData().get("messageIdentifier");
        return messageId != null ? messageId.toString() : commandMessage.getIdentifier();
    }

    /**
     * Extrae correlationId del comando
     */
    private String extractCorrelationId(CommandMessage<?> commandMessage) {
        Object correlationId = commandMessage.getMetaData().get("correlationId");
        return correlationId != null ? correlationId.toString() : null;
    }

    /**
     * Obtiene métricas del CommandBus
     */
    public Map<String, Object> getMetrics() {
        return Map.of(
            "processedCommands", processedCommands,
            "failedCommands", failedCommands,
            "duplicateCommands", duplicateCommands,
            "successRate", processedCommands > 0 ? 
                (processedCommands - failedCommands) * 100.0 / processedCommands : 0
        );
    }

    public org.axonframework.common.Registration registerHandlerInterceptor(
            @Nonnull MessageHandlerInterceptor<? super CommandMessage<?>> handlerInterceptor) {
        handlerInterceptors.add(handlerInterceptor);
        return () -> handlerInterceptors.remove(handlerInterceptor);
    }

    public org.axonframework.common.Registration registerDispatchInterceptor(
            @Nonnull org.axonframework.messaging.MessageDispatchInterceptor<? super CommandMessage<?>> dispatchInterceptor) {
        return localCommandBus.registerDispatchInterceptor(dispatchInterceptor);
    }

    /**
     * Excepción para errores de negocio (van a DLQ)
     */
    public static class BusinessException extends RuntimeException {
        public BusinessException(String message) {
            super(message);
        }
    }
}