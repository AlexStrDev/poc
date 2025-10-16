package com.example.banking.infrastructure.kafka;

import com.example.banking.infrastructure.kafka.message.AxonCommandMessage;
import org.axonframework.commandhandling.CommandBus;
import org.axonframework.commandhandling.CommandCallback;
import org.axonframework.commandhandling.CommandMessage;
import org.axonframework.commandhandling.GenericCommandMessage;
import org.axonframework.commandhandling.callbacks.NoOpCallback;
import org.axonframework.common.Registration;
import org.axonframework.messaging.MessageDispatchInterceptor;
import org.axonframework.messaging.MessageHandler;
import org.axonframework.messaging.MessageHandlerInterceptor;
import org.axonframework.messaging.MetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementación de CommandBus que usa Kafka como transporte,
 * replicando la estructura de mensajes de Axon Server
 */
@Component
public class KafkaCommandBus implements CommandBus {

    private static final Logger logger = LoggerFactory.getLogger(KafkaCommandBus.class);
    
    private final CommandBus localSegment;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final CommandSerializer commandSerializer;
    private final String commandTopic;
    
    // Almacena los handlers registrados por nombre de comando
    private final Map<String, MessageHandler<? super CommandMessage<?>>> commandHandlers = new ConcurrentHashMap<>();

    public KafkaCommandBus(CommandBus localSegment,
                          KafkaTemplate<String, String> kafkaTemplate,
                          CommandSerializer commandSerializer) {
        this.localSegment = localSegment;
        this.kafkaTemplate = kafkaTemplate;
        this.commandSerializer = commandSerializer;
        this.commandTopic = "axon-commands";
    }

    @Override
    public <C> void dispatch(@Nonnull CommandMessage<C> command) {
        dispatch(command, NoOpCallback.INSTANCE);
    }

    @Override
    public <C, R> void dispatch(@Nonnull CommandMessage<C> command,
                               @Nonnull CommandCallback<? super C, ? super R> callback) {
        try {
            String commandId = command.getIdentifier();
            String commandName = command.getCommandName();
            
            logger.debug("Dispatching command [{}] with id [{}] to Kafka", commandName, commandId);
            
            // Determinar routing key (aggregateId del comando)
            String routingKey = extractRoutingKey(command);
            
            // Serializar el comando completo (similar a Axon Server)
            AxonCommandMessage axonMessage = commandSerializer.serialize(command, routingKey, 0);
            String jsonPayload = commandSerializer.serializeToJson(axonMessage);
            
            // Enviar a Kafka usando el commandName como key para particionamiento
            CompletableFuture<SendResult<String, String>> future = 
                kafkaTemplate.send(commandTopic, commandName, jsonPayload);
            
            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    logger.error("Error sending command to Kafka", ex);
                    callback.onResult(command, 
                        org.axonframework.commandhandling.GenericCommandResultMessage.asCommandResultMessage(ex));
                } else {
                    logger.debug("Command [{}] sent successfully to Kafka partition {}", 
                        commandName, result.getRecordMetadata().partition());
                    
                    // Completar el callback exitosamente (fire-and-forget)
                    @SuppressWarnings("unchecked")
                    org.axonframework.commandhandling.CommandResultMessage<R> successResult = 
                        (org.axonframework.commandhandling.CommandResultMessage<R>) 
                        org.axonframework.commandhandling.GenericCommandResultMessage.asCommandResultMessage(commandId);
                    callback.onResult(command, successResult);
                }
            });
            
        } catch (Exception e) {
            logger.error("Error dispatching command", e);
            callback.onResult(command, 
                org.axonframework.commandhandling.GenericCommandResultMessage.asCommandResultMessage(e));
        }
    }

    @Override
    public Registration subscribe(@Nonnull String commandName,
                                  @Nonnull MessageHandler<? super CommandMessage<?>> handler) {
        logger.info("Subscribing handler for command: {}", commandName);
        
        // Registrar en el segmento local (para comandos locales)
        Registration localRegistration = localSegment.subscribe(commandName, handler);
        
        // Registrar en nuestro mapa para comandos de Kafka
        commandHandlers.put(commandName, handler);
        
        return () -> {
            commandHandlers.remove(commandName);
            return localRegistration.cancel();
        };
    }

    /**
     * Procesa un comando recibido desde Kafka
     * Deserializa el AxonCommandMessage completo y reconstruye el CommandMessage
     */
    public void handleIncomingCommand(String jsonPayload) {
        try {
            // Deserializar el mensaje completo de Kafka
            AxonCommandMessage axonMessage = commandSerializer.deserializeFromJson(jsonPayload);
            
            String commandName = axonMessage.getCommandName();
            logger.debug("Processing incoming command [{}] with ID [{}]", 
                commandName, axonMessage.getMessageIdentifier());
            
            // Obtener el handler registrado
            MessageHandler<? super CommandMessage<?>> handler = commandHandlers.get(commandName);
            
            if (handler == null) {
                logger.warn("No handler found for command: {}", commandName);
                return;
            }
            
            // Deserializar el payload
            Object commandPayload = commandSerializer.deserializePayload(axonMessage);
            
            // Reconstruir el MetaData
            MetaData metaData = MetaData.from(axonMessage.getMetaData());
            
            // Reconstruir el CommandMessage completo
            CommandMessage<?> commandMessage = new GenericCommandMessage<>(
                commandPayload,
                metaData
            ).withMetaData(Map.of(
                "messageIdentifier", axonMessage.getMessageIdentifier(),
                "timestamp", Instant.ofEpochMilli(axonMessage.getTimestamp()),
                "routingKey", axonMessage.getRoutingKey()
            ));
            
            // Ejecutar el comando en el localSegment
            localSegment.dispatch(commandMessage, (cmd, result) -> {
                if (result.isExceptional()) {
                    logger.error("Command [{}] execution failed: {}", 
                        commandName, 
                        result.exceptionResult().getMessage());
                } else {
                    logger.debug("Command [{}] processed successfully", commandName);
                }
            });
            
        } catch (Exception e) {
            logger.error("Error handling incoming command from Kafka", e);
        }
    }

    /**
     * Extrae el routing key del comando (típicamente el aggregateId)
     */
    private String extractRoutingKey(CommandMessage<?> command) {
        try {
            Object payload = command.getPayload();
            
            // Intentar obtener el campo annotado con @TargetAggregateIdentifier
            java.lang.reflect.Field[] fields = payload.getClass().getDeclaredFields();
            for (java.lang.reflect.Field field : fields) {
                if (field.isAnnotationPresent(org.axonframework.modelling.command.TargetAggregateIdentifier.class)) {
                    field.setAccessible(true);
                    Object value = field.get(payload);
                    return value != null ? value.toString() : command.getIdentifier();
                }
            }
        } catch (Exception e) {
            logger.debug("Could not extract routing key from command, using message identifier");
        }
        
        return command.getIdentifier();
    }

    /**
     * Obtiene el segmento local del command bus
     */
    public CommandBus localSegment() {
        return localSegment;
    }

    @Override
    public Registration registerHandlerInterceptor(
            MessageHandlerInterceptor<? super CommandMessage<?>> handlerInterceptor) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'registerHandlerInterceptor'");
    }

    @Override
    public Registration registerDispatchInterceptor(
            MessageDispatchInterceptor<? super CommandMessage<?>> dispatchInterceptor) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'registerDispatchInterceptor'");
    }
}