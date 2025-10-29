package com.example.banking.infrastructure.kafka.bus;

import com.example.banking.infrastructure.kafka.serializer.CommandSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.axonframework.commandhandling.CommandBus;
import org.axonframework.commandhandling.CommandCallback;
import org.axonframework.commandhandling.CommandMessage;
import org.axonframework.messaging.MessageHandler;
import org.axonframework.messaging.MessageHandlerInterceptor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * CommandBus que consume comandos desde Kafka y los despacha a handlers registrados.
 * Implementa registro dinámico de handlers por tipo de comando.
 */
@Slf4j
@Component
public class KafkaCommandBus {

    private final CommandBus localCommandBus;
    private final CommandSerializer commandSerializer;
    private final Map<String, List<MessageHandler<? super CommandMessage<?>>>> handlers;
    private final List<MessageHandlerInterceptor<? super CommandMessage<?>>> handlerInterceptors;

    public KafkaCommandBus(
            CommandBus localCommandBus,
            CommandSerializer commandSerializer) {
        this.localCommandBus = localCommandBus;
        this.commandSerializer = commandSerializer;
        this.handlers = new ConcurrentHashMap<>();
        this.handlerInterceptors = new CopyOnWriteArrayList<>();
    }

    /**
     * Registra un handler para un tipo específico de comando.
     * Este método permite que los aggregates se registren dinámicamente.
     */
    public org.axonframework.common.Registration subscribe(
            @Nonnull String commandName,
            @Nonnull MessageHandler<? super CommandMessage<?>> handler) {
        
        log.info("Registrando handler para comando: {}", commandName);
        
        handlers.computeIfAbsent(commandName, k -> new CopyOnWriteArrayList<>()).add(handler);
        
        // También registrar en el CommandBus local
        org.axonframework.common.Registration localRegistration = 
            localCommandBus.subscribe(commandName, handler);
        
        // Retornar Registration que elimina de ambos lugares
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
     * Listener de Kafka que consume comandos del tópico configurado
     */
    @KafkaListener(
            topics = "${kafka.command.topic}",
            groupId = "${kafka.command.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeCommand(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        try {
            log.info("Comando recibido - Key: {}, Partition: {}, Offset: {}", 
                    record.key(), record.partition(), record.offset());
            
            String commandJson = record.value();
            CommandMessage<?> commandMessage = commandSerializer.deserialize(commandJson);
            
            log.debug("Comando deserializado: {}", commandMessage.getPayloadType().getSimpleName());
            
            // Procesar usando el CommandBus local
            processCommand(commandMessage);
            
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
                log.debug("Comando confirmado en Kafka");
            }
            
        } catch (Exception e) {
            log.error("Error procesando comando de Kafka: {}", record.value(), e);
            // No hacer acknowledge para reprocesar
        }
    }

    private void processCommand(CommandMessage<?> commandMessage) {
        try {
            log.info("Procesando comando: {}", commandMessage.getPayloadType().getSimpleName());
            
            localCommandBus.dispatch(commandMessage, new CommandCallback<Object, Object>() {
                @Override
                public void onResult(@Nonnull CommandMessage<?> commandMessage, 
                                     @Nonnull org.axonframework.commandhandling.CommandResultMessage<?> result) {
                    if (result.isExceptional()) {
                        log.error("Error procesando comando: {}", 
                                commandMessage.getPayloadType().getSimpleName(),
                                result.exceptionResult());
                    } else {
                        log.info("Comando procesado exitosamente: {}", 
                                commandMessage.getPayloadType().getSimpleName());
                    }
                }
            });
            
        } catch (Exception e) {
            log.error("Error despachando comando al CommandBus local", e);
            throw e;
        }
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
}