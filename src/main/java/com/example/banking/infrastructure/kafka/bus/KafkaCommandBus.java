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
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * CommandBus personalizado que consume comandos desde Kafka y los procesa
 * usando el CommandBus local de Axon Framework.
 */
@Slf4j
@Component
public class KafkaCommandBus {

    private final CommandBus localCommandBus;
    private final CommandSerializer commandSerializer;
    private final List<MessageHandlerInterceptor<? super CommandMessage<?>>> handlerInterceptors;

    public KafkaCommandBus(
            CommandBus localCommandBus,
            CommandSerializer commandSerializer) {
        this.localCommandBus = localCommandBus;
        this.commandSerializer = commandSerializer;
        this.handlerInterceptors = new CopyOnWriteArrayList<>();
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
            log.info("Comando recibido de Kafka - Key: {}, Partition: {}, Offset: {}", 
                    record.key(), record.partition(), record.offset());
            
            // Deserializar el comando
            String commandJson = record.value();
            CommandMessage<?> commandMessage = commandSerializer.deserialize(commandJson);
            
            log.debug("Comando deserializado: {}", commandMessage.getPayloadType().getSimpleName());
            
            // Procesar el comando usando el CommandBus local
            processCommand(commandMessage);
            
            // Confirmar el procesamiento
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
                log.debug("Comando confirmado en Kafka");
            }
            
        } catch (Exception e) {
            log.error("Error al procesar comando de Kafka: {}", record.value(), e);
            // En caso de error, no hacer acknowledge para que el mensaje se reprocese
            // O podrías enviarlo a un DLQ (Dead Letter Queue)
        }
    }

    /**
     * Procesa un comando usando el CommandBus local de Axon
     */
    private void processCommand(CommandMessage<?> commandMessage) {
        try {
            log.info("Procesando comando: {}", commandMessage.getPayloadType().getSimpleName());
            
            // Enviar el comando al CommandBus local para que sea procesado por el Aggregate
            localCommandBus.dispatch(commandMessage, new CommandCallback<Object, Object>() {
                @Override
                public void onResult(@Nonnull CommandMessage<?> commandMessage, 
                                     @Nonnull org.axonframework.commandhandling.CommandResultMessage<?> commandResultMessage) {
                    if (commandResultMessage.isExceptional()) {
                        log.error("Error al procesar comando: {}", 
                                commandMessage.getPayloadType().getSimpleName(),
                                commandResultMessage.exceptionResult());
                    } else {
                        log.info("Comando procesado exitosamente: {} - Resultado: {}", 
                                commandMessage.getPayloadType().getSimpleName(),
                                commandResultMessage.getPayload());
                    }
                }
            });
            
        } catch (Exception e) {
            log.error("Error al despachar comando al CommandBus local", e);
            throw e;
        }
    }

    /**
     * Registra un handler para comandos específicos (delegado al CommandBus local)
     */
    public org.axonframework.common.Registration subscribe(
            @Nonnull String commandName,
            @Nonnull MessageHandler<? super CommandMessage<?>> handler) {
        log.info("Registrando handler para comando: {}", commandName);
        return localCommandBus.subscribe(commandName, handler);
    }

    /**
     * Registra un interceptor de handlers
     */
    public org.axonframework.common.Registration registerHandlerInterceptor(
            @Nonnull MessageHandlerInterceptor<? super CommandMessage<?>> handlerInterceptor) {
        handlerInterceptors.add(handlerInterceptor);
        return () -> handlerInterceptors.remove(handlerInterceptor);
    }

    /**
     * Registra un interceptor de dispatch (delegado al CommandBus local)
     */
    public org.axonframework.common.Registration registerDispatchInterceptor(
            @Nonnull org.axonframework.messaging.MessageDispatchInterceptor<? super CommandMessage<?>> dispatchInterceptor) {
        return localCommandBus.registerDispatchInterceptor(dispatchInterceptor);
    }
}