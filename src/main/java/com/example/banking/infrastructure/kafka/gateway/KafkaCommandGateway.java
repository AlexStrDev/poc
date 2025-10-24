package com.example.banking.infrastructure.kafka.gateway;

import com.example.banking.infrastructure.kafka.serializer.CommandSerializer;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.commandhandling.CommandCallback;
import org.axonframework.commandhandling.CommandMessage;
import org.axonframework.commandhandling.GenericCommandMessage;
import org.axonframework.commandhandling.GenericCommandResultMessage;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.common.Registration;
import org.axonframework.messaging.MessageDispatchInterceptor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * CommandGateway personalizado que envía comandos a Kafka en lugar de Axon Server.
 */
@Slf4j
@Component
public class KafkaCommandGateway implements CommandGateway {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final CommandSerializer commandSerializer;
    private final String commandTopic;

    public KafkaCommandGateway(
            KafkaTemplate<String, String> kafkaTemplate,
            CommandSerializer commandSerializer,
            @Value("${kafka.command.topic}") String commandTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.commandSerializer = commandSerializer;
        this.commandTopic = commandTopic;
    }

    @Override
    public <C, R> void send(C command, CommandCallback<? super C, ? super R> callback) {
        try {
            log.info("Enviando comando: {} al tópico: {}", command.getClass().getSimpleName(), commandTopic);
            
            // Crear CommandMessage
            CommandMessage<C> commandMessage = GenericCommandMessage.asCommandMessage(command);
            
            // Extraer routing key
            String routingKey = commandSerializer.extractRoutingKey(commandMessage);
            
            // Serializar el comando
            String serializedCommand = commandSerializer.serialize(commandMessage, routingKey);
            
            // Enviar a Kafka
            CompletableFuture<SendResult<String, String>> future = 
                    kafkaTemplate.send(commandTopic, routingKey, serializedCommand);
            
            // Manejar el resultado del envío
            future.whenComplete((result, throwable) -> {
                if (throwable != null) {
                    log.error("Error al enviar comando a Kafka", throwable);
                    if (callback != null) {
                        callback.onResult(commandMessage, 
                                GenericCommandResultMessage.asCommandResultMessage(throwable));
                    }
                } else {
                    log.info("Comando enviado exitosamente. Offset: {}, Partition: {}", 
                            result.getRecordMetadata().offset(),
                            result.getRecordMetadata().partition());
                    
                    // Para comandos que esperan respuesta
                    if (callback != null) {
                        // Simulamos una respuesta exitosa inmediata
                        // En producción, implementar mecanismo de respuesta real
                        CompletableFuture.delayedExecutor(100, TimeUnit.MILLISECONDS).execute(() -> {
                            callback.onResult(commandMessage, 
                                    GenericCommandResultMessage.asCommandResultMessage("Command processed"));
                        });
                    }
                }
            });
            
        } catch (Exception e) {
            log.error("Error al enviar comando", e);
            if (callback != null) {
                CommandMessage<C> commandMessage = GenericCommandMessage.asCommandMessage(command);
                callback.onResult(commandMessage, GenericCommandResultMessage.asCommandResultMessage(e));
            }
        }
    }

    @Override
    public <R> R sendAndWait(Object command) {
        return sendAndWait(command, 10, TimeUnit.SECONDS);
    }

    @Override
    public <R> R sendAndWait(Object command, long timeout, TimeUnit unit) {
        CompletableFuture<R> result = new CompletableFuture<>();
        
        send(command, new CommandCallback<Object, R>() {
            @Override
            public void onResult(CommandMessage<? extends Object> commandMessage, 
                               org.axonframework.commandhandling.CommandResultMessage<? extends R> commandResultMessage) {
                if (commandResultMessage.isExceptional()) {
                    result.completeExceptionally(commandResultMessage.exceptionResult());
                } else {
                    result.complete(commandResultMessage.getPayload());
                }
            }
        });
        
        try {
            return result.get(timeout, unit);
        } catch (Exception e) {
            throw new RuntimeException("Error esperando respuesta del comando", e);
        }
    }

    @Override
    public <R> CompletableFuture<R> send(Object command) {
        CompletableFuture<R> future = new CompletableFuture<>();
        
        send(command, new CommandCallback<Object, R>() {
            @Override
            public void onResult(CommandMessage<? extends Object> commandMessage, 
                               org.axonframework.commandhandling.CommandResultMessage<? extends R> commandResultMessage) {
                if (commandResultMessage.isExceptional()) {
                    future.completeExceptionally(commandResultMessage.exceptionResult());
                } else {
                    future.complete(commandResultMessage.getPayload());
                }
            }
        });
        
        return future;
    }

    @Override
    public Registration registerDispatchInterceptor(
            MessageDispatchInterceptor<? super CommandMessage<?>> dispatchInterceptor) {
        log.warn("registerDispatchInterceptor no está completamente implementado en KafkaCommandGateway");
        // Retornar un Registration vacío
        return () -> true;
    }
}