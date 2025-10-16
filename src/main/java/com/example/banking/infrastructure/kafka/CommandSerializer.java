package com.example.banking.infrastructure.kafka;

import com.example.banking.infrastructure.kafka.message.AxonCommandMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.axonframework.commandhandling.CommandMessage;
import org.axonframework.messaging.MetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * Serializador/Deserializador de comandos para Kafka
 * Replica la estructura de mensajes de Axon Server
 */
@Component
public class CommandSerializer {

    private static final Logger logger = LoggerFactory.getLogger(CommandSerializer.class);
    private static final String DEFAULT_REVISION = "1.0";
    
    private final ObjectMapper objectMapper;
    private final Map<String, Class<?>> commandRegistry = new HashMap<>();

    public CommandSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        registerCommands();
    }

    /**
     * Registra las clases de comandos disponibles
     */
    private void registerCommands() {
        try {
            registerCommand("com.example.banking.command.CreateAccountCommand");
            registerCommand("com.example.banking.command.DepositMoneyCommand");
            registerCommand("com.example.banking.command.WithdrawMoneyCommand");
            
            logger.info("Registered {} command types", commandRegistry.size());
        } catch (Exception e) {
            logger.error("Error registering commands", e);
        }
    }

    /**
     * Registra una clase de comando
     */
    private void registerCommand(String fullClassName) {
        try {
            Class<?> commandClass = Class.forName(fullClassName);
            String simpleClassName = commandClass.getSimpleName();
            commandRegistry.put(simpleClassName, commandClass);
            commandRegistry.put(fullClassName, commandClass);
            logger.debug("Registered command: {} -> {}", simpleClassName, fullClassName);
        } catch (ClassNotFoundException e) {
            logger.warn("Could not register command class: {}", fullClassName);
        }
    }

    /**
     * Serializa un CommandMessage completo a la estructura de Axon Server
     */
    public AxonCommandMessage serialize(CommandMessage<?> commandMessage, String routingKey, int priority) throws Exception {
        Object payload = commandMessage.getPayload();
        String payloadType = payload.getClass().getName();
        
        // Serializar el payload a JSON y luego a Base64
        String payloadJson = objectMapper.writeValueAsString(payload);
        String payloadBase64 = Base64.getEncoder().encodeToString(
            payloadJson.getBytes(StandardCharsets.UTF_8)
        );
        
        // Crear el SerializedObject
        AxonCommandMessage.SerializedObject serializedPayload = new AxonCommandMessage.SerializedObject(
            payloadType,
            DEFAULT_REVISION,
            payloadBase64
        );
        
        // Convertir MetaData a Map<String, Object>
        Map<String, Object> metaDataMap = new HashMap<>();
        commandMessage.getMetaData().forEach(metaDataMap::put);
        
        // Obtener timestamp actual (CommandMessage no tiene getTimestamp en todas las versiones)
        long timestamp = java.time.Instant.now().toEpochMilli();
        
        // Crear el mensaje completo
        AxonCommandMessage axonMessage = new AxonCommandMessage(
            commandMessage.getIdentifier(),
            commandMessage.getCommandName(),
            timestamp,
            serializedPayload,
            metaDataMap,
            routingKey,
            priority
        );
        
        logger.debug("Serialized command: {} with ID: {}", 
            commandMessage.getCommandName(), 
            commandMessage.getIdentifier());
        
        return axonMessage;
    }

    /**
     * Serializa AxonCommandMessage a JSON para Kafka
     */
    public String serializeToJson(AxonCommandMessage axonMessage) throws Exception {
        return objectMapper.writeValueAsString(axonMessage);
    }

    /**
     * Deserializa JSON de Kafka a AxonCommandMessage
     */
    public AxonCommandMessage deserializeFromJson(String json) throws Exception {
        return objectMapper.readValue(json, AxonCommandMessage.class);
    }

    /**
     * Deserializa el payload del AxonCommandMessage al objeto de comando real
     */
    public Object deserializePayload(AxonCommandMessage axonMessage) throws Exception {
        AxonCommandMessage.SerializedObject serializedPayload = axonMessage.getPayload();
        String payloadType = serializedPayload.getType();
        
        // Obtener la clase del comando
        Class<?> commandClass = commandRegistry.get(payloadType);
        if (commandClass == null) {
            // Intentar cargar la clase dinámicamente
            try {
                commandClass = Class.forName(payloadType);
                registerCommand(payloadType);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Unknown command type: " + payloadType, e);
            }
        }
        
        // Decodificar de Base64 a JSON
        byte[] payloadBytes = Base64.getDecoder().decode(serializedPayload.getData());
        String payloadJson = new String(payloadBytes, StandardCharsets.UTF_8);
        
        // Deserializar JSON al objeto de comando
        Object command = objectMapper.readValue(payloadJson, commandClass);
        
        logger.debug("Deserialized payload of type: {}", payloadType);
        
        return command;
    }

    /**
     * Obtiene la clase de un comando por su nombre
     */
    public Class<?> getCommandClass(String commandName) {
        return commandRegistry.get(commandName);
    }
}