package com.example.banking.infrastructure.kafka.serializer;

import com.example.banking.infrastructure.kafka.model.SerializedCommand;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.commandhandling.CommandMessage;
import org.axonframework.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.MetaData;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Serializa y deserializa comandos de Axon a formato JSON para Kafka.
 */
@Slf4j
@Component
public class CommandSerializer {

    private final ObjectMapper objectMapper;

    public CommandSerializer() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    /**
     * Serializa un CommandMessage de Axon a formato SerializedCommand para Kafka
     */
    public String serialize(CommandMessage<?> commandMessage, String routingKey) {
        try {
            String messageId = commandMessage.getIdentifier();
            String commandName = commandMessage.getPayloadType().getName();
            long timestamp = System.currentTimeMillis();
            
            // Serializar el payload
            Object payload = commandMessage.getPayload();
            String payloadJson = objectMapper.writeValueAsString(payload);
            
            SerializedCommand.SerializedObject serializedPayload = new SerializedCommand.SerializedObject();
            serializedPayload.setType(commandName);
            serializedPayload.setRevision("1.0");
            serializedPayload.setData(payloadJson);
            
            // Convertir MetaData a Map
            Map<String, Object> metaDataMap = new HashMap<>();
            commandMessage.getMetaData().forEach(metaDataMap::put);
            
            // Crear el comando serializado
            SerializedCommand serializedCommand = new SerializedCommand();
            serializedCommand.setMessageIdentifier(messageId);
            serializedCommand.setCommandName(commandName);
            serializedCommand.setTimestamp(timestamp);
            serializedCommand.setPayload(serializedPayload);
            serializedCommand.setMetaData(metaDataMap);
            serializedCommand.setRoutingKey(routingKey);
            
            String json = objectMapper.writeValueAsString(serializedCommand);
            log.debug("Comando serializado: {}", json);
            
            return json;
            
        } catch (Exception e) {
            log.error("Error al serializar comando: {}", commandMessage, e);
            throw new RuntimeException("Error al serializar comando", e);
        }
    }

    /**
     * Deserializa un JSON de Kafka a CommandMessage de Axon
     * 
     * ✅ CORREGIDO: Preserva TODOS los metadatos incluyendo correlationId
     */
    public CommandMessage<?> deserialize(String json) {
        try {
            log.debug("Deserializando comando: {}", json);
            
            SerializedCommand serializedCommand = objectMapper.readValue(json, SerializedCommand.class);
            
            // Deserializar el payload
            Class<?> payloadType = Class.forName(serializedCommand.getCommandName());
            Object payload = objectMapper.readValue(
                    serializedCommand.getPayload().getData(),
                    payloadType
            );
            
            // ✅ CORREGIDO: Combinar TODOS los metadatos en lugar de reemplazarlos
            Map<String, Object> allMetaData = new HashMap<>(serializedCommand.getMetaData());
            allMetaData.put("messageIdentifier", serializedCommand.getMessageIdentifier());
            
            MetaData metaData = MetaData.from(allMetaData);
            
            log.debug("✅ Metadata deserializado: {}", allMetaData);
            log.debug("✅ CorrelationId extraído: {}", allMetaData.get("correlationId"));
            
            // Crear el CommandMessage con TODO el metadata preservado
            return new GenericCommandMessage<>(payload, metaData);
            
        } catch (Exception e) {
            log.error("Error al deserializar comando: {}", json, e);
            throw new RuntimeException("Error al deserializar comando", e);
        }
    }

    /**
     * Extrae el routingKey de un CommandMessage
     */
    public String extractRoutingKey(CommandMessage<?> commandMessage) {
        try {
            Object payload = commandMessage.getPayload();
            
            // Buscar el campo anotado con @TargetAggregateIdentifier
            var fields = payload.getClass().getDeclaredFields();
            for (var field : fields) {
                if (field.isAnnotationPresent(org.axonframework.modelling.command.TargetAggregateIdentifier.class)) {
                    field.setAccessible(true);
                    Object value = field.get(payload);
                    return value != null ? value.toString() : UUID.randomUUID().toString();
                }
            }
            
            // Si no encuentra el campo, usar un UUID
            return UUID.randomUUID().toString();
            
        } catch (Exception e) {
            log.warn("No se pudo extraer routingKey, usando UUID aleatorio", e);
            return UUID.randomUUID().toString();
        }
    }
}