package com.example.banking.infrastructure.kafka.event;

import com.example.banking.infrastructure.kafka.event.AxonEventMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * Serializa y deserializa eventos para Kafka EventStore
 */
@Component("kafkaEventSerializer")  // Nombre específico para evitar conflicto con Axon
public class KafkaEventSerializer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaEventSerializer.class);
    private static final String DEFAULT_REVISION = "1.0";
    
    private final ObjectMapper objectMapper;

    public KafkaEventSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Serializa un DomainEventMessage a AxonEventMessage
     */
    public AxonEventMessage serialize(DomainEventMessage<?> eventMessage) throws Exception {
        Object payload = eventMessage.getPayload();
        String payloadType = payload.getClass().getName();
        
        // Serializar el payload a JSON y luego a Base64
        String payloadJson = objectMapper.writeValueAsString(payload);
        String payloadBase64 = Base64.getEncoder().encodeToString(
            payloadJson.getBytes(StandardCharsets.UTF_8)
        );
        
        // Crear el SerializedObject
        AxonEventMessage.SerializedObject serializedPayload = new AxonEventMessage.SerializedObject(
            payloadType,
            DEFAULT_REVISION,
            payloadBase64
        );
        
        // Convertir MetaData a Map<String, Object>
        Map<String, Object> metaDataMap = new HashMap<>();
        eventMessage.getMetaData().forEach(metaDataMap::put);
        
        // Crear el mensaje completo
        AxonEventMessage axonMessage = new AxonEventMessage(
            eventMessage.getIdentifier(),
            eventMessage.getAggregateIdentifier(),
            eventMessage.getType(),
            eventMessage.getSequenceNumber(),
            eventMessage.getTimestamp().toEpochMilli(),
            serializedPayload,
            metaDataMap,
            DEFAULT_REVISION
        );
        
        logger.debug("Serialized event: {} seq={} for aggregate: {}", 
            payloadType, 
            eventMessage.getSequenceNumber(),
            eventMessage.getAggregateIdentifier());
        
        return axonMessage;
    }

    /**
     * Serializa AxonEventMessage a JSON para Kafka
     */
    public String serializeToJson(AxonEventMessage axonMessage) throws Exception {
        return objectMapper.writeValueAsString(axonMessage);
    }

    /**
     * Deserializa JSON de Kafka a AxonEventMessage
     */
    public AxonEventMessage deserializeFromJson(String json) throws Exception {
        return objectMapper.readValue(json, AxonEventMessage.class);
    }

    /**
     * Deserializa el payload del AxonEventMessage
     */
    public Object deserializePayload(AxonEventMessage axonMessage) throws Exception {
        AxonEventMessage.SerializedObject serializedPayload = axonMessage.getPayload();
        String payloadType = serializedPayload.getType();
        
        // Cargar la clase del evento
        Class<?> eventClass = Class.forName(payloadType);
        
        // Decodificar de Base64 a JSON
        byte[] payloadBytes = Base64.getDecoder().decode(serializedPayload.getData());
        String payloadJson = new String(payloadBytes, StandardCharsets.UTF_8);
        
        // Deserializar JSON al objeto de evento
        Object event = objectMapper.readValue(payloadJson, eventClass);
        
        logger.debug("Deserialized event payload of type: {}", payloadType);
        
        return event;
    }
}