package com.example.banking.infrastructure.kafka.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * Modelo que representa un comando serializado para enviar/recibir por Kafka.
 * Formato compatible con Axon Server.
 */
@Data
public class SerializedCommand {
    
    /**
     * Identificador único del mensaje (UUID)
     */
    @JsonProperty("messageIdentifier")
    private String messageIdentifier;
    
    /**
     * Nombre completo de la clase del command
     */
    @JsonProperty("commandName")
    private String commandName;
    
    /**
     * Timestamp de creación del comando
     */
    @JsonProperty("timestamp")
    private long timestamp;
    
    /**
     * Payload serializado del comando
     */
    @JsonProperty("payload")
    private SerializedObject payload;
    
    /**
     * Metadatos del comando (información contextual)
     */
    @JsonProperty("metaData")
    private Map<String, Object> metaData = new HashMap<>();
    
    /**
     * Routing key (típicamente el aggregateId)
     */
    @JsonProperty("routingKey")
    private String routingKey;
    
    /**
     * Representa un objeto serializado
     */
    @Data
    public static class SerializedObject {
        
        /**
         * Nombre de la clase
         */
        @JsonProperty("type")
        private String type;
        
        /**
         * Versión del objeto
         */
        @JsonProperty("revision")
        private String revision;
        
        /**
         * Datos serializados en formato JSON (como String)
         */
        @JsonProperty("data")
        private String data;
    }
}