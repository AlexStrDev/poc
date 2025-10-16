package com.example.banking.infrastructure.kafka.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Representa un evento completo en el EventStore de Kafka.
 * Similar a DomainEventMessage de Axon Server.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AxonEventMessage {
    
    /**
     * Identificador único del evento (UUID)
     */
    private String eventIdentifier;
    
    /**
     * Identificador del agregado al que pertenece este evento
     */
    private String aggregateIdentifier;
    
    /**
     * Tipo del agregado (nombre de la clase)
     */
    private String aggregateType;
    
    /**
     * Número de secuencia dentro del agregado (0, 1, 2, ...)
     */
    private long sequenceNumber;
    
    /**
     * Timestamp del evento (epoch millis)
     */
    private long timestamp;
    
    /**
     * Payload serializado del evento
     */
    private SerializedObject payload;
    
    /**
     * Metadata del evento
     */
    private Map<String, Object> metaData;
    
    /**
     * Versión del esquema del evento (para evolución)
     */
    private String revision;
    
    /**
     * Representa un objeto serializado
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SerializedObject {
        /**
         * Tipo del objeto (nombre completo de la clase)
         */
        private String type;
        
        /**
         * Versión/revisión del objeto
         */
        private String revision;
        
        /**
         * Datos serializados (JSON en Base64)
         */
        private String data;
    }
}