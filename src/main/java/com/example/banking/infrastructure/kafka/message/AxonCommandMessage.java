package com.example.banking.infrastructure.kafka.message;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Representa un mensaje de comando completo, similar a como lo envía Axon Server.
 * Esta estructura replica la forma en que Axon Server serializa comandos usando gRPC.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AxonCommandMessage {
    
    /**
     * Identificador único del mensaje (UUID)
     */
    private String messageIdentifier;
    
    /**
     * Nombre completo de la clase del comando
     * Ejemplo: "com.example.banking.command.CreateAccountCommand"
     */
    private String commandName;
    
    /**
     * Timestamp de cuando se creó el comando (epoch millis)
     */
    private long timestamp;
    
    /**
     * Payload serializado del comando
     */
    private SerializedObject payload;
    
    /**
     * Metadata del comando (información contextual)
     */
    private Map<String, Object> metaData;
    
    /**
     * Routing key para enrutamiento (típicamente el aggregateId)
     */
    private String routingKey;
    
    /**
     * Prioridad del comando (opcional)
     */
    private int priority;
    
    /**
     * Representa un objeto serializado con su tipo y datos
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
         * Versión/revisión del objeto (para versionado)
         */
        private String revision;
        
        /**
         * Datos serializados del objeto (JSON en formato Base64)
         */
        private String data;
    }
}