package com.example.banking.infrastructure.logging;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.axonframework.eventhandling.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Listener que loguea todos los eventos del sistema
 * Útil para debugging y monitoreo
 */
@Component
public class EventLoggingListener {

    private static final Logger logger = LoggerFactory.getLogger(EventLoggingListener.class);
    private final ObjectMapper objectMapper;

    public EventLoggingListener(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Captura TODOS los eventos y los loguea
     */
    @EventHandler
    public void on(Object event) {
        try {
            String eventJson = objectMapper.writerWithDefaultPrettyPrinter()
                                          .writeValueAsString(event);
            
            logger.info("=== EVENTO PROCESADO ===");
            logger.info("Tipo: {}", event.getClass().getSimpleName());
            logger.info("Detalles:\n{}", eventJson);
            logger.info("========================");
            
        } catch (Exception e) {
            logger.error("Error al loguear evento: {}", event.getClass().getName(), e);
        }
    }
}