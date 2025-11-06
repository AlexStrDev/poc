package com.example.banking.infrastructure.cache;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Servicio de cache para garantizar idempotencia en el procesamiento de comandos.
 * 
 * Características:
 * - Previene procesamiento duplicado de comandos
 * - Cache en memoria con expiración automática (producción: usar Redis)
 * - Thread-safe con ConcurrentHashMap
 * 
 * En producción, reemplazar con Redis:
 * - RedisTemplate.opsForValue().setIfAbsent(key, value, timeout, TimeUnit)
 */
@Slf4j
@Service
public class CommandDeduplicationService {

    private final Map<String, ProcessedCommand> processedCommands;
    private final ScheduledExecutorService cleanupScheduler;
    
    // Tiempo de retención en cache (1 hora)
    private static final long RETENTION_MINUTES = 60;

    public CommandDeduplicationService() {
        this.processedCommands = new ConcurrentHashMap<>();
        this.cleanupScheduler = Executors.newSingleThreadScheduledExecutor();
        
        // Limpieza periódica cada 5 minutos
        startCleanupTask();
    }

    /**
     * Intenta marcar un comando como procesado.
     * 
     * @param messageId ID único del comando
     * @return true si es la primera vez que se procesa, false si es duplicado
     */
    public boolean markAsProcessed(String messageId) {
        ProcessedCommand existing = processedCommands.putIfAbsent(
            messageId, 
            new ProcessedCommand(messageId, System.currentTimeMillis())
        );
        
        if (existing == null) {
            log.debug("Comando marcado como procesado: {}", messageId);
            return true; // Primera vez
        } else {
            log.warn("⚠️ Comando duplicado detectado y bloqueado: {}", messageId);
            return false; // Duplicado
        }
    }

    /**
     * Verifica si un comando ya fue procesado
     */
    public boolean wasProcessed(String messageId) {
        return processedCommands.containsKey(messageId);
    }

    /**
     * Remueve un comando del cache (para testing)
     */
    public void remove(String messageId) {
        processedCommands.remove(messageId);
    }

    /**
     * Obtiene estadísticas del cache
     */
    public Map<String, Object> getStats() {
        return Map.of(
            "cachedCommands", processedCommands.size(),
            "retentionMinutes", RETENTION_MINUTES
        );
    }

    /**
     * Limpieza periódica de comandos expirados
     */
    private void startCleanupTask() {
        cleanupScheduler.scheduleAtFixedRate(() -> {
            try {
                long now = System.currentTimeMillis();
                long expirationTime = TimeUnit.MINUTES.toMillis(RETENTION_MINUTES);
                
                int initialSize = processedCommands.size();
                
                processedCommands.entrySet().removeIf(entry -> 
                    (now - entry.getValue().getTimestamp()) > expirationTime
                );
                
                int removed = initialSize - processedCommands.size();
                if (removed > 0) {
                    log.info("Limpieza de cache: {} comandos expirados removidos, {} restantes", 
                        removed, processedCommands.size());
                }
                
            } catch (Exception e) {
                log.error("Error en limpieza de cache", e);
            }
        }, 5, 5, TimeUnit.MINUTES);
    }

    /**
     * Detiene el servicio de limpieza (para testing)
     */
    public void shutdown() {
        cleanupScheduler.shutdown();
    }

    /**
     * Modelo para comando procesado
     */
    private static class ProcessedCommand {
        private final String messageId;
        private final long timestamp;

        public ProcessedCommand(String messageId, long timestamp) {
            this.messageId = messageId;
            this.timestamp = timestamp;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }
}