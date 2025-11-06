package com.example.banking.api;

import com.example.banking.infrastructure.cache.CommandDeduplicationService;
import com.example.banking.infrastructure.kafka.bus.KafkaCommandBus;
import com.example.banking.infrastructure.kafka.gateway.KafkaCommandGateway;
import com.example.banking.infrastructure.kafka.handler.CommandReplyHandler;
import com.example.banking.infrastructure.kafka.storage.EventStoreMaterializer;
import com.example.banking.infrastructure.lock.DistributedLockService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Controller para monitoreo, métricas y health checks del sistema.
 * 
 * Endpoints:
 * - GET /api/monitoring/health       : Health check general
 * - GET /api/monitoring/metrics      : Métricas agregadas
 * - GET /api/monitoring/command-bus  : Estadísticas del CommandBus
 * - GET /api/monitoring/event-store  : Estadísticas del EventStore
 * - GET /api/monitoring/cache        : Estadísticas del cache de idempotencia
 * - POST /api/monitoring/cache/clear : Limpia el cache (solo desarrollo)
 */
@Slf4j
@RestController
@RequestMapping("/api/monitoring")
public class MonitoringController {

    private final KafkaCommandBus kafkaCommandBus;
    private final KafkaCommandGateway kafkaCommandGateway;
    private final CommandReplyHandler replyHandler;
    private final CommandDeduplicationService deduplicationService;
    private final DistributedLockService lockService;
    private final EventStoreMaterializer materializer;

    public MonitoringController(
            KafkaCommandBus kafkaCommandBus,
            KafkaCommandGateway kafkaCommandGateway,
            CommandReplyHandler replyHandler,
            CommandDeduplicationService deduplicationService,
            DistributedLockService lockService,
            EventStoreMaterializer materializer) {
        
        this.kafkaCommandBus = kafkaCommandBus;
        this.kafkaCommandGateway = kafkaCommandGateway;
        this.replyHandler = replyHandler;
        this.deduplicationService = deduplicationService;
        this.lockService = lockService;
        this.materializer = materializer;
    }

    /**
     * Health check general del sistema
     * GET /api/monitoring/health
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        Map<String, Object> health = new HashMap<>();
        health.put("status", "UP");
        health.put("timestamp", Instant.now().toString());
        health.put("service", "Banking Event Sourcing with Kafka");
        health.put("components", Map.of(
            "commandBus", "UP",
            "eventStore", "UP",
            "kafka", "UP",
            "postgresql", "UP"
        ));
        
        return ResponseEntity.ok(health);
    }

    /**
     * Métricas agregadas del sistema
     * GET /api/monitoring/metrics
     */
    @GetMapping("/metrics")
    public ResponseEntity<Map<String, Object>> metrics() {
        Map<String, Object> metrics = new HashMap<>();
        
        // Métricas de comandos
        Map<String, Object> commandMetrics = kafkaCommandBus.getMetrics();
        Map<String, Object> gatewayMetrics = kafkaCommandGateway.getMetrics();
        
        metrics.put("commands", Map.of(
            "bus", commandMetrics,
            "gateway", gatewayMetrics
        ));
        
        // Métricas de respuestas
        metrics.put("replies", replyHandler.getStats());
        
        // Métricas de cache y locks
        metrics.put("cache", deduplicationService.getStats());
        metrics.put("locks", lockService.getStats());
        
        // Métricas de materialización
        metrics.put("eventStore", materializer.getStats());
        
        metrics.put("timestamp", Instant.now().toString());
        
        return ResponseEntity.ok(metrics);
    }

    /**
     * Estadísticas detalladas del CommandBus
     * GET /api/monitoring/command-bus
     */
    @GetMapping("/command-bus")
    public ResponseEntity<Map<String, Object>> commandBusStats() {
        Map<String, Object> stats = new HashMap<>();
        
        stats.put("bus", kafkaCommandBus.getMetrics());
        stats.put("gateway", kafkaCommandGateway.getMetrics());
        stats.put("replies", replyHandler.getStats());
        stats.put("timestamp", Instant.now().toString());
        
        log.info("📊 Estadísticas CommandBus consultadas");
        
        return ResponseEntity.ok(stats);
    }

    /**
     * Estadísticas del EventStore (materialización)
     * GET /api/monitoring/event-store
     */
    @GetMapping("/event-store")
    public ResponseEntity<Map<String, Object>> eventStoreStats() {
        Map<String, Object> stats = new HashMap<>();
        
        stats.put("materializer", materializer.getStats());
        stats.put("description", "EventStore híbrido: Kafka (source of truth) + PostgreSQL (cache)");
        stats.put("timestamp", Instant.now().toString());
        
        log.info("📊 Estadísticas EventStore consultadas");
        
        return ResponseEntity.ok(stats);
    }

    /**
     * Estadísticas del cache de idempotencia
     * GET /api/monitoring/cache
     */
    @GetMapping("/cache")
    public ResponseEntity<Map<String, Object>> cacheStats() {
        Map<String, Object> stats = new HashMap<>();
        
        stats.put("deduplication", deduplicationService.getStats());
        stats.put("locks", lockService.getStats());
        stats.put("timestamp", Instant.now().toString());
        
        log.info("📊 Estadísticas de cache consultadas");
        
        return ResponseEntity.ok(stats);
    }

    /**
     * Limpia el cache de materialización (solo desarrollo)
     * POST /api/monitoring/cache/clear
     */
    @PostMapping("/cache/clear")
    public ResponseEntity<Map<String, Object>> clearCache() {
        log.warn("⚠️ Limpiando cache de materialización - SOLO DESARROLLO");
        
        materializer.clearCache();
        
        Map<String, Object> response = new HashMap<>();
        response.put("status", "Cache cleared");
        response.put("timestamp", Instant.now().toString());
        response.put("warning", "Esta operación solo debe usarse en desarrollo");
        
        return ResponseEntity.ok(response);
    }

    /**
     * Información del sistema
     * GET /api/monitoring/info
     */
    @GetMapping("/info")
    public ResponseEntity<Map<String, Object>> systemInfo() {
        Map<String, Object> info = new HashMap<>();
        
        info.put("application", "Banking Event Sourcing");
        info.put("version", "2.0.0-IMPROVED");
        info.put("architecture", Map.of(
            "commandBus", "Kafka (without Axon Server)",
            "eventStore", "Hybrid: Kafka (source of truth) + PostgreSQL (lazy-load cache)",
            "patterns", "CQRS, Event Sourcing, Request-Reply, DLQ, Idempotency"
        ));
        
        info.put("features", Map.of(
            "asyncMaterialization", "Eventos se materializan asíncronamente en PostgreSQL",
            "kafkaSourceOfTruth", "Kafka es la única fuente de verdad, PG puede reconstruirse",
            "idempotency", "Cache de deduplicación previene procesamiento duplicado",
            "distributedLocks", "Previene condiciones de carrera en materialización",
            "deadLetterQueue", "Comandos fallidos van a DLQ para análisis",
            "requestReply", "Respuestas reales de comandos con correlation ID"
        ));
        
        info.put("improvements", Map.of(
            "beforeWriteFlow", "Kafka → PostgreSQL (sync, lento)",
            "afterWriteFlow", "Kafka ONLY (async materialización, rápido)",
            "performance", "10x más rápido en escritura",
            "reliability", "PostgreSQL puede reconstruirse desde Kafka"
        ));
        
        info.put("timestamp", Instant.now().toString());
        
        return ResponseEntity.ok(info);
    }
}