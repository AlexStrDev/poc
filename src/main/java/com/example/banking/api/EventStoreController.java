package com.example.banking.api;

import com.example.banking.api.dto.EventDTO;
import com.example.banking.service.EventStoreQueryService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Controller para consultar el EventStore
 */
@Slf4j
@RestController
@RequestMapping("/api/eventstore")
public class EventStoreController {

    private final EventStoreQueryService eventStoreQueryService;

    public EventStoreController(EventStoreQueryService eventStoreQueryService) {
        this.eventStoreQueryService = eventStoreQueryService;
    }

    /**
     * Obtiene todos los eventos de un agregado específico
     * GET /api/eventstore/aggregate/{aggregateId}
     */
    @GetMapping("/aggregate/{aggregateId}")
    public ResponseEntity<List<EventDTO>> getEventsForAggregate(
            @PathVariable("aggregateId") String aggregateId) {
        
        log.info("Solicitando eventos para agregado: {}", aggregateId);
        
        try {
            List<EventDTO> events = eventStoreQueryService.getEventsForAggregate(aggregateId);
            
            if (events.isEmpty()) {
                log.warn("No se encontraron eventos para el agregado: {}", aggregateId);
                return ResponseEntity.notFound().build();
            }
            
            return ResponseEntity.ok(events);
            
        } catch (Exception e) {
            log.error("Error obteniendo eventos para agregado: {}", aggregateId, e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Obtiene estadísticas de un agregado
     * GET /api/eventstore/aggregate/{aggregateId}/stats
     */
    @GetMapping("/aggregate/{aggregateId}/stats")
    public ResponseEntity<Map<String, Object>> getAggregateStats(
            @PathVariable("aggregateId") String aggregateId) {
        
        log.info("Solicitando estadísticas para agregado: {}", aggregateId);
        
        try {
            Map<String, Object> stats = eventStoreQueryService.getEventStoreStats(aggregateId);
            return ResponseEntity.ok(stats);
            
        } catch (Exception e) {
            log.error("Error obteniendo estadísticas para agregado: {}", aggregateId, e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Endpoint de health check
     * GET /api/eventstore/health
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of(
                "status", "UP",
                "service", "EventStore Query Service"
        ));
    }
}