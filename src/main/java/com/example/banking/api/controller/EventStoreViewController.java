package com.example.banking.api.controller;

import com.example.banking.infrastructure.eventstore.EventStoreRepository;
import org.axonframework.eventsourcing.eventstore.DomainEventStream;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/eventstore")
public class EventStoreViewController {

    private final EventStore eventStore;
    private final EventStoreRepository eventStoreRepository;

    public EventStoreViewController(EventStore eventStore, EventStoreRepository eventStoreRepository) {
        this.eventStore = eventStore;
        this.eventStoreRepository = eventStoreRepository;
    }

    /**
     * Dashboard principal del Event Store
     * GET /api/eventstore
     */
    @GetMapping
    public ResponseEntity<Map<String, Object>> getDashboard() {
        Map<String, Object> dashboard = new HashMap<>();
        
        try {
            Long totalEvents = eventStoreRepository.countEvents();
            List<String> aggregates = eventStoreRepository.findAllAggregateIds();
            
            dashboard.put("totalEvents", totalEvents);
            dashboard.put("totalAggregates", aggregates.size());
            dashboard.put("aggregateIds", aggregates);
            dashboard.put("endpoints", Map.of(
                "allEvents", "/api/eventstore/events",
                "eventsByAggregate", "/api/eventstore/aggregate/{aggregateId}",
                "eventStream", "/api/eventstore/stream/{aggregateId}"
            ));
            
        } catch (Exception e) {
            dashboard.put("info", "Usando EventStore en memoria - no hay acceso directo a tablas JPA");
            dashboard.put("endpoints", Map.of(
                "eventStream", "/api/eventstore/stream/{aggregateId}"
            ));
        }
        
        return ResponseEntity.ok(dashboard);
    }

    /**
     * Ver todos los eventos (solo funciona con JPA EventStore)
     * GET /api/eventstore/events
     */
    @GetMapping("/events")
    public ResponseEntity<?> getAllEvents() {
        try {
            List<Map<String, Object>> events = eventStoreRepository.findAllEvents();
            return ResponseEntity.ok(Map.of(
                "count", events.size(),
                "events", events
            ));
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of(
                "message", "Esta funcionalidad solo está disponible con JPA EventStore",
                "hint", "Usa /api/eventstore/stream/{aggregateId} para ver eventos de un agregado"
            ));
        }
    }

    /**
     * Ver eventos de un agregado usando EventStore API
     * GET /api/eventstore/stream/{aggregateId}
     */
    @GetMapping("/stream/{aggregateId}")
    public ResponseEntity<Map<String, Object>> getEventStream(
            @PathVariable("aggregateId") String aggregateId) {
        
        try {
            DomainEventStream eventStream = eventStore.readEvents(aggregateId);
            
            List<Map<String, Object>> events = new ArrayList<>();
            
            while (eventStream.hasNext()) {
                var eventMessage = eventStream.next();
                
                Map<String, Object> eventInfo = new HashMap<>();
                eventInfo.put("sequenceNumber", eventMessage.getSequenceNumber());
                eventInfo.put("aggregateIdentifier", eventMessage.getAggregateIdentifier());
                eventInfo.put("timestamp", eventMessage.getTimestamp());
                eventInfo.put("eventType", eventMessage.getPayloadType().getSimpleName());
                eventInfo.put("payload", eventMessage.getPayload());
                eventInfo.put("metadata", eventMessage.getMetaData());
                
                events.add(eventInfo);
            }
            
            Map<String, Object> response = new HashMap<>();
            response.put("aggregateId", aggregateId);
            response.put("eventCount", events.size());
            response.put("events", events);
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            return ResponseEntity.status(404).body(Map.of(
                "error", "No se encontraron eventos para el agregado: " + aggregateId,
                "message", e.getMessage()
            ));
        }
    }

    /**
     * Ver eventos de un agregado usando JPA (más detallado)
     * GET /api/eventstore/aggregate/{aggregateId}
     */
    @GetMapping("/aggregate/{aggregateId}")
    public ResponseEntity<?> getEventsByAggregate(
            @PathVariable("aggregateId") String aggregateId) {
        
        try {
            List<Map<String, Object>> events = eventStoreRepository.findEventsByAggregateId(aggregateId);
            
            return ResponseEntity.ok(Map.of(
                "aggregateId", aggregateId,
                "eventCount", events.size(),
                "events", events
            ));
            
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of(
                "message", "Esta funcionalidad solo está disponible con JPA EventStore",
                "hint", "Usa /api/eventstore/stream/" + aggregateId + " en su lugar"
            ));
        }
    }
}