package com.example.banking.api.controller;

import org.axonframework.eventsourcing.eventstore.DomainEventStream;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/events")
public class EventStoreController {

    private final EventStore eventStore;

    public EventStoreController(EventStore eventStore) {
        this.eventStore = eventStore;
    }

    /**
     * Obtener todos los eventos de un agregado específico
     * GET /api/events/{aggregateId}
     */
    @GetMapping("/{aggregateId}")
    public ResponseEntity<Map<String, Object>> getEventsForAggregate(
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
                eventInfo.put("payloadType", eventMessage.getPayloadType().getName());
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
            Map<String, Object> error = new HashMap<>();
            error.put("error", "No se encontraron eventos para el agregado: " + aggregateId);
            error.put("message", e.getMessage());
            return ResponseEntity.notFound().build();
        }
    }

    /**
     * Obtener un resumen de todos los eventos
     * GET /api/events
     */
    @GetMapping
    public ResponseEntity<Map<String, Object>> getAllEventsInfo() {
        Map<String, Object> info = new HashMap<>();
        info.put("message", "Para ver eventos de un agregado específico, usa: GET /api/events/{aggregateId}");
        info.put("example", "/api/events/abc-123-xyz");
        return ResponseEntity.ok(info);
    }
}