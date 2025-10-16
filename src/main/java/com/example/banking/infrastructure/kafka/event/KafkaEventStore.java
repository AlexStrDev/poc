package com.example.banking.infrastructure.kafka.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * State Store local para eventos.
 * Actúa como caché en memoria para lecturas rápidas.
 * 
 * En producción, esto podría ser:
 * - Kafka Streams State Store
 * - Redis
 * - RocksDB
 */
@Component
public class KafkaEventStore {

    private static final Logger logger = LoggerFactory.getLogger(KafkaEventStore.class);
    
    // Map: aggregateId -> List<AxonEventMessage> ordenados por sequenceNumber
    private final Map<String, List<AxonEventMessage>> eventsByAggregate = new ConcurrentHashMap<>();
    
    // Map: aggregateId -> AxonEventMessage (último snapshot)
    private final Map<String, AxonEventMessage> snapshotsByAggregate = new ConcurrentHashMap<>();
    
    // Map: aggregateId -> último sequenceNumber
    private final Map<String, Long> lastSequenceByAggregate = new ConcurrentHashMap<>();

    /**
     * Almacena un evento en el state store
     */
    public void storeEvent(String aggregateId, AxonEventMessage event) {
        eventsByAggregate.computeIfAbsent(aggregateId, k -> new ArrayList<>())
            .add(event);
        
        lastSequenceByAggregate.put(aggregateId, event.getSequenceNumber());
        
        logger.debug("Stored event seq={} for aggregate {} in local store", 
            event.getSequenceNumber(), aggregateId);
    }

    /**
     * Lee todos los eventos de un agregado desde el snapshot (si existe)
     */
    public List<AxonEventMessage> readEvents(String aggregateId) {
        Optional<AxonEventMessage> snapshot = readSnapshot(aggregateId);
        List<AxonEventMessage> allEvents = eventsByAggregate.getOrDefault(aggregateId, new ArrayList<>());
        
        if (snapshot.isPresent()) {
            long snapshotSequence = snapshot.get().getSequenceNumber();
            
            // Retornar snapshot + eventos posteriores
            List<AxonEventMessage> result = new ArrayList<>();
            result.add(snapshot.get());
            result.addAll(allEvents.stream()
                .filter(e -> e.getSequenceNumber() > snapshotSequence)
                .collect(Collectors.toList()));
            
            logger.debug("Read {} events for aggregate {} (including snapshot at seq={})", 
                result.size(), aggregateId, snapshotSequence);
            
            return result;
        }
        
        logger.debug("Read {} events for aggregate {} (no snapshot)", 
            allEvents.size(), aggregateId);
        
        return new ArrayList<>(allEvents);
    }

    /**
     * Almacena un snapshot
     */
    public void storeSnapshot(String aggregateId, AxonEventMessage snapshot) {
        snapshotsByAggregate.put(aggregateId, snapshot);
        
        // Opcionalmente, limpiar eventos anteriores al snapshot para ahorrar memoria
        long snapshotSequence = snapshot.getSequenceNumber();
        List<AxonEventMessage> events = eventsByAggregate.get(aggregateId);
        if (events != null) {
            events.removeIf(e -> e.getSequenceNumber() < snapshotSequence);
        }
        
        logger.info("Stored snapshot for aggregate {} at sequence {}", 
            aggregateId, snapshotSequence);
    }

    /**
     * Lee el último snapshot de un agregado
     */
    public Optional<AxonEventMessage> readSnapshot(String aggregateId) {
        return Optional.ofNullable(snapshotsByAggregate.get(aggregateId));
    }

    /**
     * Obtiene el último número de secuencia de un agregado
     */
    public Long getLastSequenceNumber(String aggregateId) {
        return lastSequenceByAggregate.get(aggregateId);
    }

    /**
     * Obtiene estadísticas del state store
     */
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalAggregates", eventsByAggregate.size());
        stats.put("totalEvents", eventsByAggregate.values().stream()
            .mapToInt(List::size)
            .sum());
        stats.put("totalSnapshots", snapshotsByAggregate.size());
        return stats;
    }

    /**
     * Limpia el state store (útil para testing)
     */
    public void clear() {
        eventsByAggregate.clear();
        snapshotsByAggregate.clear();
        lastSequenceByAggregate.clear();
        logger.info("Cleared event store");
    }
}