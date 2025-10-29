package com.example.banking.service;

import com.example.banking.api.dto.EventDTO;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
public class EventStoreQueryService {

    private final EventStore eventStore;
    private final ObjectMapper objectMapper;

    public EventStoreQueryService(EventStore eventStore) {
        this.eventStore = eventStore;
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Obtiene todos los eventos de un agregado específico
     */
    public List<EventDTO> getEventsForAggregate(String aggregateId) {
        log.info("Consultando eventos para agregado: {}", aggregateId);
        
        return eventStore.readEvents(aggregateId)
                .asStream()
                .map(this::convertToDTO)
                .collect(Collectors.toList());
    }

    /**
     * Convierte un DomainEventMessage a DTO
     */
    private EventDTO convertToDTO(DomainEventMessage<?> eventMessage) {
        try {
            // Convertir el payload a Map para facilitar la visualización
            Map<String, Object> payloadMap = objectMapper.convertValue(
                    eventMessage.getPayload(), 
                    Map.class
            );

            // Convertir metadata a Map
            Map<String, Object> metaDataMap = eventMessage.getMetaData().entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue
                    ));

            return new EventDTO(
                    eventMessage.getIdentifier(),
                    eventMessage.getAggregateIdentifier(),
                    eventMessage.getSequenceNumber(),
                    eventMessage.getPayloadType().getSimpleName(),
                    eventMessage.getTimestamp(),
                    payloadMap,
                    metaDataMap
            );
        } catch (Exception e) {
            log.error("Error convirtiendo evento a DTO", e);
            
            return new EventDTO(
                    eventMessage.getIdentifier(),
                    eventMessage.getAggregateIdentifier(),
                    eventMessage.getSequenceNumber(),
                    eventMessage.getPayloadType().getSimpleName(),
                    eventMessage.getTimestamp(),
                    Map.of("error", "No se pudo serializar el payload"),
                    Map.of()
            );
        }
    }

    /**
     * Obtiene estadísticas del EventStore
     */
    public Map<String, Object> getEventStoreStats(String aggregateId) {
        List<EventDTO> events = getEventsForAggregate(aggregateId);
        
        return Map.of(
                "aggregateId", aggregateId,
                "totalEvents", events.size(),
                "firstEvent", events.isEmpty() ? null : events.get(0).getTimestamp(),
                "lastEvent", events.isEmpty() ? null : events.get(events.size() - 1).getTimestamp(),
                "eventTypes", events.stream()
                        .map(EventDTO::getEventType)
                        .distinct()
                        .collect(Collectors.toList())
        );
    }
}