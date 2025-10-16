package com.example.banking.infrastructure.eventstore;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

/**
 * Repositorio para consultar directamente los eventos almacenados en JPA
 */
@Repository
public class EventStoreRepository {

    @PersistenceContext
    private EntityManager entityManager;

    /**
     * Obtener todos los eventos de la tabla DomainEventEntry
     */
    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> findAllEvents() {
        String query = """
            SELECT 
                e.aggregateIdentifier as aggregateId,
                e.sequenceNumber as sequence,
                e.type as eventType,
                e.timestamp as timestamp,
                e.payloadType as payloadType
            FROM DomainEventEntry e
            ORDER BY e.timestamp ASC
            """;
        
        return entityManager.createQuery(query).getResultList();
    }

    /**
     * Obtener eventos de un agregado específico
     */
    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> findEventsByAggregateId(String aggregateId) {
        String query = """
            SELECT 
                e.aggregateIdentifier as aggregateId,
                e.sequenceNumber as sequence,
                e.type as eventType,
                e.timestamp as timestamp,
                e.payloadType as payloadType,
                e.payload as payload,
                e.metaData as metadata
            FROM DomainEventEntry e
            WHERE e.aggregateIdentifier = :aggregateId
            ORDER BY e.sequenceNumber ASC
            """;
        
        return entityManager.createQuery(query)
                           .setParameter("aggregateId", aggregateId)
                           .getResultList();
    }

    /**
     * Contar total de eventos
     */
    public Long countEvents() {
        String query = "SELECT COUNT(e) FROM DomainEventEntry e";
        return (Long) entityManager.createQuery(query).getSingleResult();
    }

    /**
     * Obtener agregados únicos
     */
    @SuppressWarnings("unchecked")
    public List<String> findAllAggregateIds() {
        String query = "SELECT DISTINCT e.aggregateIdentifier FROM DomainEventEntry e";
        return entityManager.createQuery(query).getResultList();
    }
}