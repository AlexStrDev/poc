package com.example.banking.infrastructure.kafka.storage;

import com.example.banking.infrastructure.kafka.serializer.KafkaEventSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.axonframework.common.jpa.EntityManagerProvider;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventsourcing.eventstore.jpa.DomainEventEntry;
import org.axonframework.serialization.Serializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import jakarta.persistence.EntityManager;
import java.time.Duration;
import java.util.*;

/**
 * Materializa eventos desde Kafka a PostgreSQL (lazy-load)
 */
@Slf4j
@Component
public class EventStoreMaterializer {

    private final EntityManagerProvider entityManagerProvider;
    private final KafkaEventSerializer eventSerializer;
    private final Serializer axonSerializer;
    private final String eventTopic;
    private final KafkaConsumer<String, String> kafkaConsumer;

    public EventStoreMaterializer(
            EntityManagerProvider entityManagerProvider,
            KafkaEventSerializer eventSerializer,
            Serializer defaultSerializer,
            @Value("${kafka.events.topic}") String eventTopic,
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        
        this.entityManagerProvider = entityManagerProvider;
        this.eventSerializer = eventSerializer;
        this.axonSerializer = defaultSerializer;
        this.eventTopic = eventTopic;
        
        // Crear consumer dedicado para materialización
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "event-materializer-" + UUID.randomUUID().toString());
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        
        this.kafkaConsumer = new KafkaConsumer<>(props);
    }

    /**
     * Verifica si un aggregate está materializado en PostgreSQL
     */
    public boolean isMaterialized(String aggregateIdentifier) {
        EntityManager em = entityManagerProvider.getEntityManager();
        
        try {
            Long count = em.createQuery(
                "SELECT COUNT(e) FROM DomainEventEntry e WHERE e.aggregateIdentifier = :aggId",
                Long.class)
                .setParameter("aggId", aggregateIdentifier)
                .getSingleResult();
            
            return count > 0;
            
        } catch (Exception e) {
            log.warn("Error verificando materialización de aggregate {}: {}", 
                aggregateIdentifier, e.getMessage());
            return false;
        }
    }

    /**
     * Marca un aggregate como materializado (después de persistir)
     */
    @Transactional
    public void markAsMaterialized(String aggregateIdentifier) {
        log.debug("Aggregate {} marcado como materializado", aggregateIdentifier);
        // La existencia de eventos en DomainEventEntry ya indica que está materializado
    }

    /**
     * Materializa un aggregate desde Kafka a PostgreSQL
     */
    @Transactional
    public void materializeFromKafka(String aggregateIdentifier) {
        log.info("Materializando aggregate {} desde Kafka...", aggregateIdentifier);
        
        EntityManager em = entityManagerProvider.getEntityManager();
        List<DomainEventMessage<?>> events = new ArrayList<>();
        
        try {
            // Asignar todas las particiones del topic
            List<TopicPartition> partitions = new ArrayList<>();
            kafkaConsumer.partitionsFor(eventTopic).forEach(info -> 
                partitions.add(new TopicPartition(eventTopic, info.partition()))
            );
            kafkaConsumer.assign(partitions);
            
            // Seek al inicio
            kafkaConsumer.seekToBeginning(partitions);
            
            // Leer todos los mensajes buscando el aggregate
            boolean found = false;
            long startTime = System.currentTimeMillis();
            long timeout = 30000; // 30 segundos timeout
            
            while (System.currentTimeMillis() - startTime < timeout) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
                
                if (records.isEmpty()) {
                    if (found) break; // Ya encontramos eventos y llegamos al final
                    continue;
                }
                
                for (ConsumerRecord<String, String> record : records) {
                    // Filtrar por aggregateIdentifier (viene en la key)
                    if (aggregateIdentifier.equals(record.key())) {
                        try {
                            DomainEventMessage<?> event = eventSerializer.deserialize(record.value());
                            events.add(event);
                            found = true;
                            
                            log.debug("Evento {} encontrado en Kafka para aggregate {}", 
                                event.getSequenceNumber(), aggregateIdentifier);
                                
                        } catch (Exception e) {
                            log.error("Error deserializando evento de Kafka", e);
                        }
                    }
                }
            }
            
            if (events.isEmpty()) {
                log.warn("No se encontraron eventos para aggregate {} en Kafka", aggregateIdentifier);
                return;
            }
            
            // Ordenar eventos por secuencia
            events.sort(Comparator.comparingLong(DomainEventMessage::getSequenceNumber));
            
            // Persistir eventos en PostgreSQL
            log.info("Persistiendo {} eventos de aggregate {} en PostgreSQL", 
                events.size(), aggregateIdentifier);
            
            for (DomainEventMessage<?> event : events) {
                DomainEventEntry entry = new DomainEventEntry(event, axonSerializer);
                em.persist(entry);
            }
            
            em.flush();
            log.info("Aggregate {} materializado exitosamente ({} eventos)", 
                aggregateIdentifier, events.size());
            
        } catch (Exception e) {
            log.error("Error materializando aggregate desde Kafka", e);
            throw new RuntimeException("Error en materialización desde Kafka", e);
        }
    }
}