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
import java.util.concurrent.ConcurrentHashMap;

/**
 * Materializa eventos desde Kafka a PostgreSQL con optimizaciones:
 * 
 * ✅ Cache en memoria de aggregates materializados
 * ✅ Lectura eficiente con early-stop cuando se encuentra último evento
 * ✅ Batch processing para mejor performance
 * ✅ Timeout configurable
 * 
 * NOTA IMPORTANTE:
 * Para máxima eficiencia en producción, usar Kafka Compacted Topics:
 * - Configurar: cleanup.policy=compact
 * - Kafka retiene último evento por key (aggregateId)
 * - Elimina la necesidad de leer todo el tópico
 */
@Slf4j
@Component
public class EventStoreMaterializer {

    private final EntityManagerProvider entityManagerProvider;
    private final KafkaEventSerializer eventSerializer;
    private final Serializer axonSerializer;
    private final String eventTopic;
    private final String bootstrapServers;
    private final long timeoutSeconds;
    
    // Cache de aggregates materializados (reduce queries a PG)
    private final Set<String> materializedCache;
    
    // Pool de consumers (reutilizar en lugar de crear cada vez)
    private final Queue<KafkaConsumer<String, String>> consumerPool;
    private final int maxPoolSize = 5;

    public EventStoreMaterializer(
            EntityManagerProvider entityManagerProvider,
            KafkaEventSerializer eventSerializer,
            Serializer defaultSerializer,
            @Value("${kafka.events.topic}") String eventTopic,
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${kafka.materializer.timeout.seconds:30}") long timeoutSeconds) {
        
        this.entityManagerProvider = entityManagerProvider;
        this.eventSerializer = eventSerializer;
        this.axonSerializer = defaultSerializer;
        this.eventTopic = eventTopic;
        this.bootstrapServers = bootstrapServers;
        this.timeoutSeconds = timeoutSeconds;
        this.materializedCache = ConcurrentHashMap.newKeySet();
        this.consumerPool = new LinkedList<>();
    }

    /**
     * ✅ MEJORADO: Verifica con cache en memoria primero
     */
    public boolean isMaterialized(String aggregateIdentifier) {
        // 1. Verificar cache en memoria (ultrarrápido)
        if (materializedCache.contains(aggregateIdentifier)) {
            log.debug("✅ Cache hit (memoria): {}", aggregateIdentifier);
            return true;
        }
        
        // 2. Verificar en PostgreSQL
        EntityManager em = entityManagerProvider.getEntityManager();
        
        try {
            Long count = em.createQuery(
                "SELECT COUNT(e) FROM DomainEventEntry e WHERE e.aggregateIdentifier = :aggId",
                Long.class)
                .setParameter("aggId", aggregateIdentifier)
                .setMaxResults(1)
                .getSingleResult();
            
            boolean exists = count > 0;
            
            if (exists) {
                // Agregar a cache para futuras consultas
                materializedCache.add(aggregateIdentifier);
                log.debug("✅ Cache miss (memoria) pero encontrado en PG: {}", aggregateIdentifier);
            }
            
            return exists;
            
        } catch (Exception e) {
            log.warn("Error verificando materialización de aggregate {}: {}", 
                aggregateIdentifier, e.getMessage());
            return false;
        }
    }

    /**
     * Marca un aggregate como materializado
     */
    @Transactional
    public void markAsMaterialized(String aggregateIdentifier) {
        materializedCache.add(aggregateIdentifier);
        log.debug("✅ Aggregate marcado como materializado: {}", aggregateIdentifier);
    }

    /**
     * ✅ MEJORADO: Materializa desde Kafka con optimizaciones
     */
    @Transactional
    public void materializeFromKafka(String aggregateIdentifier) {
        log.info("🔄 Materializando aggregate {} desde Kafka (source of truth)...", aggregateIdentifier);
        
        EntityManager em = entityManagerProvider.getEntityManager();
        List<DomainEventMessage<?>> events = new ArrayList<>();
        KafkaConsumer<String, String> consumer = null;
        
        try {
            // Obtener consumer del pool o crear uno nuevo
            consumer = getConsumerFromPool();
            
            // Asignar todas las particiones del topic
            List<TopicPartition> partitions = new ArrayList<>();
            consumer.partitionsFor(eventTopic).forEach(info -> 
                partitions.add(new TopicPartition(eventTopic, info.partition()))
            );
            consumer.assign(partitions);
            
            // Seek al inicio
            consumer.seekToBeginning(partitions);
            
            // Leer eventos con early-stop optimizado
            boolean found = false;
            long startTime = System.currentTimeMillis();
            long timeoutMillis = timeoutSeconds * 1000;
            long lastEventSeq = -1;
            
            while (System.currentTimeMillis() - startTime < timeoutMillis) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                
                if (records.isEmpty()) {
                    if (found) {
                        // Ya encontramos eventos y no hay más
                        log.debug("✅ Fin del stream alcanzado");
                        break;
                    }
                    continue;
                }
                
                for (ConsumerRecord<String, String> record : records) {
                    // Filtrar por aggregateIdentifier (key)
                    if (aggregateIdentifier.equals(record.key())) {
                        try {
                            DomainEventMessage<?> event = eventSerializer.deserialize(record.value());
                            events.add(event);
                            lastEventSeq = event.getSequenceNumber();
                            found = true;
                            
                            log.debug("📥 Evento #{} encontrado en Kafka - Offset: {}", 
                                event.getSequenceNumber(), record.offset());
                                
                        } catch (Exception e) {
                            log.error("Error deserializando evento de Kafka", e);
                        }
                    }
                }
            }
            
            if (events.isEmpty()) {
                log.warn("⚠️ No se encontraron eventos para aggregate {} en Kafka", aggregateIdentifier);
                return;
            }
            
            // Ordenar eventos por secuencia
            events.sort(Comparator.comparingLong(DomainEventMessage::getSequenceNumber));
            
            log.info("📦 Persistiendo {} eventos de aggregate {} en PostgreSQL (seq: 0-{})", 
                events.size(), aggregateIdentifier, lastEventSeq);
            
            // Persistir en batch para mejor performance
            int batchSize = 50;
            for (int i = 0; i < events.size(); i++) {
                DomainEventMessage<?> event = events.get(i);
                
                // Verificar que no exista (idempotencia)
                Long count = em.createQuery(
                    "SELECT COUNT(e) FROM DomainEventEntry e " +
                    "WHERE e.aggregateIdentifier = :aggId AND e.sequenceNumber = :seq",
                    Long.class)
                    .setParameter("aggId", event.getAggregateIdentifier())
                    .setParameter("seq", event.getSequenceNumber())
                    .getSingleResult();
                
                if (count == 0) {
                    DomainEventEntry entry = new DomainEventEntry(event, axonSerializer);
                    em.persist(entry);
                }
                
                // Flush cada batch
                if (i > 0 && i % batchSize == 0) {
                    em.flush();
                    em.clear();
                    log.debug("✅ Batch {} persistido ({}/{})", i/batchSize, i, events.size());
                }
            }
            
            em.flush();
            
            // Marcar como materializado
            markAsMaterialized(aggregateIdentifier);
            
            log.info("✅ Aggregate {} materializado exitosamente: {} eventos, última seq: {}", 
                aggregateIdentifier, events.size(), lastEventSeq);
            
        } catch (Exception e) {
            log.error("💥 Error materializando aggregate desde Kafka", e);
            throw new RuntimeException("Error en materialización desde Kafka", e);
            
        } finally {
            // Devolver consumer al pool
            if (consumer != null) {
                returnConsumerToPool(consumer);
            }
        }
    }

    /**
     * Obtiene un consumer del pool o crea uno nuevo
     */
    private KafkaConsumer<String, String> getConsumerFromPool() {
        synchronized (consumerPool) {
            if (!consumerPool.isEmpty()) {
                log.debug("♻️ Reutilizando consumer del pool");
                return consumerPool.poll();
            }
        }
        
        log.debug("🆕 Creando nuevo consumer");
        return createConsumer();
    }

    /**
     * Devuelve un consumer al pool
     */
    private void returnConsumerToPool(KafkaConsumer<String, String> consumer) {
        synchronized (consumerPool) {
            if (consumerPool.size() < maxPoolSize) {
                consumerPool.offer(consumer);
                log.debug("♻️ Consumer devuelto al pool ({}/{})", 
                    consumerPool.size(), maxPoolSize);
            } else {
                consumer.close();
                log.debug("🗑️ Consumer cerrado (pool lleno)");
            }
        }
    }

    /**
     * Crea un nuevo consumer de Kafka
     */
    private KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "event-materializer-" + UUID.randomUUID().toString());
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("max.poll.records", "500"); // Batch processing
        
        return new KafkaConsumer<>(props);
    }

    /**
     * Obtiene estadísticas del materializador
     */
    public Map<String, Object> getStats() {
        synchronized (consumerPool) {
            return Map.of(
                "cachedAggregates", materializedCache.size(),
                "consumerPoolSize", consumerPool.size(),
                "maxPoolSize", maxPoolSize,
                "timeoutSeconds", timeoutSeconds
            );
        }
    }

    /**
     * Limpia el cache (para testing)
     */
    public void clearCache() {
        materializedCache.clear();
        log.info("🗑️ Cache limpiado");
    }

    /**
     * Cierra todos los consumers del pool
     */
    public void shutdown() {
        synchronized (consumerPool) {
            while (!consumerPool.isEmpty()) {
                KafkaConsumer<String, String> consumer = consumerPool.poll();
                consumer.close();
            }
            log.info("🛑 Todos los consumers cerrados");
        }
    }
}