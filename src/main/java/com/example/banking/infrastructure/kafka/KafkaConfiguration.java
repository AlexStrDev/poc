package com.example.banking.infrastructure.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfiguration {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${kafka.command.group-id:banking-command-handlers}")
    private String commandGroupId;
    
    @Value("${kafka.events.group-id:banking-event-store}")
    private String eventsGroupId;

    /**
     * Configuración del productor de Kafka para comandos y eventos
     */
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.RETRIES_CONFIG, 3);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1); // Garantiza orden
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    /**
     * Configuración del consumidor de Kafka
     */
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, commandGroupId);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Leer desde el inicio
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"); // Solo leer transacciones confirmadas
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(3); // Número de threads concurrentes
        return factory;
    }

    /**
     * Topic para comandos
     */
    @Bean
    public NewTopic commandsTopic() {
        return TopicBuilder.name("axon-commands")
            .partitions(3)
            .replicas(1)
            .config("retention.ms", "604800000") // 7 días
            .config("cleanup.policy", "delete")
            .build();
    }

    /**
     * Topic para eventos (source of truth)
     * Log-based, particionado por aggregateId
     */
    @Bean
    public NewTopic eventsTopic() {
        return TopicBuilder.name("axon-events")
            .partitions(6) // Más particiones para mayor paralelismo
            .replicas(1)
            .config("retention.ms", "-1") // Retención infinita (log compaction)
            .config("cleanup.policy", "compact") // Log compaction para mantener último estado
            .config("min.compaction.lag.ms", "86400000") // 1 día antes de compactar
            .build();
    }

    /**
     * Topic para snapshots
     * Optimización para reconstrucción rápida de agregados
     */
    @Bean
    public NewTopic snapshotsTopic() {
        return TopicBuilder.name("axon-snapshots")
            .partitions(6)
            .replicas(1)
            .config("retention.ms", "-1") // Retención infinita
            .config("cleanup.policy", "compact") // Solo último snapshot por aggregate
            .build();
    }

    /**
     * ObjectMapper para serialización JSON
     */
    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        return mapper;
    }
}