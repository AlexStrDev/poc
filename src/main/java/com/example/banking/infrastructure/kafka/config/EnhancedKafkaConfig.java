package com.example.banking.infrastructure.kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuración mejorada de Kafka con múltiples consumer factories.
 * Usa @Qualifier para evitar ambigüedad de beans.
 */
@Slf4j
@Configuration
@EnableKafka
public class EnhancedKafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    
    @Value("${kafka.events.materializer.group-id}")
    private String materializerGroupId;
    
    @Value("${kafka.command.reply.group-id}")
    private String commandReplyGroupId;

    /**
     * Consumer Factory para materialización de eventos (alta concurrencia)
     */
    @Bean
    @Qualifier("materializerConsumerFactory")
    public ConsumerFactory<String, String> materializerConsumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, materializerGroupId);
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        configProps.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024);
        configProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        
        log.info("✅ Configurando MaterializerConsumer con group-id: {}", materializerGroupId);
        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    /**
     * Container Factory para materialización con alta concurrencia
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> materializerKafkaListenerContainerFactory(
            @Qualifier("materializerConsumerFactory") ConsumerFactory<String, String> materializerConsumerFactory) {
        
        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(materializerConsumerFactory);
        factory.setConcurrency(5);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.getContainerProperties().setSyncCommits(true);
        factory.setBatchListener(false);
        
        log.info("✅ MaterializerKafkaListenerContainerFactory configurado con concurrencia: 5");
        return factory;
    }

    /**
     * Consumer Factory para respuestas de comandos
     */
    @Bean
    @Qualifier("commandReplyConsumerFactory")
    public ConsumerFactory<String, String> commandReplyConsumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, commandReplyGroupId);
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        
        log.info("✅ Configurando CommandReplyConsumer con group-id: {}", commandReplyGroupId);
        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    /**
     * Container Factory para respuestas de comandos ⚠️ CRÍTICO PARA REQUEST-REPLY
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> commandReplyKafkaListenerContainerFactory(
            @Qualifier("commandReplyConsumerFactory") ConsumerFactory<String, String> commandReplyConsumerFactory) {
        
        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(commandReplyConsumerFactory);
        factory.setConcurrency(2);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        
        log.info("✅ CommandReplyKafkaListenerContainerFactory configurado con concurrencia: 2");
        return factory;
    }
}