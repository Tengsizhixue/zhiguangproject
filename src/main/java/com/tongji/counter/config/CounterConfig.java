package com.tongji.counter.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.scheduling.annotation.EnableScheduling;
import com.tongji.counter.event.CounterTopics;

/**
 * 计数模块配置：启用调度与 Kafka，并提供字符串模板。
 */
@Configuration
@EnableScheduling
@EnableKafka
public class CounterConfig {

    /**
     * 自动创建计数事件 Topic（如果不存在）。
     * 分区数3，副本因子1（单节点Kafka）。
     */
    @Bean
    public KafkaAdmin kafkaAdmin(KafkaProperties properties) {
        var props = properties.buildAdminProperties(null);
        return new KafkaAdmin(props);
    }

    @Bean
    public NewTopic counterEventsTopic() {
        return new NewTopic(CounterTopics.EVENTS, 3, (short) 1);
    }

    // ==================== Producer 端 ====================

    @Bean
    public ProducerFactory<String, String> stringProducerFactory(KafkaProperties properties) {
        var props = properties.buildProducerProperties(null);
        return new DefaultKafkaProducerFactory<>(props, new StringSerializer(), new StringSerializer());
    }

    @Bean
    public KafkaTemplate<String, String> stringKafkaTemplate(ProducerFactory<String, String> pf) {
        return new KafkaTemplate<>(pf);
    }

    // ==================== Consumer 端 ====================

    @Bean
    public ConsumerFactory<String, String> stringConsumerFactory(KafkaProperties properties) {
        var props = properties.buildConsumerProperties(null);
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new StringDeserializer());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> cf) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(cf);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }
}