package com.tongji.counter.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import com.tongji.counter.event.CounterTopics;

/**
 * 计数模块配置：启用调度与 Kafka 内部主题自动创建。
 *
 * 核心要点：
 *   - @EnableScheduling: 开启定时任务，驱动 flush() 刷写 SDS
 *   - 其他 Kafka 核心组件 (KafkaTemplate, 序列化, 手动 Ack 等) 均由 Spring Boot 根据 application.yml 自动装配。
 */
@Configuration
@EnableScheduling
public class CounterConfig {

    // 定义计数事件 Topic：3 个分区提升并发消费能力，1 个副本（单节点 Kafka）
    // Spring Boot 内置的 KafkaAdmin 会自动发现这个 Bean 并去 Broker 创建主题
    @Bean
    public NewTopic counterEventsTopic() {
        return new NewTopic(CounterTopics.EVENTS, 3, (short) 1);
    }
}
//之前的配置过于冗余，kafka会自动装配好，无需手动配置Bean
//package com.tongji.counter.config;
//
//import org.apache.kafka.clients.admin.NewTopic;
//import org.apache.kafka.common.serialization.StringDeserializer;
//import org.apache.kafka.common.serialization.StringSerializer;
//import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.kafka.annotation.EnableKafka;
//import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
//import org.springframework.kafka.core.ConsumerFactory;
//import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
//import org.springframework.kafka.core.DefaultKafkaProducerFactory;
//import org.springframework.kafka.core.KafkaAdmin;
//import org.springframework.kafka.core.KafkaTemplate;
//import org.springframework.kafka.core.ProducerFactory;
//import org.springframework.kafka.listener.ContainerProperties;
//import org.springframework.scheduling.annotation.EnableScheduling;
//import com.tongji.counter.event.CounterTopics;
//
///**
// * 计数模块配置：启用调度与 Kafka。
// *
// * 核心要点：
// *
// *   {@code @EnableScheduling} — 让 {@code @Scheduled} 生效，驱动每秒 flush 刷写 SDS
// *   {@code @EnableKafka} — 让 {@code @KafkaListener} 生效，消费者能收消息
// *   {@code AckMode.MANUAL} — 手动提交位点，成功才 ACK，失败则重试，保证强一致性
// *
// * 其余 Bean 均为 Spring Kafka 标准样板代码，无需记忆。
// */
//@Configuration
//@EnableScheduling   // 开启定时任务：驱动 flush() 每秒将聚合桶增量刷入 SDS
//@EnableKafka        // 开启 Kafka 监听：让 @KafkaListener 注解生效
//public class CounterConfig {
//
//    // Kafka 管理客户端，用于自动创建 Topic
//    @Bean
//    public KafkaAdmin kafkaAdmin(KafkaProperties properties) {
//        var props = properties.buildAdminProperties(null);
//        return new KafkaAdmin(props);
//    }
//
//    // 定义计数事件 Topic：3 个分区提升并发消费能力，1 个副本（单节点 Kafka）
//    @Bean
//    public NewTopic counterEventsTopic() {
//        return new NewTopic(CounterTopics.EVENTS, 3, (short) 1);
//    }
//
//    // ==================== Producer 端 ====================
//
//    // 生产者工厂：Key 和 Value 均使用 String 序列化器
//    @Bean
//    public ProducerFactory<String, String> stringProducerFactory(KafkaProperties properties) {
//        var props = properties.buildProducerProperties(null);
//        return new DefaultKafkaProducerFactory<>(props, new StringSerializer(), new StringSerializer());
//    }
//
//    // KafkaTemplate：业务代码通过它发送消息（见 CounterEventProducer）
//    @Bean
//    public KafkaTemplate<String, String> stringKafkaTemplate(ProducerFactory<String, String> pf) {
//        return new KafkaTemplate<>(pf);
//    }
//
//    // ==================== Consumer 端 ====================
//
//    // 消费者工厂：Key 和 Value 均使用 String 反序列化器
//    @Bean
//    public ConsumerFactory<String, String> stringConsumerFactory(KafkaProperties properties) {
//        var props = properties.buildConsumerProperties(null);
//        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new StringDeserializer());
//    }
//
//    // ⚠️ 核心配置：手动 ACK 模式
//    // 消费者处理成功后必须调用 ack.acknowledge() 才提交位点
//    // 处理失败不提交，消息会被 Kafka 重新投递，保证不丢
//    @Bean
//    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
//            ConsumerFactory<String, String> cf) {
//        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
//        factory.setConsumerFactory(cf);
//        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
//        return factory;
//    }
//}