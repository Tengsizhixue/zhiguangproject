package com.tongji.counter.event;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * 计数事件生产者。
 *
 * <p>职责：将业务产生的计数增量事件异步发送到 Kafka 主题，供聚合消费者处理。</p>
 */
@Slf4j
@Service
public class CounterEventProducer {
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper objectMapper;

    public CounterEventProducer(KafkaTemplate<String, String> kafka, ObjectMapper objectMapper) {
        this.kafka = kafka;
        this.objectMapper = objectMapper;
    }

    /**
     * 发布计数事件到 Kafka。
     * @param event 计数事件（实体类型、ID、指标、delta 等）
     */
    public void publish(CounterEvent event) {
        try {
            // 序列化事件为 JSON 字符串，不是java自带的是jackson的
            String payload = objectMapper.writeValueAsString(event);
            kafka.send(CounterTopics.EVENTS, payload); // 异步写入计数事件主题（幂等生产已在配置启用）
            log.info("Kafka计数事件已发送: topic={}, payload={}", CounterTopics.EVENTS, payload);
        } catch (JsonProcessingException e) {
            log.error("Kafka计数事件序列化失败: entityType={}, entityId={}", event.getEntityType(), event.getEntityId(), e);
            // 生产异常不抛出影响主流程；可接入告警
        }
    }
}