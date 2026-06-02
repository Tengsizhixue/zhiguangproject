````mermaid
sequenceDiagram
    autonumber
    
    box rgb(240, 248, 255) 专线 A：普通直发模式 (用于计数器)
        participant Biz1 as 业务层 (发帖/点赞)
        participant Prod1 as CounterEventProducer
        participant Kafka1 as Topic: counter-events
        participant Cons1 as 计数聚合消费者
    end

    Biz1->>Prod1: 产生点赞事件
    Prod1->>Kafka1: kafka.send() 异步发送
    Kafka1->>Cons1: 拉取消费并更新 Redis SDS
    
    box rgb(255, 245, 238) 专线 B：事务发件箱模式 (用于核心状态解耦)
        participant Biz2 as 业务层 (如发布帖子)
        participant DB as MySQL (outbox表)
        participant Canal as CanalKafkaBridge
        participant Kafka2 as Topic: canal-outbox
        participant Cons2 as CanalOutboxConsumer
    end

    Biz2->>DB: 1. 更新业务表 + 2. 写入 outbox (同事务)
    Note over DB, Canal: MySQL 产生底层 Binlog 日志
    Canal->>DB: 模拟从库拉取 Binlog
    Canal->>Kafka2: 解析并 kafka.send()
    Kafka2->>Cons2: @KafkaListener 消费
    Cons2->>Cons2: 提取 Payload 并处理，手动 ACK