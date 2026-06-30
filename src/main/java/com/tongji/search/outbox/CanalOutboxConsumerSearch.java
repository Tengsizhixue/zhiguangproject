package com.tongji.search.outbox;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tongji.relation.outbox.OutboxTopics;
import com.tongji.common.util.OutboxMessageUtil;
import com.tongji.search.index.SearchIndexService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.List;

/**它是"三通道同步架构"中的增量兜底通道——监听 Kafka 中 Canal 投递的 MySQL 变更事件，
 * 把知文的增删改同步到 ES，保证即使业务直写 ES 失败，最终也能通过 Canal 补偿一致。
 * Canal 发现 MySQL 里有人改了一篇文章
 *   → 把变更信息打包成 JSON，扔进 Kafka 的 "canal-outbox" 快递柜
 *   → 这个类的 @KafkaListener 守在快递柜旁，有包裹就拿
 *   → 拆包裹，里面是"文章 100 被更新了"
 *   → 调用 SearchIndexService 同步到 ES
 *   → 签收确认，完事
 * 搜索索引的 Outbox 消费者：监听 Canal 投递到 Kafka 的增量变更消息，驱动 ES 索引的增删改。
 *
 * 面试自述：这个类就是"三通道同步架构"中"增量兜底通道"的消费者。它的上游是 Canal →
 * Outbox → Kafka，下游是 SearchIndexService。当知文在 MySQL 中发生变更时，
 * Canal 捕获 binlog 事件，投递到 Kafka Topic，本类消费后调用 SearchIndexService
 * 同步到 ES。
 *
 * 这类消息走的是"至少一次"语义，所以写入逻辑被设计为幂等——upsert 可以重复执行，
 * 不会产生脏数据。消费完成后手动提交 Offset，保证不丢消息。
 *
 MySQL 业务代码:
 INSERT INTO know_posts (...) VALUES (...)
 INSERT INTO outbox (payload) VALUES ('{"entity":"knowpost","op":"insert","id":"100"}')
 ↓
 Canal 监听 binlog，发现 outbox 表有 INSERT
 ↓
 Canal 组装 JSON 消息投递到 Kafka Topic "canal-outbox":
 {
 "table": "outbox",
 "type": "INSERT",
 "data": [{
 "id": "5001",
 "payload": "{\"entity\":\"knowpost\",\"op\":\"insert\",\"id\":\"100\"}"
 }]
 }
 ↓
 CanalOutboxConsumerSearch.onMessage() 收到这条消息
 → extractRows 解析出 data 数组
 → 遍历每行，读 payload 字段
 → 二次解析 payload：entity=knowpost → 调用 upsertKnowPost(100)
 → ACK 签收
 */
@Service
@RequiredArgsConstructor
public class CanalOutboxConsumerSearch {
    private final ObjectMapper objectMapper;
    private final SearchIndexService indexService;

    /**
     * 消费 Canal Outbox 消息，解析变更行并按实体类型更新 ES 索引。
     *
     * @KafkaListener 注解让 Spring Kafka 自动管理消费者线程，topic 是 canal-outbox，
     * 同一个 groupId 的消费者共享分区，实现负载均衡。
     *
     * 为什么用手动 ACK（Acknowledgment）而不是自动提交？
     * 自动提交可能在消息处理失败时仍然提交 Offset，导致消息丢失。
     * 手动 ACK 确保"只有处理成功后"才提交 Offset，保证至少一次语义。
     */
    // 我要监听"canal-outbox"号快递柜，我是"search-index-consumer"取件团队的
    @KafkaListener(topics = OutboxTopics.CANAL_OUTBOX, groupId = "search-index-" +
            "consumer")
    public void onMessage(String message, Acknowledgment ack) {
        try {
            // 用 OutboxMessageUtil 提取消息中的变更行列表
            // 一条 Kafka 消息可能包含多行变更（同一事务的批量操作），
            // 所以返回的是 List<JsonNode>，不是单个对象
            List<JsonNode> rows = OutboxMessageUtil.extractRows(objectMapper, message);

            // 空消息直接 ACK，不阻塞后续消费
            if (rows.isEmpty()) {
                ack.acknowledge();
                return;
            }

            // 遍历每一行变更，按 entity 和 op 决定处理方式
            for (JsonNode row : rows) {
                JsonNode payloadNode = row.get("payload");
                if (payloadNode == null) {
                    continue;  // 无效行，跳过
                }

                // payload 字段是二次 JSON 字符串，需要再解析一次
                JsonNode payload = objectMapper.readTree(payloadNode.asText());
                String entity = text(payload.get("entity"));  // 实体类型：knowpost / comment 等
                String op = text(payload.get("op"));          // 操作类型：insert / update / delete
                Long id = asLong(payload.get("id"));          // 实体主键 ID

                // 只处理知文类型，其他实体（如评论、用户）由各自的消费者处理
                if (!"knowpost".equals(entity) || id == null) {
                    continue;
                }

                // 删除操作走软删除（只改 status 字段），
                // 新增和更新都走 upsert（覆盖写入同一文档 ID），天然幂等，可重复消费
                if ("delete".equalsIgnoreCase(op)) {
                    indexService.softDeleteKnowPost(id);
                } else {
                    indexService.upsertKnowPost(id);
                }
            }
            // 所有行处理完毕后，手动提交 Offset，确认消费完成
            // 如果这之前抛异常，Offset 不会提交，下次重启会重新消费，保证不丢消息
            ack.acknowledge();
        } catch (Exception ignored) {
            // 异常被吞掉，不 ACK——消息会留在 Kafka 等待重试
            // 生产环境建议加上死信队列，避免单条毒消息阻塞整个分区
        }
    }

    /**
     * 安全提取 JsonNode 的文本值，null 安全。
     */
    private String text(JsonNode n) {
        return n == null ? null : n.asText();
    }

    /**
     * 安全提取 JsonNode 的 Long 值，null 安全 + 格式容错。
     * 用 Long.parseLong 而不是 n.asLong()，因为 outbox 消息中的数字字段可能是字符串格式。
     */
    private Long asLong(JsonNode n) {
        if (n == null) {
            return null;
        }

        try {
            return Long.parseLong(n.asText());
        } catch (Exception e) {
            return null;  // 非数字字符串，返回 null 让上层跳过
        }
    }
}
//1. Kafka 是什么？—— 快递公司
//把 Kafka 想象成一家超级快递公司：
//
//Kafka 概念	类比	代码中的体现
//Topic（主题）	快递柜的柜子编号	OutboxTopics.CANAL_OUTBOX = "canal-outbox" 号快递柜
//Producer（生产者）	往快递柜里放包裹的人	Canal 把 MySQL 变更事件打包放进去
//Consumer（消费者）	从快递柜里取包裹的人	这个类就是取快递的
//Message（消息）	包裹本身	一条 JSON 字符串，里面装着"哪篇文章被改了"
//Offset（位点/偏移量）	你取到第几个包裹了	记录了"这柜子里的包裹我取到第 5 个了"
//ACK（确认）	签收单	你签了字，快递员才知道你收到了
//GroupId（消费者组）	取件团队名	同一个团队的人不会重复取同一个包裹
//2. 流程类比：点外卖
//你最熟悉的外卖流程，其实就是 Kafka：
//
//
//Plain Text
//
//你点了一份外卖（Canal 发现 MySQL 数据变了）
//        ↓
//商家出餐，放到外卖柜 3 号格子（Producer 投递消息到 Topic）
//        ↓
//骑手从 3 号格子取餐（Consumer 从 Topic 拉消息）
//        ↓
//骑手送到你手上，你确认收货（Consumer 处理完，ACK 确认）
//        ↓
//外卖柜这个格子可以放下一单了（Offset 提交，消息不再重复投递）
//如果骑手取餐后摔了一跤，外卖洒了（处理异常）：
//
//他没确认收货（没 ACK）
//系统会重新派单（消息下次还会被消费）
//这就是"至少一次"语义
//3. 对应到代码中
//
//        Java
//
//// 这个注解的意思是：
//// 我要监听"canal-outbox"号快递柜，我是"search-index-consumer"取件团队的
//@KafkaListener(topics = OutboxTopics.CANAL_OUTBOX, groupId = "search-index-consumer")
//public void onMessage(String message, Acknowledgment ack) {
//    // message = 包裹内容，是一个 JSON 字符串
//    // ack = 签收单，处理完才能签
//    Topic（快递柜）：canal-outbox，Canal 把所有 MySQL 变更都扔这个柜子里，包括知文、评论、用户等各种变更。
//
//    为什么不每个实体一个 Topic？ 就像外卖柜不会分" 汉堡柜"和"奶茶柜"——一个柜子就够了，消费者自己看包裹标签判断是不是自己的。
//
//    4. 变更行列表（List<JsonNode> rows）是什么？
//
//    Java
//
//    List<JsonNode> rows = OutboxMessageUtil.extractRows(objectMapper, message);
//    类比：一个包裹里可能装了好几件商品。
//
//    比如你在淘宝一次性买了 3 件衣服，卖家可能打包成一个包裹发给你。MySQL 的一个事务里可能同时改了 3 篇文章，Canal 会把这 3 条变更打包成一条 Kafka 消息。
//
//
//    Plain Text
//
//    包裹（一条 Kafka 消息）:
//  ├── 变更行 1: 文章 ID=100，操作=insert
//  ├── 变更行 2: 文章 ID=200，操作=update
//  └── 变更行 3: 文章 ID=300，操作=delete
//    所以 extractRows 就是拆包裹——把一条消息拆成多个变更行，然后逐个处理。
//
//    5. ACK（签收确认）为什么重要？
//
//    Java
//
//    ack.acknowledge();  // 签收！
//    不签收的后果：包裹会被认定为"未送达"，系统会重新投递。
//
//
//    Java
//
//    try {
//        // 处理包裹...
//        ack.acknowledge();  // ✅ 处理成功，签收
//    } catch (Exception ignored) {
//        // ❌ 处理失败，不签收 → 包裹下次还会被投递
//    }
//    这就是"至少一次"语义：每条消息保证至少被处理一次，但可能重复（如果签收前宕机了，重启后会重新消费）。
//
//    所以代码里 upsert 被设计成幂等的——重复执行也不会出错。
//
//    6. 完整链路一句话串起来
//
//    Plain Text
//
//    Canal 发现 MySQL 里有人改了一篇文章
//  → 把变更信息打包成 JSON，扔进 Kafka 的 "canal-outbox" 快递柜
//  → 这个类的 @KafkaListener 守在快递柜旁，有包裹就拿
//  → 拆包裹，里面是"文章 100 被更新了"
//  → 调用 SearchIndexService 同步到 ES
//  → 签收确认，完事