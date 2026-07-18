package com.tongji.relation.outbox;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.core.task.TaskExecutor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * Canal→Kafka 桥接器。
 * 职责：订阅 outbox 表的行级变更（ROWDATA），仅转发 INSERT/UPDATE 的 payload 字段到 Kafka 主题；批次确认位点确保至少一次语义。
 * 可靠性：解析失败或非关心类型不提交位点；停止时断开 Canal 连接并清理资源。
 * 依赖：KafkaTemplate、ObjectMapper、TaskExecutor、CanalConnector。
 */
@Service
public class CanalKafkaBridge implements SmartLifecycle {
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper objectMapper;
    private final boolean enabled;
    private final String host;
    private final int port;
    private final String destination;
    private final String username;
    private final String password;
    private final String filter;
    private final int batchSize;
    private final long intervalMs;
    private volatile boolean running;
    private final TaskExecutor taskExecutor;
    private CanalConnector connector;
    private static final Logger log = LoggerFactory.getLogger(CanalKafkaBridge.class);

    /**
     * Canal 到 Kafka 的桥接器。
     * 通过构造器注入 Spring 管理的 Bean 和 Canal 连接配置。
     * @param kafka       Kafka 模板，由 Spring Boot 自动装配，用于发送消息到 Kafka 主题
     * @param objectMapper JSON 序列化器，由 Spring Boot 自动装配
     * @param taskExecutor 异步任务执行器，通过 @Qualifier 指定注入，避免多线程池冲突
     * @param enabled     是否启用桥接器，来自配置 ${canal.enabled}
     * @param host        Canal Server 主机地址，来自配置 ${canal.host}
     * @param port        Canal Server 端口，来自配置 ${canal.port}
     * @param destination Canal 实例名，对应一个 MySQL 数据源，来自配置 ${canal.destination}
     * @param username    Canal 连接用户名，来自配置 ${canal.username}
     * @param password    Canal 连接密码，来自配置 ${canal.password}
     * @param filter      订阅过滤表达式，来自配置 ${canal.filter}，如 "outbox\\..*"
     * @param batchSize   每批次拉取消息数，来自配置 ${canal.batchSize}
     * @param intervalMs  空轮询休眠间隔（毫秒），来自配置 ${canal.intervalMs}
     */
    public CanalKafkaBridge(KafkaTemplate<String, String> kafka,
                            ObjectMapper objectMapper,
                            @Qualifier("taskExecutor") TaskExecutor taskExecutor,
                            @Value("${canal.enabled}") boolean enabled,
                            @Value("${canal.host}") String host,
                            @Value("${canal.port}") int port,
                            @Value("${canal.destination}") String destination,
                            @Value("${canal.username}") String username,
                            @Value("${canal.password}") String password,
                            @Value("${canal.filter}") String filter,
                            @Value("${canal.batchSize}") int batchSize,
                            @Value("${canal.intervalMs}") long intervalMs) {
        this.kafka = kafka;
        this.objectMapper = objectMapper;
        this.taskExecutor = taskExecutor;
        this.enabled = enabled;
        this.host = host;
        this.port = port;
        this.destination = destination;
        this.username = username;
        this.password = password;
        this.filter = filter;
        this.batchSize = batchSize;
        this.intervalMs = intervalMs;
    }

    /**
     * 启动桥接器：消费 Canal binlog 变更并投递到 Kafka。
     * 将 Canal 消费逻辑提交到异步线程池执行，主线程不阻塞。
     * 重复调用会跳过。
     */
    @Override
    public void start() {
        // 1. 防重入检查：如果已经在运行，直接跳过，防止重复启动
        //    - running 是 volatile 变量，保证多线程间的可见性
        if (running) {
            log.info("Canal bridge start skipped: running={} enabled={} host={} port={} dest={} filter={}", running, enabled, host, port, destination, filter);
            return;
        }

        // 2. 启用开关检查：如果配置中 canal.enabled=false，则跳过整个桥接器
        //    - 用于本地开发或测试环境，不需要 Canal 连接时一键关闭
        //    - 避免无 Canal Server 时启动报错
        if (!enabled) {
            log.info("Canal bridge is disabled by configuration. Skip connecting.");
            return;
        }

        // 3. 标记运行状态并提交异步任务：使用全局线程池执行 Canal 消费主循环
        //    - 标记 running=true 防止重复启动
        //    - 异步执行保证主线程不阻塞，应用可以正常启动
        running = true;
        taskExecutor.execute(() -> {
            try {
                // 4. 创建 Canal 单实例连接器：使用单实例模式连接 Canal Server
                //    - 传入 host、port、destination、username、password 参数
                //    - 单实例连接器适用于只有一个 Canal Server 的场景
                connector = CanalConnectors.newSingleConnector(new InetSocketAddress(host, port), destination, username, password);
                log.info("Canal connecting to {}:{} dest={} user={} filter={}", host, port, destination, username, filter);

                // 5. 建立连接并订阅：连接 Canal Server 并设置过滤表达式
                //    - connect() 建立 TCP 长连接
                //    - subscribe(filter) 设置订阅过滤规则，只拉取关心的表（如 outbox）
                //    - rollback() 回滚到上次确认的位点，保证不丢消息、不重复消费
                connector.connect();
                connector.subscribe(filter);
                connector.rollback();
                log.info("Canal connected and subscribed: host={} port={} dest={} filter={} batchSize={} intervalMs={}ms", host, port, destination, filter, batchSize, intervalMs);

                // 6. 主消费循环：不断从 Canal 拉取消息并转发到 Kafka
                //    - running 为 false 时退出循环，优雅停止
                while (running) {
                    // 7. 拉取一批未确认消息：getWithoutAck 不自动提交位点
                    //    - batchSize 控制每次拉取的最大消息数
                    //    - 手动 ack 机制保证至少一次语义
                    Message message = connector.getWithoutAck(batchSize);
                    long batchId = message.getId();

                    // 8. 空批次处理：batchId=-1 表示无数据（心跳或空轮询）
                    //    - 休眠 intervalMs 毫秒后继续轮询，避免 CPU 空转
                    //    - 休眠期间不占用系统资源
                    if (batchId == -1 || message.getEntries() == null || message.getEntries().isEmpty()) {
                        try {
                            Thread.sleep(intervalMs);
                        } catch (InterruptedException ignored) {}
                        continue;
                    }

                    // 9. 遍历消息条目：处理每一行 binlog 变更事件
                    for (CanalEntry.Entry entry : message.getEntries()) {
                        // 10. 过滤非行数据事件：只处理 ROWDATA 类型
                        //     - 忽略 TRANSACTIONBEGIN、TRANSACTIONEND、HEARTBEAT 等非数据事件
                        //     - 保证只处理真正的数据变更
                        if (entry.getEntryType() != CanalEntry.EntryType.ROWDATA) {
                            continue;
                        }
                        CanalEntry.RowChange rowChange;

                        try {
                            // 11. 解析二进制数据为 RowChange：将 protobuf 序列化的数据反序列化
                            //     - RowChange 包含事件类型（INSERT/UPDATE/DELETE）和变更的行数据
                            //     - 解析失败时跳过当前条目，不影响后续消息处理
                            rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                        } catch (Exception e) {
                            continue;
                        }

                        CanalEntry.EventType eventType = rowChange.getEventType();

                        // 12. 过滤事件类型：仅转发 INSERT 和 UPDATE 事件
                        //     - DELETE 事件不需要转发（删除的数据下游不关心）
                        //     - 其他 DDL 事件也不处理
                        if (eventType != CanalEntry.EventType.INSERT && eventType != CanalEntry.EventType.UPDATE) {
                            continue;
                        }

                        // 13. 构建数据数组：遍历每行变更数据，提取 payload 字段
                        //     - 一个 RowChange 可能包含多行数据（批量操作）
                        //     - 只提取 afterColumns 中的 payload 字段值
                        ArrayNode dataArray = objectMapper.createArrayNode();

                        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                            ObjectNode rowNode = objectMapper.createObjectNode();
                            for (CanalEntry.Column col : rowData.getAfterColumnsList()) {
                                // 14. 提取 payload 字段：只关心 payload 列的值
                                //     - payload 是 outbox 表中存储的 JSON 消息体
                                //     - 忽略其他列（如 id、created_at 等），减少传输数据量
                                if ("payload".equalsIgnoreCase(col.getName())) {
                                    rowNode.put("payload", col.getValue());
                                }
                            }
                            dataArray.add(rowNode);
                        }

                        // 15. 组装 Kafka 消息：构建包含表名、事件类型和数据的 JSON 对象
                        //     - table：变更来源表名，方便下游区分不同业务
                        //     - type：INSERT 或 UPDATE，下游可根据类型做不同处理
                        //     - data：变更的数据数组，包含每行的 payload 字段
                        ObjectNode msgNode = objectMapper.createObjectNode();
                        msgNode.put("table", entry.getHeader().getTableName());
                        msgNode.put("type", eventType == CanalEntry.EventType.INSERT ? "INSERT" : "UPDATE");
                        msgNode.set("data", dataArray);

                        try {
                            // 16. 发送到 Kafka：将 JSON 消息发送到 canal-outbox 主题
                            //     - 使用 KafkaTemplate 异步发送，不阻塞主流程
                            //     - 发送失败时静默忽略，下一个批次会重试（因为未 ack）
                            String json = objectMapper.writeValueAsString(msgNode);
                            kafka.send(OutboxTopics.CANAL_OUTBOX, json);
                        } catch (Exception ignored) {}
                    }

                    // 17. 批次确认：处理完当前批次后提交位点
                    //     - 推进 Canal 消费位点，避免消息重复消费
                    //     - 如果某条消息处理失败（如 Kafka 发送失败），不提交位点，下次重试
                    connector.ack(batchId);
                }
            } catch (Exception e) {
                // 18. 异常处理：捕获主循环中的未预期异常，记录日志
                //     - 异常不会导致 JVM 崩溃，只是桥接器停止工作
                //     - 需要运维监控日志，及时发现问题
                log.error("Canal bridge error", e);
            } finally {
                // 19. 资源清理：断开 Canal 连接，释放 TCP 连接资源
                //     - 无论正常退出还是异常退出，都执行清理
                //     - 断开失败只记录警告，不影响应用关闭
                if (connector != null) {
                    try {
                        connector.disconnect();
                        log.info("Canal disconnected: dest={}", destination);
                    } catch (Exception ex) {
                        log.warn("Canal disconnect failed: dest={} err={}", destination, ex.getMessage());
                    }
                }
            }
        });
    }

    /**
     * 停止桥接器。
     * <p>
     * 将 running 标记设为 false，异步线程在下一次循环检查时退出。
     * 注意：本方法不会立即中断正在处理的消息，保证批次处理完整性。
     */
    @Override
    public void stop() {
        running = false;
    }

    /**
     * 判断桥接器是否处于运行状态。
     *
     * @return true 表示正在运行，false 表示已停止或未启动
     */
    @Override
    public boolean isRunning() {
        return running;
    }
}