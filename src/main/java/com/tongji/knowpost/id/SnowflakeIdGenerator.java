package com.tongji.knowpost.id;

import org.springframework.stereotype.Component;

/**
 * 线程安全的雪花算法 ID 生成器。
 * 41 位时间戳 + 5 位数据中心 + 5 位工作节点 + 12 位序列。
 * 1位符号位：永远是 0，保证 ID 是正数。
 * 41位时间戳：精确到毫秒，能用 69 年。
 * 10位机器节点：通常分为 5位机房号 + 5位机器号，最多支持 1024 台服务器。
 * 12位序列号：同一毫秒内自增，单台机器每毫秒最多生成 4096 个 ID。
 * 全局唯一：靠机器号隔离，绝对不冲突。
 * 高性能（无网络消耗）：完全在本地内存计算，不依赖数据库或 Redis，单机 QPS 能达到几百万。
 * 趋势递增（最核心卖点）：因为最高位是时间戳，所以生成的 ID 总是越来越大。
 * 这对于 MySQL InnoDB 的 B+ 树索引极其友好，能保证新数据总是追加在索引树的末尾，
 * 绝不会引发极其消耗性能的“页分裂”。
 * 当面试官问出：“你用过雪花算法，那你在生产环境中遇到过什么坑吗？”
 * 这是一道高薪分水岭题目。你必须要答出我们之前排查出的两个痛点：
 * 痛点一：时钟回拨问题（NTP 对时导致时间倒流）。
 * 怎么解决？ 可以回答：小幅度回拨（几毫秒）就让线程 sleep 等待一下；
 * 大幅度回拨可以抛出异常，或者在系统中预留“备用机器号”（发生回拨时临时切换机器号，假装是一台新机器）。
 * 痛点二：Worker ID（机器号）如何自动分配？
 * 怎么解决？ 可以回答：千万不能在代码里写死。我会使用 Redis 的 INCR 命令，或者 Zookeeper，
 * 或者直接获取 K8s 容器的 Pod 序号，在系统启动时动态为这台机器分配一个独立的 ID
 */
@Component
public class SnowflakeIdGenerator {
    private static final long EPOCH = 1704067200000L; // 2024-01-01 00:00:00 UTC

    private static final long WORKER_ID_BITS = 5L;
    private static final long DATACENTER_ID_BITS = 5L;
    private static final long SEQUENCE_BITS = 12L;

    private static final long MAX_WORKER_ID = ~(-1L << WORKER_ID_BITS);
    private static final long MAX_DATACENTER_ID = ~(-1L << DATACENTER_ID_BITS);

    private static final long WORKER_ID_SHIFT = SEQUENCE_BITS;
    private static final long DATACENTER_ID_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS;
    private static final long TIMESTAMP_LEFT_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS + DATACENTER_ID_BITS;
    private static final long SEQUENCE_MASK = ~(-1L << SEQUENCE_BITS);

    private final long datacenterId;
    private final long workerId;

    private long lastTimestamp = -1L;
    private long sequence = 0L;

    //TODO 机器 ID 被硬编码写死 分布式部署大灾难
    public SnowflakeIdGenerator() {
        this(1, 1);
    }

    public SnowflakeIdGenerator(long datacenterId, long workerId) {
        if (workerId > MAX_WORKER_ID || workerId < 0) {
            throw new IllegalArgumentException("workerId out of range");
        }
        if (datacenterId > MAX_DATACENTER_ID || datacenterId < 0) {
            throw new IllegalArgumentException("datacenterId out of range");
        }
        this.datacenterId = datacenterId;
        this.workerId = workerId;
    }

    public synchronized long nextId() {
        long timestamp = currentTime();

//        if (timestamp < lastTimestamp) {
//            throw new IllegalStateException("Clock moved backwards. Refusing to generate id");
//        }
        // 等待时钟追回的方案
        if (timestamp < lastTimestamp) {
            long offset = lastTimestamp - timestamp;

            // 1. 小幅度回拨（比如 NTP 校时导致的 1~5ms 间抖动）：等待一会儿再试
            if (offset <= 5) {
                try {
                    // 睡 offset 毫秒，给系统时钟一点时间“追上来”
                    Thread.sleep(offset);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Thread interrupted while waiting for clock to catch up", e);
                }

                timestamp = currentTime();
                if (timestamp < lastTimestamp) {
                    // 等完还是没追上，说明问题较严重，直接拒绝
                    throw new IllegalStateException(
                            "Clock is still behind after waiting. last=" + lastTimestamp + ", now=" + timestamp);
                }
            } else {
                // 2. 回拨幅度太大，直接拒绝，避免线程长时间阻塞
                throw new IllegalStateException(
                        "Clock moved backwards too much. Refusing to generate id. offset=" + offset + "ms");
            }
        }

        // 处理同一毫秒内的并发请求：序列号逻辑
        if (lastTimestamp == timestamp) {
            sequence = (sequence + 1) & SEQUENCE_MASK;
            if (sequence == 0) {
                // 这一毫秒的 4096 个名额用完了
                timestamp = waitNextMillis(lastTimestamp);
            }
        } else {
            sequence = 0L;
        }

        lastTimestamp = timestamp;

        // 组装 64 位 ID
        return ((timestamp - EPOCH) << TIMESTAMP_LEFT_SHIFT)
                | (datacenterId << DATACENTER_ID_SHIFT)
                | (workerId << WORKER_ID_SHIFT)
                | sequence;
    }

    private long waitNextMillis(long lastTimestamp) {
        long timestamp = currentTime();
        while (timestamp <= lastTimestamp) {
            timestamp = currentTime();
        }
        return timestamp;
    }

    private long currentTime() {
        return System.currentTimeMillis();
    }
}