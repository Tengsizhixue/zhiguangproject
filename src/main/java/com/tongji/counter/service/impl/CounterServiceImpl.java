package com.tongji.counter.service.impl;

import com.tongji.common.exception.BusinessException;
import com.tongji.common.exception.ErrorCode;
import com.tongji.counter.schema.CounterKeys;
import com.tongji.counter.schema.CounterSchema;
import com.tongji.counter.schema.BitmapShard;
import com.tongji.counter.service.CounterService;
import com.tongji.counter.event.CounterEvent;
import com.tongji.counter.event.CounterEventProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.context.ApplicationEventPublisher;
import org.redisson.api.RedissonClient;
import org.redisson.api.RLock;
import org.redisson.api.RRateLimiter;
import org.redisson.api.RateType;
import org.redisson.api.RBucket;
import org.springframework.beans.factory.annotation.Value;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 内容实体计数服务实现（位图事实 + 事件聚合 + SDS 汇总）。
 *
 * <p>职责：</p>
 * - 位图原子切换并产出计数事件（幂等）；
 * - 读取汇总计数（SDS），异常时基于位图分片重建；
 * - 批量读取优化与“是否点赞/收藏”判定。
 */
@Slf4j
@Service
public class CounterServiceImpl implements CounterService {

    private final StringRedisTemplate redis;
    private final DefaultRedisScript<Long> toggleScript;
    private final DefaultRedisScript<Long> incrSdsScript;
    private final CounterEventProducer eventProducer;
    private final ApplicationEventPublisher eventPublisher;
    private final RedissonClient redisson;
    @Value("${counter.rebuild.lock.ttl-ms:5000}")
    private long lockTtlMs;
    @Value("${counter.rebuild.rate.permits:3}")
    private int ratePermits;
    @Value("${counter.rebuild.rate.window-seconds:10}")
    private int rateWindowSeconds;
    @Value("${counter.rebuild.backoff.base-ms:500}")
    private long backoffBaseMs;
    @Value("${counter.rebuild.backoff.max-ms:30000}")
    private long backoffMaxMs;

    public CounterServiceImpl(StringRedisTemplate redis, CounterEventProducer eventProducer, ApplicationEventPublisher eventPublisher, RedissonClient redisson) {
        this.redis = redis;
        this.eventProducer = eventProducer;
        this.eventPublisher = eventPublisher;
        this.redisson = redisson;
        this.toggleScript = new DefaultRedisScript<>();
        this.toggleScript.setResultType(Long.class);
        // 位图状态原子切换，仅在状态变化时返回 1
        this.toggleScript.setScriptText(TOGGLE_LUA);

        this.incrSdsScript = new DefaultRedisScript<>();
        this.incrSdsScript.setResultType(Long.class);
        this.incrSdsScript.setScriptText(INCR_FIELD_LUA);
    }

    /**
     * 点赞：位图原子置位，仅当状态从未点赞→已点赞时返回 true。
     * 同步路径完成事实层更新后产出增量事件，异步聚合到计数快照。
     * @param entityType 实体类型
     * @param entityId 实体 ID
     * @param userId 用户 ID
     * @return 是否发生状态变化（幂等）
     */
    @Override
    public boolean like(String entityType, String entityId, long userId) {
        return toggle(entityType, entityId, userId, "like", CounterSchema.IDX_LIKE, true);
    }

    /**
     * 取消点赞：位图原子清零，仅当状态从已点赞→未点赞时返回 true。
     * 产出增量事件（delta=-1），异步聚合到计数快照。
     */
    @Override
    public boolean unlike(String entityType, String entityId, long userId) {
        return toggle(entityType, entityId, userId, "like", CounterSchema.IDX_LIKE, false);
    }

    /**
     * 收藏：位图原子置位，并产出增量事件（delta=+1）。
     */
    @Override
    public boolean fav(String entityType, String entityId, long userId) {
        return toggle(entityType, entityId, userId, "fav", CounterSchema.IDX_FAV, true);
    }

    /**
     * 取消收藏：位图原子清零，并产出增量事件（delta=-1）。
     */
    @Override
    public boolean unfav(String entityType, String entityId, long userId) {
        return toggle(entityType, entityId, userId, "fav", CounterSchema.IDX_FAV, false);
    }

    /**
     * 位图状态切换（点赞/收藏 核心方法）。
     * <p>
     * 设计要点：
     * <ol>
     *   <li><b>幂等保证</b>：仅在位图状态真正翻转（0→1 或 1→0）时才产出事件，
     *       重复调用不会产生重复计数，天然幂等</li>
     *   <li><b>原子操作</b>：Redis Lua 脚本将 GETBIT + SETBIT 打包为原子操作，
     *       避免并发下的竞态条件（如 A 读到旧状态时 B 已经修改）</li>
     *   <li><b>双通道事件</b>：Kafka 事件服务跨进程消费（计数聚合、异步落库），
     *       本地 Spring Event 服务同进程快速路径（缓存失效、Feed 实时更新）</li>
     * </ol>
     * @param etype  实体类型（如 "knowpost"，对应数据库表维度）
     * @param eid    实体 ID（如 "12345"，对应具体某条帖子）
     * @param uid    用户 ID（谁在点赞/收藏）
     * @param metric 指标名称（"like" 或 "fav"，用于拼接 Redis Key）
     * @param idx    指标在 SDS 固定结构中的字节索引（CounterSchema.IDX_LIKE=1, IDX_FAV=2）
     * @param add    true=点赞/收藏（SETBIT → 1），false=取消点赞/收藏（SETBIT → 0）
     * @return true=状态发生了实际翻转（从没赞→赞了，或从赞了→取消），false=重复操作无变化
     *      *存储层	        存什么	             查询什么	                 谁来维护
     *      * 位图分片	用户维度的点赞事实	isLiked？（当前用户是否点过）	toggle 方法直接写
     *      * SDS	汇总后的总计数	        getCounts()（帖子总共多少赞）	Kafka 消费者异步聚合
     *      * Feed 缓存	页面级的响应快照	    首页 Feed 列表	            本地监听器实时更新
     */
    private boolean toggle(String etype, String eid, long uid, String metric, int idx, boolean add) {
        // ── 第1步：位图分片定位 ──
        // 按用户ID计算所属分片（chunk），避免单个 Redis Key 存储所有用户导致大Key问题
        // 例如：每 65536 个用户一个分片，100万用户 ≈ 16个分片，每个分片仅占 8KB
//        位图已经存了，它存的是「谁点了赞」这个事实
//        但上层业务还需要两个东西：
//        快速拿到总点赞数（显示在列表页）→ 需要 SDS 汇总 → Kafka 负责异步聚合
//        点赞后立刻让用户看到新计数（体验）→ 需要本地事件即时更新 Feed 缓存
        long chunk = BitmapShard.chunkOf(uid);
        // 计算用户在该分片内的 bit 偏移，即用户在 65536 个 bit 中的第几位
        long bit = BitmapShard.bitOf(uid);

        // ── 第2步：组装 Redis Key 和 Lua 脚本参数 ──
        // Key 格式：bm:{metric}:{etype}:{eid}:{chunk}
        // 例如：bm:like:knowpost:12345:0  →  帖子12345的第0号分片（用户0~65535）
        String bmKey = CounterKeys.bitmapKey(metric, etype, eid, chunk);
        // KEYS[] 数组：Lua 脚本中要操作的 Redis Key 列表（此处只有一个位图 Key）
        List<String> keys = List.of(bmKey);
        // ARGV[] 数组：bit 偏移量 + 操作类型（"add"=置1, "remove"=置0）
        List<String> args = List.of(String.valueOf(bit), add ? "add" : "remove");

        // ── 第3步：执行 Lua 原子脚本 ──
        // 脚本逻辑（原子执行，不会被其他命令打断）：
        //   ① GETBIT bmKey bit  →  读取当前状态（0或1）
        //   ② SETBIT bmKey bit value  →  写入新状态
        //   ③ 返回：原状态 != 新状态 ? 1 : 0（即是否发生了状态变化）
        // 返回值含义：1L=状态翻转了（从没赞→赞了 / 从赞了→取消），0L=重复操作无需处理
        Long changed = redis.execute(toggleScript, keys, args.toArray());
        boolean ok = changed == 1L;
        log.info("位图切换结果: key={}, bit={}, add={}, changed={}", bmKey, bit, add, ok);

        // ── 第4步：状态真正变化时才发布事件 ──
        if (ok) {
            // 增量：点赞 +1，取消点赞 -1
            int delta = add ? 1 : -1;

            // 直接同步更新 SDS 计数器（Lua 原子操作，保证最终一致性）
            try {
                String sdsKey = CounterKeys.sdsKey(etype, eid);
                redis.execute(incrSdsScript,
                        List.of(sdsKey),
                        String.valueOf(CounterSchema.SCHEMA_LEN),
                        String.valueOf(CounterSchema.FIELD_SIZE),
                        String.valueOf(idx),
                        String.valueOf(delta));
                log.info("SDS 同步更新成功: sdsKey={}, idx={}, delta={}", sdsKey, idx, delta);
            } catch (Exception e) {
                log.error("SDS 同步更新失败: etype={}, eid={}, idx={}, delta={}", etype, eid, idx, delta, e);
            }

            CounterEvent event = CounterEvent.of(etype, eid, metric, idx, uid, delta);
            log.info("准备发布计数事件: entityType={}, entityId={}, metric={}, delta={}", etype, eid, metric, delta);

//            目的：Feed 流缓存里存着帖子的点赞数，点赞后需要立即更新缓存
            // ① Spring 本地事件 → 同进程内消费（优先保证用户体验）
            //    监听器：FeedCacheInvalidationListener，负责实时更新 Feed 缓存中的计数
            //    不经过网络，比 Kafka 路径更快，用户体验更实时
            try {
                eventPublisher.publishEvent(event);
            } catch (Exception e) {
                log.error("本地缓存刷新事件发送失败, eid:{}", eid, e);
            }

            // ② Kafka 事件 → 跨进程消费（允许失败，发件箱模式补偿）
            //    消费者：CounterAggregationConsumer，负责更新 Redis 聚合桶 → 最终刷入 SDS
            //    Kafka 分区 Key 为 entityType + entityId，保证同一实体的消息有序消费
//            100万用户 → ~16个分片 → 需要 16 次网络往返
//            Feed 列表页一次返回 20 个帖子 → 320 次 BITCOUNT，性能崩盘
//            有了 SDS：
//
//            一次 GET 拿到整个字节数组 → O(1) 读取所有指标
//            批量读取 20 个帖子用管道 → 一次往返搞定
//            为什么用 Kafka 异步聚合：
//
//            点赞是高并发操作，直接同步写 SDS 会加锁竞争延迟
//            Kafka 削峰填谷，多个点赞增量攒在一起批量刷入
//            保证最终一致性，不影响主请求延迟
            try {
                eventProducer.publish(event);
            } catch (Exception e) {
                log.error("Kafka 点赞聚合消息发送失败, eid:{}", eid, e);
            }
        }
        // 返回本次是否实际翻转，供上层（like/unlike/fav/unfav）判断操作是否生效
        return ok;
    }

    /**
     * 获取实体计数汇总（基于 SDS 固定结构读取）。
     *
     * SDS 固定结构说明：
     * 每条内容（如帖子）在 Redis 中存储为一个定长字节数组，各指标按固定偏移存放。
     * 示例：
     *   Key: cnt:v1:knowpost:12345
     *   Value: [0..3] = like (Int32)  [4..7] = fav (Int32)  [8..11] = share (Int32) ...
     *         ↑ 大端字节序，每字段占 4 字节（FIELD_SIZE = 4）
     *         ↑ 共 SCHEMA_LEN = 5 个字段，总计 20 字节
     * 选择字节数组而非 Hash 的原因：
     *   - 一次 GET 获取所有指标，O(1) 读取，比 HGET 逐个字段更快
     *   - 批量读取（如 20 个帖子）可借助 Redis 管道，单次往返完成
     *   - 固定长度，无内存碎片，适合高频读取场景
     *
     * 重建流程（SDS 缺失或损坏时触发）：
     *   1. 检查是否处于退避期 → 是则直接返回 0，避免频繁重建
     *   2. 检查限流器是否允许 → 不允许则进入退避并返回 0
     *   3. 获取分布式锁（最多等待 200ms）→ 若获取失败则抛出系统繁忙异常
     *   4. 持锁后执行 Double-Check：可能其他线程已重建完成，直接读取现有 SDS，跳过重建
     *   5. 确认需要重建：遍历所有位图分片执行 BITCOUNT → 汇总 → 写入 SDS → 清理聚合桶
     *
     * 设计说明与优化细节：
     *   - 消除“爆款假 0”问题：
     *       原逻辑使用 tryLock(0) 导致大量并发线程直接返回 0；
     *       现改为 tryLock(200L, ...) 使线程等待最多 200ms，大部分能等到锁释放并拿到真实数据，
     *       用户几乎无感知。
     *   - 引入 Double-Check 机制：
     *       持锁后首先检查 SDS 是否已被其他线程重建，避免重复执行昂贵的 BITCOUNT 操作，
     *       将潜在数万次重建压缩为实际所需的 1 次。
     *   - 优雅降级：
     *       若等待 200ms 后仍未获得锁（表明系统极度拥堵），直接抛出 BusinessException，
     *       前端可保留旧数据或提示重试，绝不返回全 0 的虚假数据。
     *   - 代码复用（DRY）：
     *       将 SDS 字节数组解析逻辑抽取为 parseSdsRaw(raw, metrics) 方法，
     *       在 Double-Check 和正常缓存命中两处复用，提高可读性与可维护性。
     *   - 锁租期（Lease Time）设为 3000ms：
     *       为快速操作设置绝对过期时间，避免 Redisson WatchDog 无限续期，作为防死锁兜底。
     *
     * @param entityType 实体类型（如 "knowpost"）
     * @param entityId   实体 ID（如 "12345"）
     * @param metrics    需要查询的指标名称列表（如 ["like", "fav"]）
     * @return 指标名 → 计数值的映射（使用 LinkedHashMap 保持插入顺序）
     * @throws BusinessException 分布式锁等待超时（系统极度拥堵时抛出）
     */
    @Override
    public Map<String, Long> getCounts(String entityType, String entityId, List<String> metrics) {
        // ── 第1步：尝试读取 SDS 固定结构 ──
        // Key 格式：cnt:v1:knowpost:12345
        String sdsKey = CounterKeys.sdsKey(entityType, entityId);
        // 预期字节数 = 指标数量 × 4字节，例如 5个指标 × 4 = 20字节
        int expectedLen = CounterSchema.SCHEMA_LEN * CounterSchema.FIELD_SIZE;
        // 从 Redis 读取原始字节数组（大端编码的 Int32 序列）
        byte[] raw = getRaw(sdsKey);
        // 两种情况需要重建：① Key 不存在（raw==null）② 数据长度异常（损坏/版本不匹配）
        boolean needRebuild = (raw == null || raw.length != expectedLen);

        Map<String, Long> result = new LinkedHashMap<>();

        // ── 第2步：SDS 缺失或损坏 → 进入重建流程 ──
        if (needRebuild) {
            log.info("计数结构不存在，需要重建");

            // 2.1 退避检查：上次重建失败的实体在退避期内，直接返回 0
// 防线1：如果在“小黑屋（退避期）”里，直接报错。
// 比如黑客一直刷一个不存在的帖子ID，底层会查不到，系统就会把这个ID关进小黑屋，短期内再有人查这个ID，直接报错，保护系统。
            if (inBackoff(entityType, entityId)) {
                log.warn("SDS 重建触发退避拦截 entityType={} entityId={}", entityType, entityId);
                throw new BusinessException(ErrorCode.SYSTEM_BUSY, "计数系统加载中，请稍候");
            }


            // 2.2 限流检查：通过令牌桶控制重建频率
            //     限制单位时间内最多允许 N 次重建，超过则拒绝并进入退避
            if (!allowedByRateLimiter(entityType, entityId)) {
                escalateBackoff(entityType, entityId);
                log.warn("SDS 重建触发限流拦截 entityType={} entityId={}", entityType, entityId);
                throw new BusinessException(ErrorCode.SYSTEM_BUSY, "计数系统加载中，请稍候");
            }

            // 2.3 分布式锁：同一实体同一时刻只允许一个实例执行重建
            //     Key 格式：lock:sds-rebuild:knowpost:12345
            String lockKey = String.format("lock:sds-rebuild:%s:%s", entityType, entityId);
            RLock lock = redisson.getLock(lockKey);
            boolean locked = false;

            try {
                // 等待最多 200ms，锁租期 3s（比原来看门狗更可控）
                // 为什么等待 200ms？因为正常重建（BITCOUNT 管道）通常 50ms 内完成，
                // 200ms 给了 4 倍余量，覆盖绝大多数重建场景，排队线程能等到结果
                // 为什么租期 3s？防止极端情况锁不释放，3s 后自动过期兜底
                locked = lock.tryLock(200L, 3000L, TimeUnit.MILLISECONDS);
                if (!locked) {
                    // 等了 200ms 还是拿不到锁 → 系统极度拥堵
                    // 此时绝不能返回 0（会让所有用户看到"假 0"），应抛出异常让前端保持旧数据或显示"加载中"
                    log.warn("SDS 重建锁等待超时 entityType={} entityId={}", entityType, entityId);
                    throw new BusinessException(ErrorCode.SYSTEM_BUSY, "计数系统加载中，请稍候");
                }

                // 2.4 Double-Check：拿到锁进门后，先检查 SDS 是否已经被其他线程重建好了
                //     因为你在门外等的 200ms 里，前一个持锁线程可能已经完成了重建并写入 Redis
                byte[] doubleCheckRaw = getRaw(sdsKey);
                if (doubleCheckRaw != null && doubleCheckRaw.length == expectedLen) {
                    // 别人已经建好了，直接解析返回，跳过耗时的 BITCOUNT 重建
                    log.info("SDS 已被其他线程重建，直接读取 entityType={} entityId={}", entityType, entityId);
                    resetBackoff(entityType, entityId);
                    return parseSdsRaw(doubleCheckRaw, metrics);
                }

                // 2.5 持锁者：执行基于位图的事实重建
                // 申请一块崭新的 20 字节白板
                byte[] newSds = new byte[expectedLen];
                //记录哪些字段被重建了，后续需要清理对应的聚合桶
                List<String> rebuildFields = new ArrayList<>();
                // ⚠️ 关键：必须重建 SDS 中所有已定义的指标，而不仅仅是调用方请求的 metrics
                // 因为 setRaw 会写入整个字节数组，如果只重建部分字段，其余字段会被清零
                // 例如：调用方只请求 ["like"]，fav 由于位图统计会被跳过，newSds 中 fav=0
                // 最终 setRaw 写入后，fav 从 50 被永久覆写为 0 → 数据丢失
                // 💡 极其关键的一步：循环遍历系统支持的【所有】指标（不管前端这次要了几个）
                for (String m : CounterSchema.SUPPORTED_METRICS) {
                    Integer idx = CounterSchema.NAME_TO_IDX.get(m);
                    if (idx == null) {
                        continue;
                    }
                    // 遍历所有位图分片，用 Redis 管道批量执行 BITCOUNT，汇总得到真实计数
                    // 例如：100万用户 → 16个分片 → 16次 BITCOUNT 管道合并 → 一次往返拿到结果
                    long sum = bitCountShardsPipelined(m, entityType, entityId);
                    // 写入 SDS 数组：偏移 = 索引 × 4字节，大端字节序
                    // 把算出来的真实数字（比如 100 赞），塞进 20 字节白板对应的格子里。
                    writeInt32BE(newSds, idx * CounterSchema.FIELD_SIZE, sum);
                    rebuildFields.add(String.valueOf(idx));
                    // 仅当该指标是调用方请求的，才放入 result 返回
                    if (metrics.contains(m)) {
                        result.put(m, sum);
                    }
                }

                // 2.6 回写 SDS 到 Redis
                setRaw(sdsKey, newSds);

                // 2.7 清理聚合桶中对应字段的增量
                //     聚合桶是 Redis Hash，存储 Kafka 消费到的未刷写增量。
                //     例如用户点赞后 → Kafka事件 → 消费者写入 agg:v1:knowpost:12345 field=1 +1
                //     定时任务每秒将聚合桶增量折叠到 SDS，然后扣减聚合桶。
                //     为什么这里要清理？因为位图 BITCOUNT 已经拿到了真实计数（包含所有增量），
                //     而聚合桶里可能还有未刷写的增量。如果不清理，下次定时 flush 会把这些增量
                //     再加到 SDS 上，导致计数翻倍。
                //     示例：位图真实计数=100，聚合桶还有+2未刷 → 不清理的话 SDS会变成102
                if (!rebuildFields.isEmpty()) {
                    String aggKey = CounterKeys.aggKey(entityType, entityId);
                    redis.opsForHash().delete(aggKey, rebuildFields.toArray());
                }

                // 2.8 重建成功 → 重置退避计时器
                resetBackoff(entityType, entityId);

            } catch (InterruptedException ie) {
                // 线程被中断 → 恢复中断标记，抛出系统繁忙异常
                Thread.currentThread().interrupt();
                log.warn("SDS 重建被中断 entityType={} entityId={}", entityType, entityId);
                throw new BusinessException(ErrorCode.SYSTEM_BUSY, "计数系统加载中，请稍候");
            } finally {
                // 确保释放锁（仅当自己持有锁时才释放）
                if (locked) {
                    try {
                        lock.unlock();
                    } catch (Exception ignore) {
                        // 锁可能已过期或被其他原因释放，忽略异常
                    }
                }
            }

        // ── 第3步：SDS 正常存在 → 直接解析 ──
        } else {
            // 拿着这 20 个字节，按照 0~3字节是赞，4~7字节是收藏的规则，直接解包返回！
            return parseSdsRaw(raw, metrics);
        }
        return result;
    }

    /**
     * 从 SDS 原始字节数组解析各指标计数。
     *
     * @param raw     SDS 原始字节数组（已校验长度正确）
     * @param metrics 需要解析的指标名称列表
     * @return 指标名 → 计数值的映射
     */
    private Map<String, Long> parseSdsRaw(byte[] raw, List<String> metrics) {
        Map<String, Long> result = new LinkedHashMap<>();
        for (String m : metrics) {
            Integer idx = CounterSchema.NAME_TO_IDX.get(m);
            if (idx == null) {
                continue;
            }
            int off = idx * CounterSchema.FIELD_SIZE;
            long val = readInt32BE(raw, off);
            result.put(m, val);
        }
        return result;
    }

    /**
     * 批量获取实体计数（Redis 管道批量 GET，将 N 次网络往返合并为 1 次）。
     * <p>
     * <b>为什么需要批量方法？</b><br>
     * Feed 列表页一次展示 20 条帖子，如果逐条调用 {@link #getCounts} 会产生 20 次 Redis 网络往返，
     * 在用户量大的情况下延迟不可接受。管道批量 GET 将所有请求打包，一次往返全部拿到。
     * <p>
     * <b>与 getCounts 的区别：</b><br>
     * 批量方法不做 SDS 重建（不在循环里触发 BITCOUNT 风暴），缺失的按 0 返回。
     * 重建逻辑留给 {@link #getCounts} 单条调用时触发，保证热点数据最终会被修复。
     * <p>
     * <b>Redis 管道原理：</b>
     * <pre>
     * 逐条 GET：Client → GET key1 → Redis → val1 → Client
     *                    → GET key2 → Redis → val2 → Client  (N × RTT)
     * 管道 GET：Client → GET key1, GET key2, ..., GET keyN → Redis → val1, val2, ..., valN → Client  (1 × RTT)
     * </pre>
     *
     * @param entityType 实体类型（如 "knowpost"）
     * @param entityIds  实体 ID 列表（如帖子的 ID 列表，一次最多 50 个建议）
     * @param metrics    需要查询的指标名称列表（如 ["like", "fav"]）
     * @return 外层 Map：entityId → 内层 Map（指标名 → 计数值），保持实体 ID 的插入顺序
     */
    @Override
    public Map<String, Map<String, Long>> getCountsBatch(String entityType, List<String> entityIds, List<String> metrics) {
        // ── 第1步：参数校验与 SDS Key 组装 ──
        Map<String, Map<String, Long>> out = new LinkedHashMap<>();
        if (entityIds == null || entityIds.isEmpty() || metrics == null || metrics.isEmpty()) {
            return out;
        }
        // 组装所有 SDS Key：cnt:v1:knowpost:12345, cnt:v1:knowpost:12346, ...
        List<String> keys = new ArrayList<>(entityIds.size());
        for (String eid : entityIds) {
            keys.add(CounterKeys.sdsKey(entityType, eid));
        }

        // ── 第2步：Redis 管道批量 GET ──
        // executePipelined 将多个 GET 命令打包到一次网络往返，大幅降低 RTT
        // 例如 20 个帖子 → 逐条 GET 需要 20 次往返（~20ms）→ 管道只需 1 次往返（~1ms）
        List<Object> raws = redis.executePipelined((RedisCallback<Object>) connection -> {
            for (String k : keys) {
                // 注意：管道中的命令不会立即返回结果，全部发出后统一接收
                connection.stringCommands().get(k.getBytes(StandardCharsets.UTF_8));
            }
            return null; // 返回值会在管道执行完毕后通过 raws 列表获取
        });

        // ── 第3步：解析每个实体的 SDS 计数 ──
        int expectedLen = CounterSchema.SCHEMA_LEN * CounterSchema.FIELD_SIZE;
        for (int i = 0; i < entityIds.size(); i++) {
            String eid = entityIds.get(i);
            // 管道返回的结果顺序与命令发送顺序一致，按索引取回对应实体的 SDS 数据
            Object rawObj = i < raws.size() ? raws.get(i) : null;
            byte[] raw = (rawObj instanceof byte[]) ? (byte[]) rawObj : null;

            Map<String, Long> m = new LinkedHashMap<>();
            if (raw != null && raw.length == expectedLen) {
                // SDS 正常存在 → 按大端字节序解析各指标的 Int32 值
                for (String name : metrics) {
                    // 查表获取指标在 SDS 中的索引位置
                    Integer idx = CounterSchema.NAME_TO_IDX.get(name);
                    if (idx == null) {
                        continue;
                    }
                    // 偏移 = 索引 × 4字节，大端读取
                    int off = idx * CounterSchema.FIELD_SIZE;
                    long val = readInt32BE(raw, off);
                    m.put(name, val);
                }
            } else {
                // SDS 缺失或结构异常 → 补 0，不触发重建
                // 为什么不在批量方法里重建？因为批量方法可能处理 20+ 个实体，
                // 如果每个缺失的都触发 BITCOUNT 重建（遍历所有分片），会导致 Redis 负载爆炸
                // 重建逻辑交给 getCounts 单条调用时处理，保证热点数据最终一致性
                for (String name : metrics) {
                    m.put(name, 0L);
                }
            }
            out.put(eid, m);
        }
        return out;
    }

    /**
     * 是否点赞判定：基于分片位图在分片内做位测试。
     * 毫秒级读取，不依赖计数快照。
     */
    @Override
    public boolean isLiked(String entityType, String entityId, long userId) {
        long chunk = BitmapShard.chunkOf(userId);
        long bit = BitmapShard.bitOf(userId);
        return getBit(CounterKeys.bitmapKey("like", entityType, entityId, chunk), bit);
    }

    /**
     * 是否收藏判定：同点赞，基于分片位图位测试。
     */
    @Override
    public boolean isFaved(String entityType, String entityId, long userId) {
        long chunk = BitmapShard.chunkOf(userId);
        long bit = BitmapShard.bitOf(userId);
        return getBit(CounterKeys.bitmapKey("fav", entityType, entityId, chunk), bit);
    }

    /**
     * 读取位图某偏移位（GETBIT）。
     * @param key 位图分片键
     * @param offset 分片内位偏移
     * @return 位是否为 1
     */
    private boolean getBit(String key, long offset) {
        Boolean bit = redis.execute((RedisCallback<Boolean>) connection ->
                connection.stringCommands().getBit(key.getBytes(StandardCharsets.UTF_8), offset));
        return Boolean.TRUE.equals(bit);
    }

    /**
     * 读取 SDS 原始字节（固定结构，长度=字段数×4）。
     */
    private byte[] getRaw(String key) {
        return redis.execute((RedisCallback<byte[]>) connection ->
                connection.stringCommands().get(key.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * 写入 SDS 原始字节（覆盖式写）。
     */
    private void setRaw(String key, byte[] val) {
        redis.execute((RedisCallback<Void>) connection -> {
            connection.stringCommands().set(key.getBytes(StandardCharsets.UTF_8), val);
            return null;
        });
    }

    /**
     * 是否处于指数退避期：期间跳过重建并返回降级结果。
     */
    private boolean inBackoff(String entityType, String entityId) {
        String bKey = String.format("backoff:sds-rebuild:until:%s:%s", entityType, entityId);
        RBucket<Long> bucket = redisson.getBucket(bKey);
        Long until = bucket.get();

        return until != null && System.currentTimeMillis() < until;
    }

    /**
     * 增加退避级别并设置下次允许尝试的时间（指数递增，封顶）。
     */
    private void escalateBackoff(String entityType, String entityId) {
        String eKey = String.format("backoff:sds-rebuild:exp:%s:%s", entityType, entityId);
        String uKey = String.format("backoff:sds-rebuild:until:%s:%s", entityType, entityId);

        RBucket<Integer> expB = redisson.getBucket(eKey);
        RBucket<Long> untilB = redisson.getBucket(uKey);
        Integer exp = expB.get();

        int nextExp = Math.min(exp == null ? 0 : exp + 1, 10);
        long delay = Math.min(backoffBaseMs * (1L << nextExp), backoffMaxMs);
        long until = System.currentTimeMillis() + delay;

        // 设置过期时间，避免长时间残留
        expB.set(nextExp);
        untilB.set(until, Duration.ofMillis(delay + 1000));
    }

    /**
     * 重置退避状态（成功重建后）。
     */
    private void resetBackoff(String entityType, String entityId) {
        String eKey = String.format("backoff:sds-rebuild:exp:%s:%s", entityType, entityId);
        String uKey = String.format("backoff:sds-rebuild:until:%s:%s", entityType, entityId);

        try {
            redisson.getBucket(eKey).delete();
        } catch (Exception ignore) {}

        try {
            redisson.getBucket(uKey).delete();
        } catch (Exception ignore) {}
    }

    /**
     * 限流判断：单位窗口可重建次数，防止抖动与风暴。
     */
    private boolean allowedByRateLimiter(String entityType, String entityId) {
        String rlKey = String.format("rl:sds-rebuild:%s:%s", entityType, entityId);
        RRateLimiter limiter = redisson.getRateLimiter(rlKey);

        // 初始化速率（如已存在则忽略）
        limiter.trySetRate(RateType.OVERALL, ratePermits, Duration.ofSeconds(rateWindowSeconds));

        return limiter.tryAcquire(1);
    }

    /**
     * 以大端序读取 32 位无符号整型。
     */
    private static long readInt32BE(byte[] buf, int off) {
        long n = 0;
        for (int i = 0; i < 4; i++) {
            n = (n << 8) | (buf[off + i] & 0xFFL);
        }
        return n;
    }

    /**
     * 以大端序写入 32 位无符号整型（截断到 0~2^32-1）。
     */
    private static void writeInt32BE(byte[] buf, int off, long val) {
        long n = Math.max(0, Math.min(val, 0xFFFF_FFFFL));
        buf[off] = (byte) ((n >>> 24) & 0xFF);
        buf[off + 1] = (byte) ((n >>> 16) & 0xFF);
        buf[off + 2] = (byte) ((n >>> 8) & 0xFF);
        buf[off + 3] = (byte) (n & 0xFF);
    }

    /**
     * 基于位图分片进行管道化 BITCOUNT 汇总，用于按事实重建计数。
     * 说明：当前使用 KEYS 枚举分片（生产建议维护索引集合），结果按分片 BITCOUNT 求和。
     */
    private long bitCountShardsPipelined(String metric, String etype, String eid) {
        String pattern = String.format("bm:%s:%s:%s:*", metric, etype, eid);
        // 生产环境建议以索引集合替代 KEYS
        Set<String> keys = redis.keys(pattern); 
        if (keys.isEmpty()) return 0L;

        // 管道批量 BITCOUNT 汇总
        List<Object> res = redis.executePipelined((RedisCallback<Object>) connection -> {
            for (String k : keys) {
                connection.stringCommands().bitCount(k.getBytes(StandardCharsets.UTF_8));
            }
            return null;
        });
        long sum = 0L;

        for (Object o : res) {
            if (o instanceof Number n) {
                sum += n.longValue();
            }
        }
        return sum;
    }
 
    // Redis 内嵌 Lua（Redis 5/6 的 Lua 5.1），位图原子切换（分片内偏移）
    private static final String TOGGLE_LUA = """
            local bmKey = KEYS[1]
            local offset = tonumber(ARGV[1])
            local op = ARGV[2] -- 'add' or 'remove'
            local prev = redis.call('GETBIT', bmKey, offset)
            if op == 'add' then
              if prev == 1 then return 0 end
              redis.call('SETBIT', bmKey, offset, 1)
              return 1
            elseif op == 'remove' then
              if prev == 0 then return 0 end
              redis.call('SETBIT', bmKey, offset, 0)
              return 1
            end
            return -1
            """;

    // SDS 字段原子增减（大端 Int32），用于同步更新计数汇总
    private static final String INCR_FIELD_LUA = """
            local cntKey = KEYS[1]
            local schemaLen = tonumber(ARGV[1])
            local fieldSize = tonumber(ARGV[2])
            local idx = tonumber(ARGV[3])
            local delta = tonumber(ARGV[4])
            
            local function read32be(s, off)
              local b = {string.byte(s, off+1, off+4)}
              local n = 0
              for i=1,4 do n = n * 256 + b[i] end
              return n
            end
            
            local function write32be(n)
              local t = {}
              for i=4,1,-1 do t[i] = n % 256; n = math.floor(n/256) end
              return string.char(unpack(t))
            end
            
            local cnt = redis.call('GET', cntKey)
            if not cnt then cnt = string.rep(string.char(0), schemaLen * fieldSize) end
            local off = idx * fieldSize
            local v = read32be(cnt, off) + delta
            if v < 0 then v = 0 end
            local seg = write32be(v)
            cnt = string.sub(cnt, 1, off) .. seg .. string.sub(cnt, off+fieldSize+1)
            redis.call('SET', cntKey, cnt)
            return 1
            """;
}