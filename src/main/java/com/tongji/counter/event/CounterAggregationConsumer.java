package com.tongji.counter.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tongji.counter.schema.CounterKeys;
import com.tongji.counter.schema.CounterSchema;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Set;
import java.util.List;

/**
 * 计数事件聚合与刷写消费者。
 *
 * 职责：
 * - 消费点赞/收藏等增量事件，写入 Redis 聚合桶（Hash）；
 * - 以固定延迟定时任务将聚合增量折叠到 SDS 固定结构计数；
 * - 刷写成功后删除聚合字段，避免重复加算。
 */
@Slf4j
@Service
public class CounterAggregationConsumer {

    private final ObjectMapper objectMapper;
    private final StringRedisTemplate redis;
    private final DefaultRedisScript<Long> incrScript;
    private final DefaultRedisScript<Long> decrScript;
    private int flushTick;

    // 使用 Redis Hash 作为持久化聚合桶：agg:{schema}:{etype}:{eid} ，field=idx ，value=delta
    public CounterAggregationConsumer(ObjectMapper objectMapper, StringRedisTemplate redis) {
        this.objectMapper = objectMapper;
        this.redis = redis;
        this.incrScript = new DefaultRedisScript<>();
        this.incrScript.setResultType(Long.class);
        this.incrScript.setScriptText(INCR_FIELD_LUA);

        this.decrScript = new DefaultRedisScript<>();
        this.decrScript.setResultType(Long.class);
        this.decrScript.setScriptText(DECR_FIELD_LUA);
    }

    @PostConstruct
    public void init() {
        log.info("CounterAggregationConsumer 初始化完成, 准备消费 topic={}", CounterTopics.EVENTS);
    }

    /**
     * 消费计数事件并写入聚合桶。
     * @param message 事件 JSON
     * @param ack 位点确认对象（手动提交）
     */
    @KafkaListener(topics = CounterTopics.EVENTS)
    // 手动ACK主要是用于那些强一致性的场景，确保每个事件都被处理。
    public void onMessage(String message, Acknowledgment ack) throws Exception {
        log.info("收到Kafka计数事件: {}", message);
        // kafka只能传递字符串，所以需要反序列化为 CounterEvent 对象
        CounterEvent evt = objectMapper.readValue(message, CounterEvent.class);
        String aggKey = CounterKeys.aggKey(evt.getEntityType(), evt.getEntityId());
        String field = String.valueOf(evt.getIdx());
        try {
            // 将增量持久化到 Redis Hash
            redis.opsForHash().increment(aggKey, field, evt.getDelta());
            log.info("聚合桶更新成功: key={}, field={}, delta={}", aggKey, field, evt.getDelta());
            // 成功后提交位点，绑定"已持久化"语义
            ack.acknowledge();
        } catch (Exception ex) {
            log.error("处理计数事件失败: key={}, field={}, delta={}", aggKey, field, evt.getDelta(), ex);
            // 不提交位点以便重试
        }
    }

    /**
     * 将聚合增量刷写到 SDS 固定结构计数。
     * 固定延迟 1s，保证秒级最终一致性。
     */
    @Scheduled(fixedDelay = 1000L)
    public void flush() {
        flushTick++;
        // KEYS 会遍历整个 Redis 键空间，执行期间 Redis 是单线程阻塞的，无法处理其他请求
        // 生产环境建议使用索引集合（如 Redis Set）来存储聚合桶键，避免遍历所有键
        Set<String> keys = redis.keys("agg:" + CounterSchema.SCHEMA_ID + ":*");
        if (keys.isEmpty()) {
            if (flushTick % 30 == 1) {
                log.info("flush 心跳 #{}，当前无聚合桶待刷写", flushTick);
            }
            return;
        }

        log.info("开始刷写聚合桶，共 {} 个键", keys.size());

        for (String aggKey : keys) {
//            entries 追求 “全且准”，但代价是 “堵死系统”；
//            scan 追求 “稳且流畅”，但代价是 “可能多取或少取”
//            数据量 > 5000 条，且数据高频变化（比如实时在线用户列表）：
//            如果业务允许一点点偏差，用 scan；如果不允许任何遗漏，必须放弃 HGETALL**，
//            改为在设计上维护一个单独的 Set 来记录所有 Field，或者用其他中间件，因为 HGETALL 在大数据量下是绝对不能用的。
            Map<Object, Object> entries = redis.opsForHash().entries(aggKey);
            if (entries.isEmpty()) {
                continue;
            }
            // 解析 etype/eid 以定位 SDS key
            String[] parts = aggKey.split(":"); // agg:schema:etype:eid
            if (parts.length < 4) {
                continue;
            }

            String cntKey = CounterKeys.sdsKey(parts[2], parts[3]);
            log.info("刷写聚合桶: aggKey={}, cntKey={}, entries={}", aggKey, cntKey, entries);

//redis.opsForHash()：获取 Redis 的 Hash 操作对象（HashOperations），专门用于操作 Redis 中的 Hash 数据类型。
//.entries(aggKey)：执行 Redis 的 HGETALL 命令。它会一次性拉取 aggKey 这个 Hash 中 所有的 字段（Field）和对应的值（Value），并将其封装成一个 Java 的 Map<Object, Object> 对象返回给你。
//.entrySet()：将上一步返回的 Map 对象转换为 Set<Map.Entry<Object, Object>>。这样你就可以使用增强 for 循环或 Stream 流来遍历每个键值对了。
            for (Map.Entry<Object, Object> e : entries.entrySet()) {
                String field = String.valueOf(e.getKey());
                // 增量
                long delta;
                try {
//                    把 Redis Hash 的值转为 long 类型：先转为 String，再转为 long
                   delta = Long.parseLong(String.valueOf(e.getValue()));
                } catch (NumberFormatException nfe) {
                    continue;
                }
                if (delta == 0) continue;
                int idx;

                try {
                    idx = Integer.parseInt(field);
                } catch (NumberFormatException nfe) {
                    continue;
                }

                try {
//                    允许你直接操作底层的 Redis 连接（Connection），执行任何原生的 Redis 命令或 Lua 脚本。
                    redis.execute(incrScript, List.of(cntKey),
                            String.valueOf(CounterSchema.SCHEMA_LEN),
                            String.valueOf(CounterSchema.FIELD_SIZE),
                            String.valueOf(idx),
                            String.valueOf(delta));
                    log.info("SDS刷写成功: cntKey={}, idx={}, delta={}", cntKey, idx, delta);

                    // 成功后扣减该字段，若结果为0则删除，避免并发写入丢失
                    redis.execute(decrScript, List.of(aggKey), field, String.valueOf(delta));
                } catch (Exception ex) {
                    log.error("SDS刷写失败: cntKey={}, idx={}, delta={}", cntKey, idx, delta, ex);
                    // 留存字段，下一轮重试
                }
            }
            // 如 Hash 已为空，删除聚合桶Key
            // 目的：降低键空间噪音，避免后续无效扫描
            Long size = redis.opsForHash().size(aggKey);
            if (size == 0L) {
                redis.delete(aggKey);
                log.info("聚合桶已清空并删除: {}", aggKey);
            }
        }
    }
//TODO 这里的没看懂，大端int之类的，不过核心是sds在读多写少的情况下占优势
    private static final String INCR_FIELD_LUA = """
            
            local cntKey = KEYS[1]
            local schemaLen = tonumber(ARGV[1])
            local fieldSize = tonumber(ARGV[2]) -- 固定为4
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

    private static final String DECR_FIELD_LUA = """
            local key = KEYS[1]
            local field = ARGV[1]
            local delta = tonumber(ARGV[2])
            local v = redis.call('HINCRBY', key, field, -delta)
            if v == 0 then
                redis.call('HDEL', key, field)
            end
            return v
            """;
}