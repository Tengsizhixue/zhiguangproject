package com.tongji.cache.hotkey;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.tongji.cache.config.CacheProperties;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * 热键探测器（滑动时间窗口计数 + 热度分级 + TTL 动态扩展）。
 * <p>
 * 设计说明：
 * - 采用固定分段滑动窗口：窗口长度 windowSeconds，分段长度 segmentSeconds，段数 segments=window/segment；
 * - 每个 key 维护长度为 segments 的数组 counters[key]，current 指向当前活跃段；
 * - 周期性 rotate 将 current 前移并清零新段，实现近窗口热度的自然衰减；
 * - 根据总热度 h=Σ段计数，映射到 NONE/LOW/MEDIUM/HIGH 的热度等级；
 * - 提供 ttlForPublic/ttlForMine：在基准 TTL 上叠加等级扩展秒数，保护热点请求。
 * <p>
 * 并发语义：
 * - 使用 ConcurrentHashMap 存储计数数组，AtomicInteger 维护段游标；
 * - 计数递增为无锁数组操作，rotate 仅清零新段，避免大范围写冲突；
 * - 统计为近似滑窗，保证在高并发下的稳定与低开销。
 */
@Component
//TODO  （已修改）1、rotate() 定时任务虽然会把数组里的数字清零，
// 但它从来没有把这个 Key 从 Map 里删掉！
// 2、在 Java 中，对普通的基本类型数组元素执行 ++ 操作，根本不是原子性的！它分为三步：读取、加一、写回。
// 线程 A 读到数字是 100，还没来得及加，线程 B 也读到了 100。最后两个线程算完都写回 101。两次点击，只加了 1 次！
// 3、解决方法：使用 AtomicIntegerArray 替换普通数组，每个元素都是原子整型，支持并发加一。
//用于记录某个 key 的访问次数，是热键检测的核心功能
public class HotKeyDetector {
    public enum Level { NONE, LOW, MEDIUM, HIGH }

    private final CacheProperties properties;

    // 💡 修复一：使用 Caffeine 本地缓存替换 ConcurrentHashMap
    // 存储的结构变为：Cache<String, 原子整型数组>
    private final Cache<String, AtomicIntegerArray> counters;

    private final AtomicInteger current = new AtomicInteger(0);
    private final int segments;

    public HotKeyDetector(CacheProperties properties) {
        this.properties = properties;
        int segSeconds = properties.getHotkey().getSegmentSeconds();
        int winSeconds = properties.getHotkey().getWindowSeconds();
        this.segments = Math.max(1, winSeconds / Math.max(1, segSeconds));

        // 💡 修复一配置：建立具备“自动淘汰”能力的内存池
        // 假设总窗口是 60 秒，我们设置如果一个帖子超过 5 分钟没人看，
        // Caffeine 就会自动把它从内存中抹除，彻底杜绝 OOM 内存泄漏！
        this.counters = Caffeine.newBuilder()
                //如果这个帖子连续 5 分钟没有被任何人点击（record 或 heat），Caffeine 就会在后台偷偷把它彻底销毁删掉
                .expireAfterAccess(5, TimeUnit.MINUTES)
                .build();
    }

    public void record(String key) {
        // 💡 修复二：如果不存在，分配一个 AtomicIntegerArray
        AtomicIntegerArray arr = counters.get(key, k -> new AtomicIntegerArray(segments));
        if (arr != null) {
            // 💡 修复二核心：使用底层 CAS 操作替代原生的 arr[i]++
            //原来的 arr[i]++ 翻译到底层其实是 读内存 -> 加 1 -> 写回内存 三步。
            // 高并发下，线程 A 和 线程 B 读到了同一个旧数字，最终少加了一次
            // 它在底层会直接调用 CPU 的 CAS（Compare-And-Swap 比较并交换）指令。
            // 它会在写回的瞬间去检查“这期间有没有别人改过？”，如果有，它就自旋重试。
            // 这保证了哪怕 1 秒钟内一百万人点击，最终的结果也必定精确到 1,000,000 次，一次都不会丢！。
            // 哪怕一万个线程同时点这个帖子，数字也绝对不会少算一个！
            arr.getAndIncrement(current.get());
        }
    }

    public int heat(String key) {
        // 从 Caffeine 中安全获取
        AtomicIntegerArray arr = counters.getIfPresent(key);
        if (arr == null) {
            return 0;
        }

        int sum = 0;
        for (int i = 0; i < segments; i++) {
            // 安全读取原子数组里的值
            sum += arr.get(i);
        }
        return sum;
    }

    public Level level(String key) {
        int h = heat(key);
        if (h >= properties.getHotkey().getLevelHigh()) return Level.HIGH;
        if (h >= properties.getHotkey().getLevelMedium()) return Level.MEDIUM;
        if (h >= properties.getHotkey().getLevelLow()) return Level.LOW;

        return Level.NONE;
    }

    public int ttlForPublic(int baseTtlSeconds, String key) {
        return baseTtlSeconds + extendSeconds(level(key));
    }

    public int ttlForMine(int baseTtlSeconds, String key) {
        return baseTtlSeconds + extendSeconds(level(key));
    }

    private int extendSeconds(Level l) {
        return switch (l) {
            case HIGH -> properties.getHotkey().getExtendHighSeconds();
            case MEDIUM -> properties.getHotkey().getExtendMediumSeconds();
            case LOW -> properties.getHotkey().getExtendLowSeconds();
            default -> 0;
        };
    }

    @Scheduled(fixedRateString = "${cache.hotkey.segment-seconds:10}000")
    public void rotate() {
        int next = (current.get() + 1) % segments;
        current.set(next);

        // counters.asMap().values() 遍历时，Caffeine 会自动忽略掉那些已经过期被淘汰的旧 Key
        // 大大减少了 for 循环的压力
        for (AtomicIntegerArray arr : counters.asMap().values()) {
            // 💡 修复二：安全地将新的一格清零
            arr.set(next, 0);
        }
    }

    public void reset(String key) {
        AtomicIntegerArray arr = counters.getIfPresent(key);
        if (arr != null) {
            for (int i = 0; i < segments; i++) {
                arr.set(i, 0);
            }
        }
    }
}
///**public class HotKeyDetector {
// public enum Level { NONE, LOW, MEDIUM, HIGH }
//
// /** 缓存配置（包含窗口/分段参数、等级阈值、扩展秒数） */
//private final CacheProperties properties;
///** 每个 key 的滑窗分段计数数组，长度为 segments */
//private final Map<String, int[]> counters = new ConcurrentHashMap<>();
///** 当前活跃分段索引（原子维护） */
//private final AtomicInteger current = new AtomicInteger(0);
///** 滑窗分段数量：windowSeconds / segmentSeconds */
//private final int segments;
//
///**
// * 初始化探测器：根据配置计算分段数量。
// * @param properties 缓存配置（hotkey）
// */
//public HotKeyDetector(CacheProperties properties) {
//    this.properties = properties;
//    int segSeconds = properties.getHotkey().getSegmentSeconds();
//    int winSeconds = properties.getHotkey().getWindowSeconds();
//    this.segments = Math.max(1, winSeconds / Math.max(1, segSeconds));
//}
//
///**
// * 记录一次访问，将计数累加到当前分段。
// * @param key 缓存键
// */
//public void record(String key) {
//    // 如果这个帖子是第一次被访问，
//    // 就给它分配一个长度为 segments 的 int 数组（比如长度为 6）。
//    // 如果之前访问过，就把它的数组拿出来。
//    int[] arr = counters.computeIfAbsent(key, k -> new int[segments]);
//    // 2. current.get() 拿到系统当前的时间游标,然后直接给数组的这一格加 1！
//    arr[current.get()]++;
//}
//
///**
// * 计算近窗口总热度（各分段求和）。
// * @param key 缓存键
// * @return 热度值
// */
//public int heat(String key) {
//    int[] arr = counters.get(key);
//    if (arr == null) {
//        return 0;
//    }
//
//    int sum = 0;
//    for (int v : arr) {
//        sum += v;
//    }
//    return sum;
//}
//
///**
// * 计算热度评级：根据总热度与阈值映射到等级。
// * 阈值来源：properties.hotkey.levelLow/Medium/High。
// * @param key 缓存键
// * @return 热度等级
// */
//public Level level(String key) {
//    //算出它的近期真实点击总数
//    int h = heat(key);
//    //根据总热度与阈值映射到等级
//    if (h >= properties.getHotkey().getLevelHigh()) {
//        return Level.HIGH;
//    }
//    if (h >= properties.getHotkey().getLevelMedium()) {
//        return Level.MEDIUM;
//    }
//    if (h >= properties.getHotkey().getLevelLow()) {
//        return Level.LOW;
//    }
//
//    return Level.NONE;
//}
//
///**
// * 计算公共页面的动态 TTL：基准 TTL + 等级扩展秒数。
// * @param baseTtlSeconds 基准 TTL 秒数
// * @param key 缓存键
// * @return 动态 TTL 秒数
// */
//public int ttlForPublic(int baseTtlSeconds, String key) {
//    //先去算一算，它当前的热度等级到底算老几？
//    Level l = level(key);
//    //根据等级返回扩展秒数
//    return baseTtlSeconds + extendSeconds(l);
//}
//
///**
// * 计算“我的发布”页面的动态 TTL：基准 TTL + 等级扩展秒数。
// * @param baseTtlSeconds 基准 TTL 秒数
// * @param key 缓存键
// * @return 动态 TTL 秒数
// */
//public int ttlForMine(int baseTtlSeconds, String key) {
//    Level l = level(key);
//    return baseTtlSeconds + extendSeconds(l);
//}
//
///**
// * 根据热度等级返回扩展秒数。
// * @param l 热度等级
// * @return 扩展秒数
// */
//private int extendSeconds(Level l) {
//    return switch (l) {
//        //根据等级返回扩展秒数
//        case HIGH -> properties.getHotkey().getExtendHighSeconds();
//        case MEDIUM -> properties.getHotkey().getExtendMediumSeconds();
//        case LOW -> properties.getHotkey().getExtendLowSeconds();
//        default -> 0;
//    };
//}
//
///**
// * 定时轮转当前分段，清零新分段以实现滑动窗口统计。
// * 触发频率由配置 `cache.hotkey.segment-seconds` 指定（单位秒）。
// */
//// 每 10 秒执行一次
//@Scheduled(fixedRateString = "${cache.hotkey.segment-seconds:10}000")
//public void rotate() {
//    // 1. current 游标往前走一格。如果超过了 6 格，就回到 0
//    int next = (current.get() + 1) % segments;
//    // 2. 更新当前活跃分段索引
//    current.set(next);
//    // 3. 清零分段的计数
//    for (int[] arr : counters.values()) {
//        arr[next] = 0;
//    }
//}
//
///**
// * 重置指定 key 的滑窗计数（全部清零）。
// * 用于手动降级或在配置变更后清理历史热度。
// * @param key 缓存键
// */
//public void reset(String key) {
//    int[] arr = counters.get(key);
//    if (arr != null) Arrays.fill(arr, 0);
//}
//}*/
//
