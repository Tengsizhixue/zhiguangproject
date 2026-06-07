package com.tongji.knowpost.service.impl;

import com.tongji.knowpost.service.KnowPostFeedService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tongji.knowpost.api.dto.FeedItemResponse;
import com.tongji.knowpost.api.dto.FeedPageResponse;
import com.tongji.knowpost.mapper.KnowPostMapper;
import com.tongji.knowpost.model.KnowPostFeedRow;
import com.tongji.counter.service.CounterService;
import com.github.benmanes.caffeine.cache.Cache;
import com.tongji.cache.hotkey.HotKeyDetector;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.ConcurrentHashMap;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class KnowPostFeedServiceImpl implements KnowPostFeedService {

    private final KnowPostMapper mapper;
    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final CounterService counterService;
    private final Cache<String, FeedPageResponse> feedPublicCache;
    private final Cache<String, FeedPageResponse> feedMineCache;
    private final HotKeyDetector hotKey;
    private static final Logger log = LoggerFactory.getLogger(KnowPostFeedServiceImpl.class);
    private static final int LAYOUT_VER = 1;
    private final ConcurrentHashMap<String, Object> singleFlight = new ConcurrentHashMap<>();

    /**
     * 构造函数：注入 Mapper、Redis、对象映射器、计数服务与本地缓存。
     * @param mapper 数据访问层
     * @param redis Redis 客户端
     * @param objectMapper JSON 序列化/反序列化器
     * @param counterService 点赞/收藏计数服务
     * @param feedPublicCache 首页公共 Feed 本地缓存
     * @param feedMineCache 我的发布 Feed 本地缓存
     * @param hotKey 热点 Key 检测器，用于动态延长 TTL
     */
    @Autowired
    public KnowPostFeedServiceImpl(
            KnowPostMapper mapper,
            StringRedisTemplate redis,
            ObjectMapper objectMapper,
            CounterService counterService,
            @Qualifier("feedPublicCache") Cache<String, FeedPageResponse> feedPublicCache,
            @Qualifier("feedMineCache") Cache<String, FeedPageResponse> feedMineCache,
            HotKeyDetector hotKey
    ) {
        this.mapper = mapper;
        this.redis = redis;
        this.objectMapper = objectMapper;
        this.counterService = counterService;
        this.feedPublicCache = feedPublicCache;
        this.feedMineCache = feedMineCache;
        this.hotKey = hotKey;
    }

    /**
     * 生成公共 Feed 页面的全局统一缓存 Key（包含分页与布局版本）。
     * L1 和 L2 统一使用此逻辑，不再做人为的时间割裂。
     * * @param page 页码（1 起）
     * @param size 每页大小
     * @return Redis/Page 缓存的 Key
     */
    private String cacheKey(int page, int size) {
        return "feed:public:" + size + ":" + page + ":v" + LAYOUT_VER;
    }

    /**
     * 获取公开的首页 Feed（按发布时间倒序，不受置顶影响）。
     * 采用三级缓存：本地 Caffeine、Redis 页面缓存、Redis 片段缓存（ids/item/count）。
     * * @param page 页码（≥1）
     * @param size 每页数量（1~50）
     * @param currentUserIdNullable 当前用户 ID（为空表示匿名）
     * @return 带分页信息的 Feed 列表（liked/faved 为当前请求维度的私人状态）
     */
    @Override
    public FeedPageResponse getPublicFeed(int page, int size, Long currentUserIdNullable) {
        //     - Math.max(size, 1)：确保 size 至少为 1，防止请求 0 条或负数条数据
        //     - Math.min(..., 50)：确保 size 不超过 50，防止恶意请求超大分页拖垮数据库
//        - Math.max(page, 1)：确保页码至少为 1，防止负数页码导致 SQL OFFSET 异常
        int safeSize = Math.min(Math.max(size, 1), 50);
        int safePage = Math.max(page, 1);

        // 1.2 构造 L1 本地缓存 Key
        String localPageKey = cacheKey(safePage, safeSize);

        // 1.3 构造 L2 Redis 缓存 Key
        //作用：存储第一页的 20 个帖子 ID 列表（JSON 数组）
        //hasMoreKey：标记该分页是否有下一页，作用：存储 "1" 表示有下一页，避免每次都要查数据库判断

        String idsKey = "feed:public:ids:" + safeSize + ":" + safePage;
        String hasMoreKey = idsKey + ":hasMore";

        // 查询流程：1. 从 Caffeine 中按 localPageKey 查找
        // 2. 如果命中且 items 不为空，记录热度并延长相关缓存 TTL
        // 3. 调用 enrich() 为当前用户叠加私人状态（liked/faved）
        // 4. 返回带用户私人状态的响应
        FeedPageResponse local = feedPublicCache.getIfPresent(localPageKey);
        if (local != null && local.items() != null) {
            // L1 命中：遍历每个帖子，记录其热度
            // 热点内容会被识别并延长其在 Redis 中的缓存时间
            for (FeedItemResponse item : local.items()) {
                recordItemHotKey(item.id());
            }
            log.info("feed.public source=local localPageKey={} page={} size={}", localPageKey, safePage, safeSize);

            //缓存里拿出来的永远是绝对干净的公共数据
            // 返回前，单独为当前请求的用户实时叠加私人状态（如：是否点赞）
            // 这样即使缓存被多个用户共享，也不会泄露任何用户的私人数据
            List<FeedItemResponse> enrichedLocal = enrich(local.items(), currentUserIdNullable);
            return new FeedPageResponse(enrichedLocal, local.page(), local.size(), local.hasMore());
        }

        // 查询流程：
        // 1. 从 Redis 中按 idsKey 获取 ID 列表
        // 2. 根据 ID 列表批量获取每个帖子的摘要片段
        // 3. 组装成 FeedPageResponse
        FeedPageResponse cleanFromL2 = assembleFromCache(idsKey, hasMoreKey, safePage, safeSize, null);
        if (cleanFromL2 != null) {
            feedPublicCache.put(localPageKey, cleanFromL2); // 存入 L1 的是干净数据
            // L2 命中：将干净数据回填到 L1 本地缓存
            // 这样下次请求同一分页时可以直接从 L1 命中，跳过 Redis 查询
            if (cleanFromL2.items() != null) {
                for (FeedItemResponse item : cleanFromL2.items()) {
                    // 记录每个帖子的热度，用于后续缓存 TTL 动态调整
                    recordItemHotKey(item.id());
                }
            }
            log.info("feed.public source=3tier localPageKey={} page={} size={}", localPageKey, safePage, safeSize);

            // 组装当前用户的私人状态并返回给前端
            List<FeedItemResponse> enrichedL2 = enrich(cleanFromL2.items(), currentUserIdNullable);
            return new FeedPageResponse(enrichedL2, cleanFromL2.page(), cleanFromL2.size(), cleanFromL2.hasMore());
        }

        // 当 L1 和 L2 都未命中时，需要查询 MySQL 数据库。
        // 为防止高并发下大量请求同时打到数据库（缓存击穿/惊群效应），
        // 使用 SingleFlight 模式：对同一个 idsKey，只允许一个线程执行数据库查询，
        // 其他线程在 synchronized 块外排队等待。
        // SingleFlight 实现原理：
        // 1. singleFlight 是一个 ConcurrentHashMap<String, Object>
        // 2. computeIfAbsent：如果 Key 不存在，创建一个新的 Object 作为锁
        // 3. synchronized(lock)：只有获取到锁的线程才能执行数据库查询
        // 4. 其他线程在 synchronized 块外阻塞等待
        // 5. 第一个线程查询完成后写入缓存并释放锁
        // 6. 等待中的线程获取锁后，通过 Double-Check 直接从缓存获取结果
        Object lock = singleFlight.computeIfAbsent(idsKey, k -> new Object());
        synchronized (lock) {
            // 使用 try-finally 保护区，确保无论发生什么情况（查库超时、
            // 抛出异常、甚至 OOM），锁对象都能被从 singleFlight Map 中移除，
            // 彻底杜绝死锁与内存泄漏！
            try {
                // 在获取锁的等待过程中，前一个线程可能已经完成了数据库查询
                // 并把结果写入了 Redis 缓存。因此需要再次检查缓存，
                // 避免重复查询数据库。
                // 同样传入 null 确保拿到的数据是绝对干净的。
                FeedPageResponse againClean = assembleFromCache(idsKey, hasMoreKey, safePage, safeSize, null);
                if (againClean != null) {
                    // 缓存已被前一个线程填充，直接使用
                    feedPublicCache.put(localPageKey, againClean);
                    if (againClean.items() != null) {
                        for (FeedItemResponse item : againClean.items()) {
                            recordItemHotKey(item.id());
                        }
                    }
                    log.info("feed.public source=3tier(after-flight) localPageKey={} page={} size={}", localPageKey, safePage, safeSize);

                    List<FeedItemResponse> enrichedAgain = enrich(againClean.items(), currentUserIdNullable);
                    return new FeedPageResponse(enrichedAgain, againClean.page(), againClean.size(), againClean.hasMore());
                }

                // 真正去查 MySQL 数据库：读取 size+1 以判断是否有下一页
                int offset = (safePage - 1) * safeSize;
                // 查询 size+1 条记录：多查一条用于判断是否有下一页
                // 例如：请求 20 条，实际查询 21 条
                // - 如果返回 21 条 → hasMore = true，截取前 20 条返回
                // - 如果返回 ≤20 条 → hasMore = false，全部返回
                // 这种"多查一条"的策略避免了额外的 COUNT(*) 查询，性能更优
                List<KnowPostFeedRow> rows = mapper.listFeedPublic(safeSize + 1, offset);
                boolean hasMore = rows.size() > safeSize;
                if (hasMore) {
                // 有下一页：截取前 safeSize 条，丢弃多查的那一条
                    rows = rows.subList(0, safeSize);
                }
                // 4.3 构建干净的缓存数据
                // 将数据库行映射为 FeedItemResponse 列表
                // 传入 null 作为 userId：liked/faved 置为 null，确保缓存数据绝对干净
                // 传入 false：不从缓存读取，因为这是数据库回源
                List<FeedItemResponse> cleanItems = mapRowsToItems(rows, null, false);
                // 构建用于缓存的响应对象（liked/faved 均为 null，绝对纯净）
                FeedPageResponse respForCache = new FeedPageResponse(cleanItems, safePage, safeSize, hasMore);

                // 基础 TTL：60 秒
                // 随机抖动：0~29 秒（使用 ThreadLocalRandom，线程安全且高性能）
                // 实际过期时间：60~89 秒之间的随机值
                int baseTtl = 60;
                int jitter = ThreadLocalRandom.current().nextInt(30);
                Duration frTtl = Duration.ofSeconds(baseTtl + jitter);

                // 4.5 写入多级缓存
                // writeCaches 将数据写入 Redis 的三个 Key：
                // 1. idsKey：存储该分页的帖子 ID 列表（JSON 数组）
                // 2. hasMoreKey：存储是否有下一页的标记
                // 3. feed:item:{id}：存储每个帖子的摘要片段
                // 同时将 respForCache 写入 L1 本地缓存
                // 注意：此处写入的所有缓存数据都是绝对纯净的（liked/faved = null）
                writeCaches(localPageKey, idsKey, hasMoreKey, safeSize, rows, cleanItems, hasMore, frTtl);
                feedPublicCache.put(localPageKey, respForCache);
                log.info("feed.public source=db localPageKey={} page={} size={} hasMore={}", localPageKey, safePage, safeSize, hasMore);

                // 最后一步：为当前请求的用户实时查询 liked/faved 状态
                // 这样即使缓存是全局共享的，每个用户看到的都是自己的私人数据
                // 实现了"缓存共享，状态隔离"的架构设计
                List<FeedItemResponse> enriched = enrich(cleanItems, currentUserIdNullable);
                return new FeedPageResponse(enriched, safePage, safeSize, hasMore);

            } finally {
                singleFlight.remove(idsKey);
            }
        }
    }


    /**
     * 记录单个内容条目的热度，并尝试延长其相关片段缓存的 TTL。
     * @param itemId 内容 ID
     */
    private void recordItemHotKey(String itemId) {
        // 使用内容 ID 作为热点统计 Key，而不是页面 Key
        String hotKeyId = "knowpost:" + itemId;
        hotKey.record(hotKeyId);
        
        int baseTtl = 60;
        int target = hotKey.ttlForPublic(baseTtl, hotKeyId);
        
        // 延长该内容的详情片段缓存
        String itemKey = "feed:item:" + itemId;
        Long itemTtl = redis.getExpire(itemKey);
        if (itemTtl < target) {
            redis.expire(itemKey, Duration.ofSeconds(target));
        }
    }

    /**
     * 叠加用户维度状态，将 liked/faved 根据用户计算覆盖到列表上。
     * 不改写底层缓存，避免不同用户状态互相污染。
     * @param base 基础列表（含计数）
     * @param uid 用户 ID（可空）
     * @return 叠加 liked/faved 的列表
     */
    private List<FeedItemResponse> enrich(List<FeedItemResponse> base, Long uid) {
        List<FeedItemResponse> out = new ArrayList<>(base.size());

        for (FeedItemResponse it : base) {
            boolean liked = uid != null && counterService.isLiked("knowpost", it.id(), uid);
            boolean faved = uid != null && counterService.isFaved("knowpost", it.id(), uid);
            out.add(new FeedItemResponse(
                    it.id(),
                    it.title(),
                    it.description(),
                    it.coverImage(),
                    it.tags(),
                    it.authorAvatar(),
                    it.authorNickname(),
                    it.tagJson(),
                    it.likeCount(),
                    it.favoriteCount(),
                    liked,
                    faved,
                    it.isTop()
            ));
        }
        return out;
    }

    /**
     * 从 Redis 片段缓存组装页面：
     * - idsKey：列表 ID 顺序
     * - itemKey：每个条目基础信息
     * - countKey：点赞/收藏计数
     * 若缺片段则回源修补并写回软缓存。
     * @param idsKey Redis 列表 Key
     * @param hasMoreKey Redis 软缓存 hasMore Key
     * @param page 页码
     * @param size 每页大小
     * @param uid 当前用户 ID（用于 liked/faved）
     * @return 组装完成的页面；不存在时返回 null
     */
    private FeedPageResponse assembleFromCache(String idsKey, String hasMoreKey, int page, int size, Long uid) {
        // 第一步：从 Redis 获取分页的帖子 ID 列表
        // idsKey 格式：feed:public:ids:{size}:{page}
        // Redis 数据类型：List，使用 LRANGE 获取指定范围的元素
        // 例如 size=20 时，获取索引 0~19 共 20 个帖子 ID
        List<String> idList = redis.opsForList().range(idsKey, 0, size - 1);
        // 第二步：获取 hasMore 标记
        // hasMoreKey 存储 "1" 表示有下一页，不存在或其他值表示没有
        // 如果 ID 列表为空，说明缓存中不存在该分页数据，返回 null 触发回源
        String hasMoreStr = redis.opsForValue().get(hasMoreKey);
        if (idList == null || idList.isEmpty()) {
            return null;
        }

        // 第三步：构造每个帖子的片段缓存 Key
        // Key 格式：feed:item:{id}，示例：feed:item:12345
        // 二级结构优势：更新单个帖子不影响分页列表；同一帖子被不同分页共享
        List<String> itemKeys = new ArrayList<>(idList.size());
        for (String id : idList) {
            itemKeys.add("feed:item:" + id);
        }
        // 批量获取知文 元数据
        // 将多次网络 IO 合并为一次，20 个 Key 从 ~20ms 降至 ~1ms
        List<String> itemJsons = redis.opsForValue().multiGet(itemKeys);
        // 全量校验：任何一个片段缺失或解析失败，返回 null 触发回源
        List<FeedItemResponse> items = new ArrayList<>(idList.size());

        for (int i = 0; i < idList.size(); i++) {
            String itemJson = (itemJsons != null && i < itemJsons.size()) ? itemJsons.get(i) : null;
            if (itemJson == null) {
                // 缺失元数据片段，触发回源
                return null;
            }

            try {
                items.add(objectMapper.readValue(itemJson, FeedItemResponse.class));
            } catch (Exception e) {
                // JSON 解析失败：数据损坏或格式不兼容，触发回源重新生成
                return null;
            }
        }
        // 第六步：为每个帖子实时填充计数和用户状态
        // 为什么不在片段缓存中存储？
        // - 计数变化频繁，缓存会导致数据滞后
        // - liked/faved 是用户维度的，缓存会泄露用户隐私
        List<FeedItemResponse> enriched = new ArrayList<>(idList.size());
        for (int i = 0; i < idList.size(); i++) {
            FeedItemResponse base = items.get(i);
            if (base == null) {
                continue;
            }
            // 6.1 查询公共计数：点赞数和收藏数
            Map<String, Long> counts = counterService.getCounts("knowpost", String.valueOf(base.id()), List.of("like", "fav"));
            Long likeCount = counts.getOrDefault("like", 0L);
            Long favoriteCount = counts.getOrDefault("fav", 0L);

            // 用户维度状态实时计算，不落入片段缓存以避免用户数据污染
            // 6.2 查询用户私人状态：是否点赞、是否收藏
            // 关键设计：uid 为 null 时 liked/faved 均为 false
            // 这正是 getPublicFeed 传入 null 的原因——生成"绝对干净"的缓存数据
            boolean liked = uid != null && counterService.isLiked("knowpost", base.id(), uid);
            boolean faved = uid != null && counterService.isFaved("knowpost", base.id(), uid);
            // 6.3 组装完整的 FeedItemResponse：片段缓存基础信息 + 实时计数 + 用户状态
            enriched.add(new FeedItemResponse(
                    base.id(),
                    base.title(),
                    base.description(),
                    base.coverImage(),
                    base.tags(),
                    base.authorAvatar(),
                    base.authorNickname(),
                    base.tagJson(),
                    likeCount,
                    favoriteCount,
                    liked,
                    faved,
                    base.isTop())
            );
        }
        // hasMore 优先使用软缓存值；若缺失，则以“满页”作为兜底判断
        // 第七步：判断是否有下一页
        // 优先使用缓存中的 hasMore 标记；若缺失则以"满页"作为兜底判断
        // 满页兜底是乐观估计：多显示一个"加载更多"好于漏掉内容
        boolean hasMore = hasMoreStr != null ? "1".equals(hasMoreStr) : (idList.size() == size);

        return new FeedPageResponse(enriched, page, size, hasMore);
    }

    /**
     * 写入片段缓存与软缓存：
     * - idsKey：ID 列表（中 TTL）
     * - item：条目片段（中 TTL）
     * - hasMore：软缓存，满页时缓存 true 10~20s，否则 10s
     * 注意：不再写入 Redis 整页缓存 (pageKey)，避免双重存储。
     * @param pageKey 页面缓存 Key (用于反向索引引用)
     * @param idsKey ID 列表 Key
     * @param hasMoreKey 软缓存 Key
     * @param size 每页大小
     * @param rows 原始行数据
     * @param items 条目列表（计数已填充，liked/faved 为空）
     * @param hasMore 是否还有更多
     * @param frTtl 片段缓存 TTL
     */
    private void writeCaches(String pageKey, String idsKey, String hasMoreKey, int size, List<KnowPostFeedRow> rows, List<FeedItemResponse> items, boolean hasMore, Duration frTtl) {
        // 第一步：从数据库行提取帖子 ID，构建 ID 列表
        List<String> idVals = new ArrayList<>();

        for (KnowPostFeedRow r : rows) {
            idVals.add(String.valueOf(r.getId()));
        }
        // 第二步：写入 ID 列表到 Redis List
        if (!idVals.isEmpty()) {
            // 使用 LPUSH 将所有 ID 一次性写入 Redis List
            redis.opsForList().leftPushAll(idsKey, idVals);
            redis.expire(idsKey, frTtl);
            // 写入 hasMore 标记到缓存（软缓存，TTL 很短）
            // 特殊策略：满页且有下一页时，TTL = 10~20 秒（带随机抖动）
            // 其他情况：TTL = 固定 10 秒
            // 为什么 hasMore 使用很短的 TTL？
            // - 数据不敏感：即使 hasMore 失效，idList 满页兜底也能做出正确判断
            // - 数据可变：新帖子发布后 hasMore 可能从 false 变为 true
            // - 短 TTL 保证及时刷新，避免用户看到"已无更多"的实际还有内容
            if (idVals.size() == size && hasMore) {
                redis.opsForValue().set(hasMoreKey, "1", Duration.ofSeconds(10 + ThreadLocalRandom.current().nextInt(11)));
            } else {
                redis.opsForValue().set(hasMoreKey, hasMore ? "1" : "0", Duration.ofSeconds(10));
            }
        }

        // 第三步：将当前页面 Key 添加到全局页面集合索引
        // 用途：按页面维度批量失效与清理缓存
        // 即使不再写入 Redis 整页缓存，仍保留此索引用于：
        // 1. 本地缓存失效通知  2. 缓存清理  3. 全局 Feed 分页记录
        redis.opsForSet().add("feed:public:pages", pageKey);

        for (FeedItemResponse it : items) {
            // 反向索引：按小时为每个内容建立“页面引用关系”，支持内容更新时快速定位受影响页面
            // 构建反向索引：记录哪些页面包含了当前帖子
            // 用途：当帖子内容更新（标题、封面等）时，通过反向索引 已知帖子 → 查询这个帖子出现在哪些页面。
            //      快速定位出哪些分页缓存包含了这个帖子，然后批量失效。
            // 时间分片：按小时分片（hourSlot = 当前毫秒 / 3600000）
            // 一个小时内所有页面索引放在同一个 Set 中，自动过期清理。
            long hourSlot = System.currentTimeMillis() / 3600000L;
            String idxKey = "feed:public:index:" + it.id() + ":" + hourSlot;
            redis.opsForSet().add(idxKey, pageKey);
            redis.expire(idxKey, frTtl);
            // 写入帖子片段缓存,将帖子摘要序列化为 JSON 并写入 Redis
            // 存储干净的基础信息（不含用户私人状态），供 assembleFromCache 读取
            // 异常忽略：写入失败不影响主流程，下次回源会重试
            try {
                String itemKey = "feed:item:" + it.id();
                String itemJson = objectMapper.writeValueAsString(it);
                redis.opsForValue().set(itemKey, itemJson, frTtl);
            } catch (Exception ignored) {}
        }
    }

    /**
     * 生成“我的发布”列表的缓存 Key（用户维度）。
     * @param userId 用户 ID
     * @param page 页码
     * @param size 每页大小
     * @return Redis 页面缓存 Key
     */
    private String myCacheKey(long userId, int page, int size) {
        return "feed:mine:" + userId + ":" + size + ":" + page;
    }

    /**
     * 获取当前用户自己发布的知文列表（按发布时间倒序，包含置顶信息）。
     * <p>
     * 与 {@code getPublicFeed} 的核心区别：
     * <ul>
     *   <li><b>用户维度</b>：每个用户看到的列表完全不同，缓存 Key 必须包含 userId</li>
     *   <li><b>缓存结构简化</b>：直接缓存整页 JSON，不需要"ID列表+片段"的二级结构，
     *       因为用户维度数据无法跨用户共享</li>
     *   <li><b>TTL 更短</b>：基础 TTL 30 秒（+抖动 0~19 秒），
     *       因为用户对自己发布内容的更新感知更敏感</li>
     *   <li><b>无需 SingleFlight</b>：用户维度请求量远小于公开 Feed，回源压力可控</li>
     *   <li><b>直接填充用户状态</b>：查询时直接传入 userId 到 mapRowsToItems，
     *       无需像公开 Feed 那样先缓存干净数据再 enrich</li>
     * </ul>
     * <p>
     * 缓存策略：L1 Caffeine 本地缓存 + L2 Redis 整页 JSON 缓存。
     *
     * @param userId 当前用户 ID
     * @param page   页码，从 1 开始，小于 1 时自动修正为 1
     * @param size   每页数量，范围 1~50，超出范围自动修正
     * @return 带分页信息的个人发布列表，包含 isTop 字段表示是否置顶
     */
    public FeedPageResponse getMyPublished(long userId, int page, int size) {

        // 第一步：参数安全校验
        // 限制 size 在 1~50 之间，防止恶意超大分页拖垮数据库
        int safeSize = Math.min(Math.max(size, 1), 50);
        // 页码最小为 1，防止负数导致 SQL OFFSET 异常
        int safePage = Math.max(page, 1);
        // 用户维度缓存 Key：包含 userId，每个用户独立缓存
        // 格式示例：feed:mine:{userId}:{size}:{page}
        String key = myCacheKey(userId, safePage, safeSize);

        // 第二步：L1 本地缓存（Caffeine）
        // "我的发布"是用户维度的，缓存在本地比公开 Feed 更合适，
        // 因为同一用户短时间内多次查看自己发布列表的概率更高
        FeedPageResponse local = feedMineCache.getIfPresent(key);
        if (local != null) {
            // 记录热度，热点用户后续访问时 TTL 会被动态延长
            hotKey.record(key);
            maybeExtendTtlMine(key);
            log.info("feed.mine source=local key={} page={} size={} user={}", key, safePage, safeSize, userId);
            // 直接返回，无需 enrich：查询时已填充了用户维度的 liked/faved
            return local;
        }

        // 第三步：L2 Redis 缓存
        // 与公开 Feed 不同，这里缓存整页 JSON（用户维度数据无法跨用户共享 ID列表）
        String cached = redis.opsForValue().get(key);
        if (cached != null) {
            try {
                FeedPageResponse cachedResp = objectMapper.readValue(cached, FeedPageResponse.class);
                // 校验缓存完整性：所有条目的计数字段必须存在
                // 缺失说明是老版本缓存或数据损坏，需要回源
                boolean hasCounts = cachedResp.items() != null && cachedResp.items().stream()
                        .allMatch(it -> it.likeCount() != null && it.favoriteCount() != null);
                if (hasCounts) {
                    // 缓存有效：回填 L1、记录热度、延长 TTL
                    feedMineCache.put(key, cachedResp);
                    hotKey.record(key);
                    maybeExtendTtlMine(key);
                    log.info("feed.mine source=page key={} page={} size={} user={}", key, safePage, safeSize, userId);
                // enrich 会为当前用户实时查询点赞/收藏状态
                // 即使缓存中数据稍旧，用户.也能看到最新状态
                List<FeedItemResponse> enriched = enrich(cachedResp.items(), userId);
                return new FeedPageResponse(enriched, cachedResp.page(), cachedResp.size(), cachedResp.hasMore());
            }
            } catch (Exception ignored) {
                // JSON 解析失败：兼容历史数据格式变化，静默回源
            }
        }

        // 第四步：数据库回源
        int offset = (safePage - 1) * safeSize;
        // 多查一条（size+1），用于判断 hasMore，避免额外 COUNT 查询
        List<KnowPostFeedRow> rows = mapper.listMyPublished(userId, safeSize + 1, offset);
        boolean hasMore = rows.size() > safeSize;
        // 截掉多查的那一条，只返回 safeSize 条
        if (hasMore) rows = rows.subList(0, safeSize);

        // 将数据库行映射为响应条目，直接传入 userId 填充 liked/faved
        List<FeedItemResponse> items = mapRowsToItems(rows, userId, true);

        FeedPageResponse resp = new FeedPageResponse(items, safePage, safeSize, hasMore);


        // 第五步：回写两级缓存
        try {
            String json = objectMapper.writeValueAsString(resp);
            // 用户维度缓存 TTL 更短（基础 30 秒 + 抖动 0~19 秒）
            // 理由：用户对自己发布的内容更新感知更敏感
            int baseTtl = 30;
            int jitter = ThreadLocalRandom.current().nextInt(20);
            // 写入 L2 Redis
            redis.opsForValue().set(key, json, Duration.ofSeconds(baseTtl + jitter));
            // 回填 L1 本地缓存
            feedMineCache.put(key, resp);
            // 记录热度
            hotKey.record(key);
        } catch (Exception ignored) {
            // 序列化或 Redis 写入失败：静默忽略，不影响返回结果
        }
        log.info("feed.mine source=db key={} page={} size={} user={} hasMore={}", key, safePage, safeSize, userId, hasMore);
        return resp;
    }

    /**
     * 解析 JSON 数组字符串为 List<String>。
     * @param json JSON 数组字符串
     * @return 字符串列表；解析失败或空字符串返回空列表
     */
    private List<String> parseStringArray(String json) {
        if (json == null || json.isBlank()) return Collections.emptyList();
        try {
            return objectMapper.readValue(json, new TypeReference<>() {
            });
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    /**
     * 将数据库行映射为 FeedItemResponse 响应条目。
     * <p>
     * 处理流程：
     * <ol>
     *   <li>解析 JSON 字段（tags、imgUrls）为 Java 集合</li>
     *   <li>通过 CounterService 获取 likeCount 和 favoriteCount（Redis SDS 读取）</li>
     *   <li>按需查询当前用户的 liked/faved 状态（Redis 位图查询）</li>
     *   <li>可选字段：isTop 仅在"我的发布"列表返回，公开 Feed 传 null</li>
     * </ol>
     *
     * @param rows           数据库查询结果行
     * @param userIdNullable 当前用户 ID（可为 null，null 时不查询 liked/faved）
     * @param includeIsTop   是否在响应中包含 isTop 字段
     * @return 映射后的条目列表，顺序与输入一致
     */
    private List<FeedItemResponse> mapRowsToItems(List<KnowPostFeedRow> rows, Long userIdNullable, boolean includeIsTop) {
        // 预分配列表容量，避免扩容开销
        List<FeedItemResponse> items = new ArrayList<>(rows.size());

        for (KnowPostFeedRow r : rows) {
            // 解析 JSON 数组字段：tags（标签列表）、imgUrls（图片URL列表）
            List<String> tags = parseStringArray(r.getTags());
            List<String> imgs = parseStringArray(r.getImgUrls());
            // 取第一张图片作为封面，无图片时返回 null
            String cover = imgs.isEmpty() ? null : imgs.getFirst();

            // 通过 CounterService 获取点赞和收藏计数（Redis SDS 固定结构读取）
            Map<String, Long> counts = counterService.getCounts("knowpost", String.valueOf(r.getId()), List.of("like", "fav"));
            Long likeCount = counts.getOrDefault("like", 0L);
            Long favoriteCount = counts.getOrDefault("fav", 0L);

            // 查询当前用户是否已点赞/已收藏（Redis 位图 GETBIT 查询）
            // 利用短路求值：userIdNullable 为 null 时不发起 Redis 查询
            Boolean liked = userIdNullable != null && counterService.isLiked("knowpost", String.valueOf(r.getId()), userIdNullable);
            Boolean faved = userIdNullable != null && counterService.isFaved("knowpost", String.valueOf(r.getId()), userIdNullable);
            // isTop 仅在"我的发布"场景返回（includeIsTop=true），公开 Feed 传 null 表示不展示
            Boolean isTop = includeIsTop ? r.getIsTop() : null;

            items.add(new FeedItemResponse(
                    String.valueOf(r.getId()),   // 帖子 ID
                    r.getTitle(),                // 标题
                    r.getDescription(),          // 描述
                    cover,                       // 封面图片 URL
                    tags,                        // 标签列表
                    r.getAuthorAvatar(),         // 作者头像
                    r.getAuthorNickname(),       // 作者昵称
                    r.getAuthorTagJson(),        // 作者标签 JSON
                    likeCount,                   // 点赞数
                    favoriteCount,               // 收藏数
                    liked,                       // 当前用户是否已点赞
                    faved,                       // 当前用户是否已收藏
                    isTop                        // 是否置顶（仅"我的发布"有效）
            ));
        }
        return items;
    }



    /**
     * 根据热点级别动态延长“我的发布”页面缓存 TTL。
     * @param key 页面缓存 Key
     */
    private void maybeExtendTtlMine (String key) {
        int baseTtl = 30;
        int target = hotKey.ttlForMine(baseTtl, key);
        Long currentTtl = redis.getExpire(key);
        if (currentTtl < target) {
            redis.expire(key, Duration.ofSeconds(target));
        }
    }
}

///**
//     * 获取公开的首页 Feed（按发布时间倒序，不受置顶影响）。
//     * 采用三级缓存：本地 Caffeine、Redis 页面缓存、Redis 片段缓存（ids/item/count）。
//     * @param page 页码（≥1）
//     * @param size 每页数量（1~50）
//     * @param currentUserIdNullable 当前用户 ID（为空表示匿名）
//     * @return 带分页信息的 Feed 列表（liked/faved 为用户维度）
//     */
//    public FeedPageResponse getPublicFeed(int page, int size, Long currentUserIdNullable) {
//        int safeSize = Math.min(Math.max(size, 1), 50);
//        int safePage = Math.max(page, 1);
//        // 这个 localPageKey 是本地缓存的页面 Key（非 Redis）
//        String localPageKey = cacheKey(safePage, safeSize);
//
//        // 按小时分片的片段缓存键：降低跨小时内容更新导致的大面积失效风险
//        // 将分页维度（size/page）与时间维度（hourSlot）组合，避免热门页在整站失效时同时回源
//        long hourSlot = System.currentTimeMillis() / 3600000L;
//        String idsKey = "feed:public:ids:" + safeSize + ":" + hourSlot + ":" + safePage;
//        String hasMoreKey = "feed:public:ids:" + safeSize + ":" + hourSlot + ":" + safePage + ":hasMore";
//
//        // L1: 先从本地缓存拿数据，高并发时抗 80% 流量
//        FeedPageResponse local = feedPublicCache.getIfPresent(localPageKey);
//
//        if (local != null && local.items() != null) {
//            // 对返回列表中的每个条目进行热度统计
//            for (FeedItemResponse item : local.items()) {
//                recordItemHotKey(item.id());
//            }
//
//            log.info("feed.public source=local localPageKey={} page={} size={}", localPageKey, safePage, safeSize);
//            List<FeedItemResponse> enrichedLocal = enrich(local.items(), currentUserIdNullable);
//
//            return new FeedPageResponse(enrichedLocal, local.page(), local.size(), local.hasMore());
//        }
//
//        // L2: 二级缓存，Redis 片段缓存，组装
//        FeedPageResponse fromCache = assembleFromCache(idsKey, hasMoreKey, safePage, safeSize, currentUserIdNullable);
//        if (fromCache != null) {
//            feedPublicCache.put(localPageKey, fromCache);
//            // 对返回列表中的每个条目进行热度统计
//            if (fromCache.items() != null) {
//                for (FeedItemResponse item : fromCache.items()) {
//                    recordItemHotKey(item.id());
//                }
//            }
//            log.info("feed.public source=3tier localPageKey={} page={} size={}", localPageKey, safePage, safeSize);
//            return fromCache;
//        }
//
//        // 当上述两级缓存都没有数据，说明需要回源查数据库
//        // 为了防止高并发下（例如 1000 个请求同时访问同一页）
//        // 所有请求同时打到数据库（造成 缓存击穿 ），这里使用了锁
//        // 单航班机制：以 idsKey 作为“航班号”
//        // 并发下同一页只允许一个请求回源数据库，其余在锁内优先重查缓存，避免击穿惊群
//        Object lock = singleFlight.computeIfAbsent(idsKey, k -> new Object());
//        synchronized (lock) {
//            // 重查 L2 缓存，避免重复回源
//            FeedPageResponse again = assembleFromCache(idsKey, hasMoreKey, safePage, safeSize, currentUserIdNullable);
//            if (again != null) {
//                feedPublicCache.put(localPageKey, again);
//                // 对返回列表中的每个条目进行热度统计
//                if (again.items() != null) {
//                    for (FeedItemResponse item : again.items()) {
//                        recordItemHotKey(item.id());
//                    }
//                }
//                log.info("feed.public source=3tier(after-flight) localPageKey={} page={} size={}", localPageKey, safePage, safeSize);
//                singleFlight.remove(idsKey);
//                return again;
//            }
//
//            // 数据库回源：读取 size+1 以判断是否有下一页，后裁剪为当前页
//            int offset = (safePage - 1) * safeSize;
//            List<KnowPostFeedRow> rows = mapper.listFeedPublic(safeSize + 1, offset);
//            boolean hasMore = rows.size() > safeSize;
//            if (hasMore) {
//                rows = rows.subList(0, safeSize);
//            }
//
//            // 构建基础列表（计数已填充），liked/faved 置为 null 以免污染用户维度缓存
//            List<FeedItemResponse> items = mapRowsToItems(rows, null, false);
//
//            FeedPageResponse respForCache = new FeedPageResponse(items, safePage, safeSize, hasMore);
//            // 片段缓存（ids/item/count）TTL 更长并加入随机抖动，降低同一时刻大量过期
//            int baseTtl = 60;
//            int jitter = ThreadLocalRandom.current().nextInt(30);
//            Duration frTtl = Duration.ofSeconds(baseTtl + jitter);
//
//            // 写入片段缓存与本地缓存
//            writeCaches(localPageKey, idsKey, hasMoreKey, safeSize, rows, items, hasMore, frTtl);
//            feedPublicCache.put(localPageKey, respForCache);
//
//            // 返回时覆盖用户维度状态，不写回缓存
//            List<FeedItemResponse> enriched = enrich(items, currentUserIdNullable);
//            log.info("feed.public source=db localPageKey={} page={} size={} hasMore={}", localPageKey, safePage, safeSize, hasMore);
//            // 释放单航班锁，允许后续请求正常进入
//            singleFlight.remove(idsKey);
//
//            return new FeedPageResponse(enriched, safePage, safeSize, hasMore);
//        }
//    }    /**
//     * 生成公共 Feed 页面的缓存 Key（包含分页与布局版本）。
//     * @param page 页码（1 起）
//     * @param size 每页大小
//     * @return Redis/Page 缓存的 Key
//     */
//    private String cacheKey(int page, int size) {
//        return "feed:public:" + size + ":" + page + ":v" + LAYOUT_VER;
//    }