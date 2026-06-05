package com.tongji.knowpost.service.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tongji.counter.service.UserCounterService;
import com.tongji.knowpost.service.KnowPostService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tongji.common.exception.BusinessException;
import com.tongji.common.exception.ErrorCode;
import com.tongji.knowpost.id.SnowflakeIdGenerator;
import com.tongji.knowpost.mapper.KnowPostMapper;
import com.tongji.knowpost.model.KnowPost;
import com.tongji.knowpost.model.KnowPostDetailRow;
import com.tongji.knowpost.api.dto.KnowPostDetailResponse;
import com.github.benmanes.caffeine.cache.Cache;
import com.tongji.counter.service.CounterService;
import com.tongji.storage.config.OssProperties;
import com.tongji.llm.rag.RagIndexService;
import com.tongji.relation.mapper.OutboxMapper;
import com.tongji.cache.hotkey.HotKeyDetector;
import com.tongji.search.index.SearchIndexService;
import jakarta.annotation.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class KnowPostServiceImpl implements KnowPostService {

    private final KnowPostMapper mapper;
    private final SnowflakeIdGenerator idGen;
    private final ObjectMapper objectMapper;
    private final OssProperties ossProperties;
    private final CounterService counterService;
    private final UserCounterService userCounterService;
    private final StringRedisTemplate redis;

    // 什么是 @Qualifier？
    // 翻译过来叫“限定符”。在 Spring 仓库里，像 Mapper、Redis 这种东西往往只有一个。
    // 但是！Cache<String, ...>（本地缓存）这个东西，系统里可能配置了好多个（比如有存用户信息的缓存、有存帖子详情的缓存）。
    // Spring 的困惑：当 Spring 看到大管家要一个 Cache 时，它懵了：“仓库里有 5 个不同的 Cache，你要哪一个？”
    // 破局：所以程序员用 @Qualifier("knowPostDetailCache") 明确指着 Spring 的鼻子说：“我只要名字叫 knowPostDetailCache 的那一个，别拿错了！”
    @Qualifier("knowPostDetailCache")
    private final Cache<String, KnowPostDetailResponse> knowPostDetailCache;
    private final HotKeyDetector hotKey;
    private static final Logger log = LoggerFactory.getLogger(KnowPostServiceImpl.class);
    private static final int DETAIL_LAYOUT_VER = 1;
    private final ConcurrentHashMap<String, Object> singleFlight = new ConcurrentHashMap<>();
    private final RagIndexService ragIndexService;
    private final OutboxMapper outboxMapper;
    private final SearchIndexService searchIndexService;

    // 手动编写构造器，Spring的@Qualifier直接标注在参数上（核心）
    public KnowPostServiceImpl(
            KnowPostMapper mapper,
            SnowflakeIdGenerator idGen,
            ObjectMapper objectMapper,
            OssProperties ossProperties,
            CounterService counterService,
            UserCounterService userCounterService,
            StringRedisTemplate redis,
            @Qualifier("knowPostDetailCache") Cache<String, KnowPostDetailResponse> knowPostDetailCache,
            HotKeyDetector hotKey,
            RagIndexService ragIndexService,
            OutboxMapper outboxMapper,
            SearchIndexService searchIndexService
    ) {
        this.mapper = mapper;
        this.idGen = idGen;
        this.objectMapper = objectMapper;
        this.ossProperties = ossProperties;
        this.counterService = counterService;
        this.userCounterService = userCounterService;
        this.redis = redis;
        this.knowPostDetailCache = knowPostDetailCache; // 带@Qualifier的参数赋值
        this.hotKey = hotKey;
        this.ragIndexService = ragIndexService;
        this.outboxMapper = outboxMapper;
        this.searchIndexService = searchIndexService;
    }

    /**
     * 创建草稿并返回新 ID。
     * 草稿状态说明：
     * - status: "draft" 表示草稿状态，未公开发布
     * - type: "image_text" 表示图文类型，支持图片和文字内容
     * - visible: "public" 表示可见性为公开（草稿阶段不生效）
     * - isTop: false 表示不置顶
     */
    @Transactional
    public long createDraft(long creatorId) {
        // 雪花算法生成唯一ID
        // - 避免数据库自增ID的单点问题
        // - 支持分布式部署，多台机器同时生成ID
        // - ID包含时间信息，可以按时间排序
        // - 性能高，不需要访问数据库
        long id = idGen.nextId();
        Instant now = Instant.now();

        KnowPost post = KnowPost.builder()
                .id(id)                  // 设置帖子ID
                .creatorId(creatorId)    // 设置创建者ID
                .status("draft")         // 设置状态为草稿
                .type("image_text")      // 设置类型为图文
                .visible("public")       // 设置可见性为公开
                .isTop(false)            // 设置不置顶
                .createTime(now)         // 设置创建时间
                .updateTime(now)         // 设置更新时间
                .build();

        mapper.insertDraft(post);

        // 返回新创建的草稿ID
        // 调用者可以使用此ID进行后续操作：编辑草稿内容、上传图片、发布草稿、删除草稿
        return id;
    }

    /**
     * 确认内容上传（写入 objectKey、etag、大小、校验和，并生成公共 URL）。
     */
    @Transactional
    public void confirmContent(long creatorId, long id, String objectKey, String etag, Long size, String sha256) {
        // 缓存双删
        invalidateCache(id);

        KnowPost post = KnowPost.builder()
                .id(id)
                .creatorId(creatorId)
                .contentObjectKey(objectKey)
                .contentEtag(etag)
                .contentSize(size)
                .contentSha256(sha256)
                .contentUrl(publicUrl(objectKey))
                .updateTime(Instant.now())
                .build();

        // 防御性编程：直接在 UPDATE 语句中加上了条件（WHERE id = ? AND creator_id = ?）。
        // 如果更新行数为 0，说明要么帖子不存在，要么当前登录的人不是这篇帖子的作者（没权限）。
        int updated = mapper.updateContent(post);
        if (updated == 0) {
            throw new BusinessException(ErrorCode.BAD_REQUEST, "草稿不存在或无权限");
        }

        invalidateCache(id);

        try {
            // 触发一次预索引（草稿阶段可能因可见性/状态被跳过）
            // 因为大模型的向量索引服务可能发生网络超时，所以用 try-catch 包裹，不影响核心流程。
            // 【RAG】：Retrieval-Augmented Generation (检索增强生成)。这里是通知后台 AI 服务把这篇帖子切片存入向量数据库，以便将来 AI 能根据这篇帖子回答问题。
            ragIndexService.ensureIndexed(id);
        } catch (Exception e) {
            log.warn("Pre-index after content confirm failed, post {}: {}", id, e.getMessage());
        }
    }

    /**
     * 更新元数据：标题、标签、可见性、置顶、图片列表等。
     */
    @Transactional
    public void updateMetadata(long creatorId, long id, String title, Long tagId, List<String> tags, List<String> imgUrls, String visible, Boolean isTop, String description) {
        invalidateCache(id);

        KnowPost post = KnowPost.builder()
                .id(id)
                .creatorId(creatorId)
                .title(title)
                .tagId(tagId)
                .tags(toJsonOrNull(tags))
                .imgUrls(toJsonOrNull(imgUrls))
                .visible(visible)
                .isTop(isTop)
                .description(description)
                .type("image_text")
                .updateTime(Instant.now())
                .build();

        int updated = mapper.updateMetadata(post);

        if (updated == 0) {
            throw new BusinessException(ErrorCode.BAD_REQUEST, "草稿不存在或无权限");
        }

        try {
            // 元数据变更后写入 Outbox 事件，驱动搜索索引更新
            // 【发件箱模式 (Outbox)】：把发送消息的动作当成一条普通的数据库记录，和上面的 updateMetadata 处于同一个事务。
            // 这样只要数据库更新成功，事件就一定不会丢。后续会有 Canal 监听底层表数据变更，扔到 Kafka 去异步更新 Elasticsearch 搜索引擎。
            long outId = idGen.nextId();
            String payload = objectMapper.writeValueAsString(Map.of("entity", "knowpost", "op", "upsert", "id", id));
            outboxMapper.insert(outId, "knowpost", id, "KnowPostMetadataUpdated", payload);
        } catch (Exception e) {
            log.warn("Outbox event after metadata update failed, post {}: {}", id, e.getMessage());
        }

        invalidateCache(id);
    }

    /**
     * 发布草稿，设置状态与发布时间。
     */
    @Transactional
    public void publish(long creatorId, long id) {
        int updated = mapper.publish(id, creatorId);

        if (updated == 0) {
            throw new BusinessException(ErrorCode.BAD_REQUEST, "草稿不存在或无权限");
        }
        try {
            userCounterService.incrementPosts(creatorId, 1);
            // 就算 Redis 挂了、计数失败了，也不影响帖子发布成功的核心结果。
        } catch (Exception ignored) {}

        // 写入 Outbox 事件，驱动搜索索引增量更新
        try {
            long outId = idGen.nextId();
            String payload = objectMapper.writeValueAsString(Map.of("entity", "knowpost", "op", "upsert", "id", id));
            outboxMapper.insert(outId, "knowpost", id, "KnowPostPublished", payload);
        } catch (Exception e) {
            log.warn("Outbox event after publish failed, post {}: {}", id, e.getMessage());
        }

        // 发布成功后触发一次预索引，减少首次问答冷启动
        try {
            ragIndexService.ensureIndexed(id);
        } catch (Exception e) {
            log.warn("Pre-index after publish failed, post {}: {}", id, e.getMessage());
        }

        // 直接写入 ES 搜索索引，确保发布后立即可搜（无需依赖 Canal→Kafka 管道）
        try {
            searchIndexService.upsertKnowPost(id);
        } catch (Exception e) {
            log.warn("Direct ES index after publish failed, post {}: {}", id, e.getMessage());
        }
    }

    /**
     * 设置置顶。
     */
    @Transactional
    public void updateTop(long creatorId, long id, boolean isTop) {
        invalidateCache(id);

        int updated = mapper.updateTop(id, creatorId, isTop);

        if (updated == 0) {
            throw new BusinessException(ErrorCode.BAD_REQUEST, "草稿不存在或无权限");
        }

        invalidateCache(id);
    }

    /**
     * 设置可见性（权限）。
     */
    @Transactional
    public void updateVisibility(long creatorId, long id, String visible) {
        if (!isValidVisible(visible)) {
            throw new BusinessException(ErrorCode.BAD_REQUEST, "可见性取值非法");
        }

        invalidateCache(id);

        int updated = mapper.updateVisibility(id, creatorId, visible);

        if (updated == 0) {
            throw new BusinessException(ErrorCode.BAD_REQUEST, "草稿不存在或无权限");
        }

        invalidateCache(id);
    }

    /**
     * 软删除。
     */
    @Transactional
    public void delete(long creatorId, long id) {
        invalidateCache(id);

        int updated = mapper.softDelete(id, creatorId);
        if (updated == 0) {
            throw new BusinessException(ErrorCode.BAD_REQUEST, "草稿不存在或无权限");
        }

        // 写入 Outbox 事件，驱动搜索索引软删
        try {
            long outId = idGen.nextId();
            String payload = objectMapper.writeValueAsString(Map.of("entity", "knowpost", "op", "delete", "id", id));
            outboxMapper.insert(outId, "knowpost", id, "KnowPostDeleted", payload);
        } catch (Exception e) {
            log.warn("Outbox event after delete failed, post {}: {}", id, e.getMessage());
        }

        invalidateCache(id);
    }

    private boolean isValidVisible(String visible) {
        if (visible == null) {
            return false;
        }

        return switch (visible) {
            case "public", "followers", "school", "private", "unlisted" -> true;
            default -> false;
        };
    }

    private String toJsonOrNull(List<String> list) {
        if (list == null) {
            return null;
        }

        try {
            return objectMapper.writeValueAsString(list);
        } catch (JsonProcessingException e) {
            throw new BusinessException(ErrorCode.BAD_REQUEST, "JSON 处理失败");
        }
    }

    private String publicUrl(String objectKey) {
        String publicDomain = ossProperties.getPublicDomain();

        if (publicDomain != null && !publicDomain.isBlank()) {
            return publicDomain.replaceAll("/$", "") + "/" + objectKey;
        }

        return "https://" + ossProperties.getBucket() + "." + ossProperties.getEndpoint() + "/" + objectKey;
    }

    /**
     * 获取知文详情（含作者信息、图片列表）。
     * <p>
     * 流程：
     * 1. 尝试读取 Redis 缓存。
     * 2. 若缓存命中，直接返回（需叠加实时计数与用户状态）。
     * 3. 若缓存未命中，使用 SingleFlight 锁机制防止缓存击穿。
     * 4. 锁内再次检查缓存（双重检查）。
     * 5. 若仍未命中，回源查询数据库。
     * 6. 校验内容状态与访问权限。
     * 7. 组装数据并写入 Redis 缓存（带随机过期时间与热点自动延期）。
     * 8. 返回最终结果（叠加用户维度状态）。
     * </p>
     *
     * @param id 要查询的知文 ID
     * @param currentUserIdNullable 当前用户 ID（可空，用于判断权限与点赞状态）
     * @return 知文详情响应,包含帖子的完整信息
     */
    @Transactional(readOnly = true)
    public KnowPostDetailResponse getDetail(long id, Long currentUserIdNullable) {
        // 1. 构造缓存 Key：knowpost:detail:{id}:v{version}
        String pageKey = "knowpost:detail:" + id + ":v" + DETAIL_LAYOUT_VER;

        // 0. L1 本地缓存（Caffeine）查询本地缓存
        // L1缓存命中，记录热点key并延长TTL
        // 热点检测：频繁访问的内容被认为是热点
        // TTL延长：热点内容的缓存时间更长
        KnowPostDetailResponse local = knowPostDetailCache.getIfPresent(pageKey);
        if (local != null) {
            // 🚨 缓存命中后必须先鉴权 (防止本地缓存带来的越权脏读)
            if (!isAuthorized(local, currentUserIdNullable)) {
                throw new BusinessException(ErrorCode.BAD_REQUEST, "无权限查看");
            }
            recordHotKeyAndExtendTtl(id, pageKey);
            log.info("detail source=local key={}", pageKey);
            return enrichDetailResponse(local, currentUserIdNullable, true);
        }

        // 2. 第一次尝试处理 L2 缓存(Redis) 命中
        String cached = redis.opsForValue().get(pageKey);
        KnowPostDetailResponse resp = tryProcessCacheHit(cached, id, pageKey, currentUserIdNullable, "page");
        if (resp != null) {
            return resp; // 说明其他服务器之前处理过这个帖子且鉴权通过
        }

        // 3. 缓存未命中，进入 SingleFlight 模式
        // 对同一个 pageKey 加锁，防止高并发下大量请求同时打到数据库（缓存击穿/惊群效应）
        Object lock = singleFlight.computeIfAbsent(pageKey, k -> new Object());
        synchronized (lock) {
            try {
                // 💡 修复1：使用 try-finally 保证锁绝对会被释放

                // 4. 双重检查（Double Check）
                String again = redis.opsForValue().get(pageKey);
                try {
                    resp = tryProcessCacheHit(again, id, pageKey, currentUserIdNullable, "page(after-flight)");
                } catch (BusinessException e) {
                    throw e;
                }
                if (resp != null) {
                    return resp;
                }

                // 5. 数据库回源查询
                KnowPostDetailRow row = mapper.findDetailById(id);

                // 6. 处理内容不存在或已删除
                if (row == null || "deleted".equals(row.getStatus())) {
                    redis.opsForValue().set(pageKey, "NULL", java.time.Duration.ofSeconds(30 + java.util.concurrent.ThreadLocalRandom.current().nextInt(31)));
                    throw new BusinessException(ErrorCode.BAD_REQUEST, "内容不存在");
                }

                // 💡 修复2：先组装数据，绝不在写入公共缓存完成前做抛出异常的鉴权！
                List<String> images = parseStringArray(row.getImgUrls());
                List<String> tags = parseStringArray(row.getTags());
                Map<String, Long> counts = counterService.getCounts("knowpost", String.valueOf(row.getId()), List.of("like", "fav"));

                resp = new KnowPostDetailResponse(
                        String.valueOf(row.getId()), row.getTitle(), row.getDescription(), row.getContentUrl(),
                        images, tags, String.valueOf(row.getCreatorId()), row.getAuthorAvatar(), row.getAuthorNickname(),
                        row.getAuthorTagJson(), counts.getOrDefault("like", 0L), counts.getOrDefault("fav", 0L),
                        null, null, row.getIsTop(), row.getVisible(), row.getType(), row.getPublishTime()
                );

                // 判断内容是否为公开状态
                boolean isPublic = "published".equals(row.getStatus()) && "public".equals(row.getVisible());

                // 💡 修复3：只有公开内容才写入 Redis 和 L1 缓存
                // 这样可以避免非公开内容进入全局缓存浪费内存，更能从根源杜绝越权泄露
                if (isPublic) {
                    try {
                        String json = objectMapper.writeValueAsString(resp);
                        int baseTtl = 60;
                        int target = hotKey.ttlForPublic(baseTtl, pageKey);
                        redis.opsForValue().set(pageKey, json, Duration.ofSeconds(Math.max(target, baseTtl + ThreadLocalRandom.current().nextInt(30))));
                        knowPostDetailCache.put(pageKey, resp);
                        log.info("detail source=db key={}", pageKey);
                    } catch (Exception ignored) {}
                }

                // 💡 修复4：权限校验（此时公共缓存写入完毕，在返回给用户前最后一步执行）
                boolean isOwner = currentUserIdNullable != null && row.getCreatorId() != null && currentUserIdNullable.equals(row.getCreatorId());
                if (!isPublic && !isOwner) {
                    throw new BusinessException(ErrorCode.BAD_REQUEST, "无权限查看");
                }

                return enrichDetailResponse(resp, currentUserIdNullable, false);

            } finally {
                // 💡 修复1(续)：无论中间发生任何异常（数据库断连、网络报错），锁内存都会被安全清理！
                singleFlight.remove(pageKey);
            }
        }
    }

    /**
     * 尝试处理缓存命中逻辑
     *
     * 该方法是缓存查询的核心处理逻辑，负责处理从 Redis 获取的缓存数据。
     * 实现了多层缓存架构和缓存穿透防护机制，确保缓存数据的正确性和实时性。
     *
     * 处理流程：
     * 1. 检查缓存是否为空（未命中）
     * 2. 检查是否命中空值缓存（防止缓存穿透）
     * 3. 反序列化缓存数据并填充 L1 缓存
     * 4. 记录内容热度并动态调整缓存 TTL
     * 5. 叠加实时数据（计数和用户状态）后返回
     *
     * @param cached 从 Redis L2 缓存中读取的缓存字符串，可能为 null
     * @param id 内容的唯一标识符，用于热度记录和缓存键生成
     * @param pageKey 页面缓存的唯一键，用于 L1 缓存填充和日志记录
     * @param uid 当前请求用户的 ID，用于查询个性化状态；如果为 null 则表示未登录用户
     * @param sourceLog 日志来源标识，用于追踪数据来源（如 "L1"、"L2"、"DB"）
     * @return 若成功处理缓存命中则返回叠加了实时数据的响应对象；若缓存未命中或处理失败则返回 null
     * @throws BusinessException 当命中空值缓存时抛出，表示内容不存在
     */
    private KnowPostDetailResponse tryProcessCacheHit(String cached, long id, String pageKey, Long uid, String sourceLog) {
        // 第一步：检查缓存是否为空
        // 如果 cached 为 null，表示 Redis 中不存在该键，属于缓存未命中情况
        // 返回 null 会让调用方进行数据库回源查询
        if (cached == null) {
            return null;
        }

        // 第二步：检查是否命中空值缓存（缓存穿透防护）
        // 使用 "NULL" 字符串作为空值缓存的标识，防止恶意查询不存在的 ID 导致频繁访问数据库
        if ("NULL".equals(cached)) {
            throw new BusinessException(ErrorCode.BAD_REQUEST, "内容不存在");
        }

        try {
            // 第三步：反序列化缓存数据
            // 将 JSON 字符串反序列化为 KnowPostDetailResponse 对象
            KnowPostDetailResponse base = objectMapper.readValue(cached, KnowPostDetailResponse.class);

            // 🚨 缓存命中后必须先鉴权
            // 在使用缓存数据前，必须验证当前用户是否有权查看该内容
            // 鉴权规则：公开内容所有人可查看，非公开内容仅作者本人可查看
            if (!isAuthorized(base, uid)) {
                throw new BusinessException(ErrorCode.BAD_REQUEST, "无权限查看");
            }

            // 填充 L1 本地缓存（Caffeine）
            // 将从 Redis 获取的数据填充到本地缓存中，减少后续对 Redis 的访问
            knowPostDetailCache.put(pageKey, base);

            // 第四步：记录内容热度并尝试续期
            // 根据内容的访问频率动态调整缓存 TTL
            recordHotKeyAndExtendTtl(id, pageKey);

            // 记录数据来源日志，便于监控和问题排查
            log.info("detail source={} key={}", sourceLog, pageKey);

            // 第五步：叠加实时数据并返回
            // 调用 enrichDetailResponse 方法叠加两类实时数据：
            // 1. 实时计数（点赞数、收藏数）：从 CounterService 获取最新数据
            // 2. 用户个性化状态（是否已点赞/收藏）：根据当前用户 ID 实时查询
            return enrichDetailResponse(base, uid, true);

        } catch (BusinessException e) {
            // 💡 架构师安全修复：如果是我们自己主动抛出的业务异常（如无权限），必须继续往外抛！
            // 绝对不能让它被当做"缓存未命中"去查数据库，防止恶意攻击利用异常生吞机制抽干数据库连接池。
            throw e;
        } catch (Exception e) {
            // 💡 只有遇到了 JSON 反序列化失败等真正的"系统意外错误"，
            // 才认为缓存坏了，返回 null 触发数据库回源去修复脏缓存。
            log.warn("缓存数据解析失败，强制回源 key: {}", pageKey, e);
            return null;
        }
    }

    /**
     * 丰富详情响应：叠加实时计数与用户状态
     *
     * 该方法用于在基础响应对象上叠加实时数据和用户个性化状态，确保返回给前端的数据是最新的。
     * 主要处理两类数据：
     * 1. 实时计数数据（点赞数、收藏数）：从 CounterService 获取权威数据
     * 2. 用户个性化状态（是否已点赞/收藏）：根据当前用户ID实时查询
     *
     * @param base 基础响应对象，可能来自缓存或数据库查询结果
     * @param uid 当前请求用户的ID，用于查询个性化状态；如果为null则表示未登录用户
     * @param refreshCounts 是否需要从 CounterService 刷新计数标志位
     * - true：从缓存获取数据时需要刷新，因为缓存中的计数可能过期
     * - false：从数据库回源时不需要刷新，因为数据库数据已经是最新的
     * @return 叠加了最新计数和用户状态的完整响应对象
     */
    private KnowPostDetailResponse enrichDetailResponse(KnowPostDetailResponse base, Long uid, boolean refreshCounts) {
        Long likeCount = base.likeCount();
        Long favoriteCount = base.favoriteCount();

        // 第一步：刷新实时计数（仅在缓存命中时执行）
        // 只有从缓存获取数据时才需要刷新，从数据库回源时数据本身就是最新的
        if (refreshCounts) {
            Map<String, Long> counts = counterService.getCounts("knowpost", base.id(), List.of("like", "fav"));
            if (counts != null) {
                likeCount = counts.getOrDefault("like", likeCount == null ? 0L : likeCount);
                favoriteCount = counts.getOrDefault("fav", favoriteCount == null ? 0L : favoriteCount);
            }
        }

        // 第二步：获取用户维度的个性化状态（是否已点赞/收藏）
        // 这部分数据是用户特定的，不能存入公共缓存，必须每次实时查询
        Boolean liked = uid != null && counterService.isLiked("knowpost", base.id(), uid);
        Boolean faved = uid != null && counterService.isFaved("knowpost", base.id(), uid);

        // 第三步：构造新的 Record 对象返回
        return new KnowPostDetailResponse(
                base.id(), base.title(), base.description(), base.contentUrl(), base.images(), base.tags(),
                base.authorId(), base.authorAvatar(), base.authorNickname(), base.authorTagJson(),
                likeCount, favoriteCount, liked, faved, base.isTop(), base.visible(), base.type(), base.publishTime()
        );
    }

    /**
     * 记录内容热度，并根据热度等级延长相关缓存的 TTL。
     * 延长的缓存包括：
     * 1. 详情页整页缓存 (knowpost:detail:{id})
     * 2. Feed 流内容片段缓存 (feed:item:{id})
     * 这样可以确保热点内容在 Feed 流中也不会轻易过期，避免 Feed 流回源。
     */
    private void recordHotKeyAndExtendTtl(long id, String detailPageKey) {
        // 统一使用 knowpost:{id} 作为热度统计 Key
        String hotKeyId = "knowpost:" + id;
        // hotKey.record() 会记录当前时间戳和访问次数
        hotKey.record(hotKeyId);
        // 基础时间：60秒
        int baseTtl = 60;
        // 根据热度等级返回扩展秒数
        int target = hotKey.ttlForPublic(baseTtl, hotKeyId);

        // 1. 延长详情页缓存的当前TTL
        Long detailTtl = redis.getExpire(detailPageKey);
        if (detailTtl != null && detailTtl < target) {
            // Duration.ofSeconds() 创建 Duration 对象
            redis.expire(detailPageKey, java.time.Duration.ofSeconds(target));
        }

        // 2. 延长 Feed 流内容片段缓存
        // 发现页时，那个你可以一直往下滑，不断出现新内容的列表，就叫做 Feed 流。
        // 当底层系统探测到一本“书（详情页）”突然火了被疯狂翻阅时，它不仅会重点保护这本“书”，
        // 还会立刻派人跑到门口的展示架上，把这本书的“宣传海报（Feed卡片）”也用玻璃罩死死地保护起来，绝对不允许海报在这个时候被撤下！
        String itemKey = "feed:item:" + id;
        Long itemTtl = redis.getExpire(itemKey);
        if (itemTtl != null && itemTtl < target) {
            redis.expire(itemKey, java.time.Duration.ofSeconds(target));
        }
    }

    /**
     * 使指定帖子的缓存失效
     * @param id 帖子ID
     */
    private void invalidateCache(long id) {
        String pageKey
                = "knowpost:detail:" + id + ":v" + DETAIL_LAYOUT_VER;
        redis.delete(pageKey);
        knowPostDetailCache.invalidate(pageKey);
    }

    /**
     * 将JSON字符串解析为字符串列表
     * @param json JSON格式的字符串
     * @return 解析后的字符串列表，解析失败或输入为空时返回空列表
     */
    private List<String> parseStringArray(String json) {
        if (json == null || json.isBlank()) return Collections.emptyList();
        try {
            return objectMapper.readValue(json, new TypeReference<>() {});
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    /**
     * 判断当前用户是否有权查看该帖子。
     * @param resp 从缓存或数据库组装出的响应对象（包含 visible、authorId 等字段）
     * @param currentUserId 当前用户ID（可能为 null）
     * @return true 有权访问
     */
    private boolean isAuthorized(KnowPostDetailResponse resp, Long currentUserId) {
        // 注意：resp 中的 visible 可能为 "public", "followers", "school", "private", "unlisted"
        // 简化处理：仅公开（public）或者作者本人可以查看。
        // 如果你需要支持 followers/school 等更细粒度权限，需要额外查询关系表。
        if ("public".equals(resp.visible())) {
            return true;
        }
        // 作者本人永远有权
        return currentUserId != null && String.valueOf(currentUserId).equals(resp.authorId());
    }
}
//TODO confirmContent 中内容为空时的风险
// objectKey 等参数未做非空校验。如果前端传了空 objectKey，publicUrl 可能生成非法 URL。
// 建议：在方法开头增加 if (objectKey == null) throw ...。
// 2. updateMetadata 中 type 被硬编码为 image_text
//如果未来支持其他类型（如 video），该字段会被覆盖。建议从请求中传入或从数据库读取原有值再合并。
//
//3. Outbox 事件写入失败只记录 warn，不抛异常
//当前所有 Outbox 写入都是 catch (Exception e) { log.warn(...) }，不会导致事务回滚。这意味着数据库已更新，但事件可能丢失，导致搜索索引不一致。
//
//业务判断：如果搜索索引最终一致可接受，则保留；如果需要强一致，应抛出异常让事务回滚。建议至少增加监控告警。
//
//4. userCounterService.incrementPosts 异常被完全吞掉
//publish 方法中 catch (Exception ignored) {}，计数器更新失败没有日志。建议至少记录 error 日志，或使用消息队列异步重试。
//
//5. getDetail 方法上的 @Transactional(readOnly = true) 包含大量非数据库操作
//Redis、热点检测、计数服务调用等都在事务内，会长时间持有数据库连接，可能造成连接池压力。
//
//改进：移除 @Transactional，仅对必要的数据库查询手动使用 @Transactional(propagation = SUPPORTS) 或直接调用 Mapper。
//
//6. isAuthorized 方法仅支持 public 和作者本人
//你的可见性配置还支持 followers（粉丝可见）、school（同校可见）等，但 isAuthorized 并未实现这些逻辑。例如，当 visible = "followers" 时，需要查询关系表判断当前用户是否关注了作者。
//
//建议：根据业务需要扩展 isAuthorized，或者将复杂权限校验下沉到独立的 PermissionService。
//
//7. invalidateCache 没有删除 Feed 片段缓存
//更新帖子后，feed:item:{id} 缓存仍然存在，导致 Feed 流展示旧数据（标题、图片等）。
//
//改进：在 invalidateCache 中加上 redis.delete("feed:item:" + id);。
//
//8. recordHotKeyAndExtendTtl 中 detailTtl 为 -2 时的处理
//redis.getExpire 对于不存在的 key 返回 -2。与 target 比较时 -2 < target 为真，会执行 expire，但 expire 对不存在的 key 无效（返回 0），不影响功能，但建议先判断 detailTtl != null && detailTtl > 0 再延长，避免无效调用。
//
//9. RAG 索引服务调用没有重试机制
//ragIndexService.ensureIndexed 失败只记录 warn，用户首次问答可能遇到冷启动。可以考虑使用消息队列异步重试。
//
//10. 缓存空值（"NULL"）的 TTL 固定为 30~60 秒，可能被恶意遍历 ID
//攻击者可以不断请求不存在的 ID，每次都会写入空值缓存，虽然后续请求被拦截，但第一次仍会回源。可以引入布隆过滤器预先拦截不存在的 ID。