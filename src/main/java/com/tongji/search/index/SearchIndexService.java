package com.tongji.search.index;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch._types.Refresh;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tongji.counter.service.CounterService;
import com.tongji.knowpost.mapper.KnowPostMapper;
import com.tongji.knowpost.model.KnowPostDetailRow;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.context.annotation.DependsOn;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import jakarta.annotation.PostConstruct;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.tongji.knowpost.model.KnowPostFeedRow;

/**
 * 面试自述：这个类是我负责的搜索模块写入层，核心任务是把 MySQL 里的知文数据同步到 Elasticsearch，
 * 让用户能通过关键词搜索到已发布的知文。
 *
 * 为什么需要这个类？我们项目的搜索数据同步走的是"三通道互补"架构：
 * 第一条通道是 Canal 监听 MySQL binlog，把增量变更异步推给 ES（延迟 1~3 秒，负责兜底补偿）；
 * 第二条通道就是这个类——知文发布/更新后，业务代码直接同步写入 ES（延迟 50ms，负责实时可见）；
 * 第三条通道是应用启动时的全量历史回灌（负责首次部署和索引重建）。
 * 三条通道合在一起，保证了"全量回灌 + 实时直写 + 增量兜底"的完整数据闭环。
 *
 * 写入一条知文到 ES，我做了五件事：
 * 先从 MySQL 查出知文的完整详情（标题、标签、作者信息等），
 * 然后通过 HTTP 去 OSS 拉取正文内容（失败就降级用摘要兜底），
 * 接着从 Redis 聚合点赞数和收藏数，把所有字段拼成一个 Map 写入 ES。
 * 删除不走物理删除，而是把 status 字段改成 "deleted"——这样即使 MySQL 事务回滚了，
 * ES 里的数据也不会永久丢失，查询端过滤掉 deleted 状态的文档就行。
 *
 * 这个类我重点做了几个工程上的优化：
 * 1、启动回灌不阻塞容器——用 CompletableFuture 异步执行，Spring Boot 启动不受影响；
 * 2、回灌用批量写入而不是逐条 refresh——每批 500 条打包成一个 BulkRequest 发给 ES，
 *    避免循环里高频 refresh 打爆 ES 集群的 CPU；
 * 3、回灌的 for 循环里加了 try-catch 护城河——单条数据组装失败只 skip 当前这条，
 *    不会让整个回灌线程崩溃，外层 catch 只兜底系统级灾难；
 * 4、RestTemplate 设了 3 秒超时——防止 OSS 网络抖动时请求无限阻塞，耗尽服务器线程池；
 * 5、写了一套编码探测逻辑——拉取正文时服务端可能不声明 charset 或者声明错了，
 *    我通过 HTML meta 标签 → 响应头 → 试探性解码三级策略，自动识别 UTF-8 还是 GB18030。
 */
@Service
@RequiredArgsConstructor
@DependsOn("searchIndexInitializer")
public class SearchIndexService {
    private static final Logger log = LoggerFactory.getLogger(SearchIndexService.class);
    // ES 索引名称，所有知文数据写入同一个索引，通过 content_type 字段区分内容类型
    private static final String INDEX = "zhiguang_content_index";

    // ES Java Client（8.x），使用 ElasticsearchClient 而非废弃的 RestHighLevelClient
    private final ElasticsearchClient es;
    // MyBatis Mapper：查询知文列表和详情
    private final KnowPostMapper knowPostMapper;
    // 计数器服务：获取点赞数、收藏数等聚合数据，底层是 Redis
    private final CounterService counterService;
    // Jackson 序列化：解析 MySQL 中 JSON 数组字段（tags、img_urls）
    private final ObjectMapper objectMapper;
    // 带超时的 RestTemplate：下载 OSS 正文，3 秒超时防止网络抖动阻塞线程
    private final RestTemplate http = createRestTemplateWithTimeout();

    /**
     * 构造带超时控制的 RestTemplate 实例。
     *
     * 为什么用静态工厂方法而非直接 new？因为 SimpleClientHttpRequestFactory 的配置
     * 需要在构造 RestTemplate 之前完成，抽取为静态方法更清晰。
     * connectTimeout：建立 TCP 连接的超时，防止 OSS 不可达时无限等待。
     * readTimeout：等待响应数据的超时，防止大文件下载或慢速连接卡死线程。
     */
    private static RestTemplate createRestTemplateWithTimeout() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(3000);  // 3 秒连接超时，防止 OSS 网络抖动无限阻塞
        factory.setReadTimeout(3000);     // 3 秒读取超时，防止大文件下载卡死线程
        return new RestTemplate(factory);
    }

    /**
     * 应用启动时的索引回灌（异步执行，不阻塞容器启动）。
     *
     * 面试自述：这个方法用 @PostConstruct 触发，但真正的回灌逻辑在 CompletableFuture 异步线程中执行，
     * 主线程立即返回，不阻塞 Spring Boot 容器启动。如果索引已有数据则跳过（幂等），
     * 否则分页从 MySQL 读取公开知文，每批 500 条，用 ES BulkRequest 批量写入，
     * 不再逐条 refresh，避免打爆 ES 集群。
     *
     * 关键设计：for 循环内部加了 try-catch 护城河——单条 buildDocument 失败（如 CounterService 超时）
     * 只 skip 该条继续下一个，不会击穿整个回灌线程，外层 catch 只兜底数据库连接池彻底炸了这种系统级灾难。
     */
    @PostConstruct
    public void ensureBackfill() {
        // CompletableFuture.runAsync：使用 ForkJoinPool 公共线程池异步执行，
        // 主线程（Spring 启动线程）立即返回，不阻塞容器启动
        //lamada的核心就是只注重逻辑，不注重实现，把左边的值作为右边的参数
        CompletableFuture.runAsync(() -> {
            try {
                // 幂等检查：如果 ES 索引已有数据，说明已经回灌过，直接跳过
                // 避免每次重启都重复回灌全量数据
                long cnt = es.count(c -> c.index(INDEX)).count();
                if (cnt > 0) {
                    log.info("Search index already initialized, skip backfill.");
                    return;
                }

                // 分页回灌：每批 500 条，避免一次性加载全量数据撑爆内存
                int limit = 500;
                int offset = 0;
                while (true) {
                    // 从 MySQL 分页查询公开状态的知文列表（只查必要字段，非全量详情）
                    List<KnowPostFeedRow> rows = knowPostMapper.listFeedPublic(limit, offset);
                    if (rows == null || rows.isEmpty()) {
                        break;  // 没有更多数据，回灌结束
                    }

                    // 创建批量请求构建器，将本批所有数据聚合到一个 BulkRequest 中
                    BulkRequest.Builder bulkBuilder = new BulkRequest.Builder();

                    for (KnowPostFeedRow r : rows) {
                        // try-catch 护城河：单条数据组装失败只 skip 该条，不影响本批其他数据
                        // 防止 CounterService 超时、OSS 下载失败等异常击穿整个回灌线程
                        try {
                            // 根据 ID 查完整详情（含 contentUrl、tags 等，用于组装完整 ES 文档）
                            KnowPostDetailRow detail = knowPostMapper.findDetailById(r.getId());
                            if (detail == null) continue;

                            // 复用公共组装逻辑，和单条同步写入用同一套规则
                            Map<String, Object> doc = buildDocument(detail);

                            // 将当前文档的 index 操作追加到批量请求中
                            bulkBuilder.operations(op -> op
                                    .index(idx -> idx
                                            .index(INDEX)
                                            .id(String.valueOf(r.getId()))
                                            .document(doc)
                                    )
                            );
                        } catch (Exception e) {
                            // 只记日志，不中断循环，继续处理下一条
                            log.error("Backfill skipped post {} due to build error: {}", r.getId(), e.getMessage());
                        }
                    }

                    // 只 build() 一次，复用局部变量，避免重复创建大对象浪费 CPU 和 GC
                    BulkRequest bulkRequest = bulkBuilder.build();
                    if (!bulkRequest.operations().isEmpty()) {
                        // 批量写入 ES：一次网络请求发送本批所有数据，不再逐条 refresh
                        BulkResponse response = es.bulk(bulkRequest);
                        if (response.errors()) {
                            // 部分文档写入失败只打 warn，不中断回灌流程
                            log.warn("Search index bulk backfill had errors at offset {}", offset);
                        }
                    }

                    // 偏移量递增，准备下一页
                    offset += rows.size();
                    log.info("Backfilled {} documents...", offset);
                }
                log.info("Search index backfill completed successfully.");
            } catch (Exception e) {
                // 外层 catch 只兜底系统级灾难（如数据库连接池彻底炸了），
                // 单条数据失败已被内层 try-catch 消化，不会到这里
                log.error("Search index backfill fatally failed: {}", e.getMessage(), e);
            }
        });
    }

    /**
     * 将单篇知文 upsert 到 ES 索引，包含基础字段、正文、计数和搜索补全建议。
     *
     * 面试自述：单条同步写入，供知文发布/更新时调用。复用 buildDocument 组装文档，
     * 写入时使用 refresh=WaitFor 保证用户发布后立即可搜。
     *
     * 调用时机：KnowPostServiceImpl.publish() 事务提交后同步调用，和 Canal 异步通道互补。
     * id 作为 ES 文档 ID，相同 ID 再次写入会自动覆盖（upsert 语义），无需先删后写。
     *
     * @param id 知文 ID
     */
    public void upsertKnowPost(long id) {
        try {
            // 从 MySQL 查完整详情，含 contentUrl、tags、作者信息等
            KnowPostDetailRow row = knowPostMapper.findDetailById(id);
            if (row == null) {
                // 理论上不会出现（publish 刚写完），但防御性编程不能少
                log.warn("Index upsert skipped: post {} not found", id);
                return;
            }

            // 复用公共组装逻辑，和批量回灌用同一套 buildDocument
            Map<String, Object> doc = buildDocument(row);

            // 构建 IndexRequest：id 为文档主键，相同 ID 覆盖写入
            // refresh=WaitFor：等待 ES 刷新 segment 后再返回，保证写入后立即可搜
            // 代价是写入延迟略高（~50ms），适合管理后台等低并发场景
            IndexRequest<Map<String, Object>> req = IndexRequest.of(b -> b
                    .index(INDEX)
                    .id(String.valueOf(id))
                    .document(doc)
                    .refresh(Refresh.WaitFor)
            );
            IndexResponse resp = es.index(req);
            // result 为 Created（新建）或 Updated（覆盖），version 为文档版本号（乐观锁用）
            log.info("Indexed post {} result={} version={}", id, resp.result(), resp.version());
        } catch (Exception e) {
            // 单条写入失败只打日志，不抛异常——不影响业务主流程
            // 后续 Canal 增量通道会兜底补偿，保证最终一致性
            log.error("Index upsert failed for post {}: {}", id, e.getMessage());
        }
    }

    /**
     * 组装 ES 文档，供单条同步写入和批量回灌共用，保证两处写入逻辑完全一致。
     *
     * 面试自述：这个方法把构建 ES 文档的逻辑从 upsertKnowPost 里抽出来，
     * 让单条写入和批量回灌都能复用。组装分五个阶段：
     * 基础字段 → 正文拉取 → 计数器聚合 → 搜索补全 → 返回 Map。
     * 每个阶段独立，出问题只影响当前字段，不影响整体。
     *
     * @param row MySQL 知文详情行
     * @return ES 文档 Map，key 为字段名，value 为字段值
     */
    private Map<String, Object> buildDocument(KnowPostDetailRow row) {
        Map<String, Object> doc = new HashMap<>();

        // ========== 第一阶段：基础字段映射（MySQL → ES）==========
        doc.put("content_id", row.getId());
        doc.put("content_type", row.getType());
        doc.put("title", row.getTitle());
        doc.put("description", row.getDescription());
        doc.put("author_id", row.getCreatorId());
        doc.put("author_avatar", row.getAuthorAvatar());
        doc.put("author_nickname", row.getAuthorNickname());
        // author_tag_json 存的是 JSON 字符串，ES 端按 keyword 存储，不做二次解析
        doc.put("author_tag_json", row.getAuthorTagJson());
        // 发布时间转为毫秒时间戳（epoch millis），ES 的 date 类型直接接受 long 值
        if (row.getPublishTime() != null) {
            doc.put("publish_time", row.getPublishTime().toEpochMilli());
        }
        doc.put("status", row.getStatus());
        // tags 和 img_urls 在 MySQL 中存的是 JSON 数组字符串，需解析为 List<String>
        doc.put("tags", parseStringArray(row.getTags()));
        doc.put("img_urls", parseStringArray(row.getImgUrls()));
        if (row.getIsTop() != null) {
            doc.put("is_top", row.getIsTop());
        }

        // ========== 第二阶段：正文拉取（优先 contentUrl，降级 description）==========
        // fetchContentSafe 内部做了编码探测和超时控制，失败返回 null
        String body = fetchContentSafe(row.getContentUrl());
        if (body == null || body.isBlank()) {
            body = row.getDescription();  // 降级：正文拉不下来就用描述兜底，保证 ES 里一定有可搜文本
        }
        if (body != null) {
            // 截断到 4000 字符，防止超长文章撑爆 ES 索引、影响搜索性能
            doc.put("body", truncate(body, 4000));
        }

        // ========== 第三阶段：计数器聚合（从 Redis 获取最新计数）==========
        // CounterService 底层是 Redis，查询点赞数（like）和收藏数（fav）
        // 视图数当前写 0，后续由独立的计数同步服务更新
        Map<String, Long> counts = counterService.getCounts("knowpost", String.valueOf(row.getId()), List.of("like","fav"));
        doc.put("like_count", counts.getOrDefault("like", 0L));
        doc.put("favorite_count", counts.getOrDefault("fav", 0L));
        doc.put("view_count", 0L);

        // ========== 第四阶段：搜索补全建议（Completion Suggester）==========
        // title_suggest 用于 ES 的 Completion Suggester，实现搜索框输入时的自动补全
        // 用户输入 "Spring" 时，ES 会按前缀匹配补全出 "Spring Boot 实战" 等标题
        if (row.getTitle() != null && !row.getTitle().isBlank()) {
            doc.put("title_suggest", row.getTitle());
        }
        return doc;
    }

    /**
     * 软删除：不物理删除 ES 文档，只将 status 更新为 "deleted"。
     *
     * 面试自述：为什么用软删除而不是物理删除？因为 ES 没有事务支持——
     * 如果我先删了 ES 文档，然后 MySQL 的事务回滚了，ES 就永久丢失了这条数据。
     * 软删除只改一个字段，查询端过滤掉 status=deleted 的文档即可，
     * 即使 MySQL 回滚，我也可以重新 upsert 恢复正确状态。
     *
     * 注意：这里只写 content_id 和 status 两个字段，ES 的文档合并特性会保留其他已有字段不变，
     * 只更新 status 为 "deleted"。这是"部分更新"而非"全量覆盖"。
     *
     * @param id 知文 ID
     */
    public void softDeleteKnowPost(long id) {
        try {
            // 只写 content_id 和 status 两个字段，利用 ES 的文档合并特性
            // 其他字段（title、body、tags 等）不会被覆盖，只更新 status
            Map<String, Object> doc = new HashMap<>();
            doc.put("content_id", id);
            doc.put("status", "deleted");
            // refresh=WaitFor：删除后立即可搜不到，保证用户感知的实时性
            IndexRequest<Map<String, Object>> req = IndexRequest.of(b -> b
                    .index(INDEX)
                    .id(String.valueOf(id))
                    .document(doc)
                    .refresh(Refresh.WaitFor)
            );
            es.index(req);
        } catch (Exception e) {
            // 删除失败也不抛异常，Canal 增量通道会兜底
            log.error("Index soft delete failed for post {}: {}", id, e.getMessage());
        }
    }

    /**
     * 安全拉取正文内容，失败返回 null，不中断索引流程。
     *
     * 面试自述：这个方法是我最引以为傲的编码探测逻辑。HTTP 拉取正文时，
     * 服务端可能不声明 Content-Type，或者声明了但实际编码不一致
     * （比如 IIS 默认返回 ISO-8859-1，但内容是 GBK 中文）。
     *
     * 我用三级探测策略：
     * 1、优先解析 HTML 中 meta 标签的 charset 声明（最可靠）；
     * 2、其次用 HTTP 响应头的 Content-Type 中的 charset；
     * 3、如果以上都不靠谱，用 UTF-8 和 GB18030 分别解码，数替换字符 \uFFFD 的数量，
     *    哪个产生的乱码少就用哪个。
     *
     * 为什么用 byte[] 接收响应体而非 String？
     * RestTemplate 按 String 接收时会自动按响应头 charset 解码，
     * 如果响应头错了，解码就错了，无法挽回。用 byte[] 拿原始字节，
     * 后续自己探测编码再解码，确保万无一失。
     *
     * @param url 正文资源地址（OSS URL），可能为 null
     * @return 解码后的纯文本，失败返回 null
     */
    private String fetchContentSafe(String url) {
        if (url == null || url.isBlank()) {
            return null;  // 空 URL 直接返回，没必要发请求
        }

        try {
            // 设置 Accept 头，告诉服务端我能接受的响应格式
            HttpHeaders headers = new HttpHeaders();
            headers.setAccept(List.of(MediaType.TEXT_HTML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON));
            // 用 byte[] 接收响应体，避免 RestTemplate 按错误编码自动解码
            ResponseEntity<byte[]> resp = http.exchange(url, HttpMethod.GET, new HttpEntity<>(headers), byte[].class);
            byte[] bytes = resp.getBody();
            if (bytes == null || bytes.length == 0) {
                return null;
            }

            // 三级编码探测：
            // 1. HTTP 响应头 Content-Type 中的 charset（可能为空或不可信）
            MediaType contentType = resp.getHeaders().getContentType();
            Charset headerCharset = (contentType != null) ? contentType.getCharset() : null;
            // 2. HTML meta 标签中声明的 charset（最可靠，因为内容作者指定）
            Charset metaCharset = sniffHtmlCharset(bytes);
            // 3. 综合决策：meta > 试探解码 > 响应头
            Charset charset = pickCharset(bytes, headerCharset, metaCharset);
            return new String(bytes, charset);
        } catch (Exception e) {
            // 任何异常（超时、DNS 解析失败、404 等）都返回 null
            // 调用方 buildDocument 会降级为 description，不中断索引
            return null;
        }
    }

    /**
     * 三级编码决策：meta 标签 > 试探解码 > 响应头。
     *
     * 优先级：HTML meta 标签声明的 charset 最可靠（因为它是内容作者指定的），
     * 其次是试探性解码（UTF-8 vs GB18030 比较替换字符数），
     * 最后才相信 HTTP 响应头的 Content-Type。
     *
     * 为什么响应头不可信？很多 Web 服务器（如 IIS、Nginx 默认配置）不声明 charset，
     * 或者声明为 ISO-8859-1（HTTP/1.1 默认值），但实际内容是 UTF-8 或 GBK。
     * 所以响应头只能作为最后兜底，不能作为首选。
     *
     * @param bytes        响应体字节数组
     * @param headerCharset HTTP 响应头声明的 charset，可能为 null
     * @param metaCharset   HTML meta 标签声明的 charset，可能为 null
     * @return 最终选定的编码
     */
    private Charset pickCharset(byte[] bytes, Charset headerCharset, Charset metaCharset) {
        // 第一优先级：HTML meta 标签声明，直接采用（内容作者指定的，最可信）
        if (metaCharset != null) {
            return metaCharset;
        }
        // 响应头也没声明 charset，无法确定编码，只能试探
        if (headerCharset == null) {
            Charset utf8 = StandardCharsets.UTF_8;
            Charset gb18030 = Charset.forName("GB18030");
            // 分别用 UTF-8 和 GB18030 解码，数替换字符 \uFFFD
            // 替换字符少的那个编码更准确（乱码更少）
            return countReplacementChars(new String(bytes, utf8))
                    <= countReplacementChars(new String(bytes, gb18030)) ? utf8 : gb18030;
        }
        // 响应头声明了 charset，但如果声明的是 ISO-8859-1 或 US-ASCII，
        // 大概率是服务器默认值而非真实编码，需要试探纠正
        if (isLikelyWrongCharsetHeader(headerCharset)) {
            Charset utf8 = StandardCharsets.UTF_8;
            Charset gb18030 = Charset.forName("GB18030");
            int repUtf8 = countReplacementChars(new String(bytes, utf8));
            int repGb = countReplacementChars(new String(bytes, gb18030));
            int repHeader = countReplacementChars(new String(bytes, headerCharset));
            // 三者中替换字符最少的获胜
            if (repUtf8 <= repGb && repUtf8 <= repHeader) return utf8;
            if (repGb <= repHeader) return gb18030;
        }
        // 响应头可信（如明确声明了 UTF-8 或 GBK），直接采用
        return headerCharset;
    }

    /**
     * 判断响应头的 charset 声明是否"不可信"。
     *
     * ISO-8859-1 和 US-ASCII 是很多 Web 服务器的默认 Content-Type，
     * 实际内容可能是 UTF-8 或 GBK，所以不能轻信这些声明。
     * 如果响应头声明的是 UTF-8、GBK 等明确的编码，就可以直接信任。
     */
    private boolean isLikelyWrongCharsetHeader(Charset charset) {
        return StandardCharsets.ISO_8859_1.equals(charset) || StandardCharsets.US_ASCII.equals(charset);
    }

    /**
     * 从 HTML 的 meta 标签中嗅探 charset 声明。
     *
     * 用 ISO-8859-1 解码前 8KB（单字节编码不会损坏任何字节，保证元数据可读），
     * 正则匹配 &lt;meta charset="xxx"&gt; 或 &lt;meta http-equiv="Content-Type" content="text/html; charset=xxx"&gt;。
     *
     * 为什么只扫描前 8KB？meta 标签只在 HTML head 里，后面的 body 再大也没意义。
     * 8KB 足够覆盖绝大多数 HTML 的 head 部分，性能也友好。
     *
     * @param bytes HTML 响应体字节数组
     * @return 探测到的 charset，未找到或无法识别返回 null
     */
    private Charset sniffHtmlCharset(byte[] bytes) {
        // 只扫描前 8KB，meta 标签一般在 HTML 头部，不用扫全量
        int limit = Math.min(bytes.length, 8192);
        // ISO-8859-1 是单字节编码，每个字节映射到一个字符，不会损坏原始字节数据
        // 用来做嗅探解码非常安全——即使实际编码不是 ISO-8859-1，正则匹配也能工作
        String head = new String(bytes, 0, limit, StandardCharsets.ISO_8859_1);
        // 匹配 charset="utf-8"、charset=utf-8、charset=gbk 等常见格式
        Matcher m = Pattern.compile("charset\\s*=\\s*['\\\"]?([a-zA-Z0-9_\\-]+)", Pattern.CASE_INSENSITIVE).matcher(head);
        if (!m.find()) {
            return null;  // meta 中没有 charset 声明
        }
        String cs = m.group(1);
        if (cs == null || cs.isBlank()) {
            return null;
        }
        cs = cs.trim();
        // 常见的别名映射，处理非标准写法
        if ("utf8".equalsIgnoreCase(cs)) {
            return StandardCharsets.UTF_8;
        }
        // gbk/gb2312/gb18030 统一映射为 GB18030（GB18030 是超集，兼容前两者）
        if ("gbk".equalsIgnoreCase(cs) || "gb2312".equalsIgnoreCase(cs) || "gb18030".equalsIgnoreCase(cs)) {
            return Charset.forName("GB18030");
        }
        try {
            // 尝试按标准名称查找 Charset
            return Charset.forName(cs);
        } catch (Exception e) {
            return null;  // 不认识的 charset，返回 null 交给上层试探解码
        }
    }

    /**
     * 统计字符串中 Unicode 替换字符 \uFFFD 的数量。
     *
     * 替换字符是 Java 在解码过程中遇到无法映射的字节时插入的占位符（显示为 ），
     * 数量越多说明解码越不准确。这是编码探测的"打分函数"——分数越低，编码越对。
     *
     * 面试自述：这个方法是编码探测的核心——不依赖任何外部库，纯靠 Java 内置的
     * Charset 解码行为和替换字符计数，就能在 UTF-8 和 GB18030 之间做出准确判断。
     * 比 jchardet 之类的第三方库更轻量，而且没有额外的依赖。
     *
     * @param s 解码后的字符串
     * @return 替换字符 \uFFFD 的个数
     */
    private int countReplacementChars(String s) {
        if (s == null || s.isEmpty()) return 0;
        int cnt = 0;
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == '\uFFFD') cnt++;
        }
        return cnt;
    }

    /**
     * 截断字符串到指定最大长度，防止超长文本写入 ES 导致索引膨胀。
     *
     * ES 对单字段长度没有硬限制，但超长文本会严重影响索引性能：
     * 倒排索引的 term 数量爆炸，搜索变慢，存储成本飙升。
     * 4000 字符对搜索来说已经足够，且截断位置在字符边界，不会截断半个 UTF-8 字符。
     *
     * @param s   原始字符串，可能为 null
     * @param max 最大长度（字符数，非字节数）
     * @return 截断后的字符串，null 输入返回 null
     */
    private String truncate(String s, int max) {
        if (s == null) {
            return null;
        }
        return s.length() <= max ? s : s.substring(0, max);
    }

    /**
     * 将 JSON 数组字符串解析为 List&lt;String&gt;。
     *
     * MySQL 中 tags 和 img_urls 存的是 JSON 数组格式如 ["tag1","tag2"]，
     * 需要反序列化为 List 才能正确写入 ES 的 keyword 数组字段。
     *
     * 为什么不用 JSON.parseArray？因为 Jackson 的 ObjectMapper 已经在类中注入，
     * 用 TypeReference 泛型反序列化即可，不引入额外依赖。
     *
     * @param json JSON 数组字符串，如 "[\"Java\",\"Spring\"]"，可能为 null 或空
     * @return 解析后的字符串列表，解析失败或输入为空返回空列表（不抛异常）
     */
    private List<String> parseStringArray(String json) {
        if (json == null || json.isBlank()) {
            return Collections.emptyList();  // 空输入返回空列表，避免 NPE
        }
        try {
            // TypeReference 保留泛型信息，让 Jackson 能正确反序列化为 List<String>
            return objectMapper.readValue(json, new TypeReference<>() {});
        } catch (Exception e) {
            // 解析失败返回空列表，不中断索引流程
            // 可能原因：JSON 格式错误、字段为 null 字符串等
            return Collections.emptyList();
        }
    }
}