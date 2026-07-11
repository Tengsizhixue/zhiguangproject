package com.tongji.search.service.impl;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.FieldValueFactorModifier;
import co.elastic.clients.elasticsearch._types.query_dsl.FunctionBoostMode;
import co.elastic.clients.elasticsearch.core.search.HighlightField;
import co.elastic.clients.elasticsearch.core.search.Suggestion;
import co.elastic.clients.util.NamedValue;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.tongji.knowpost.api.dto.FeedItemResponse;
import com.tongji.counter.service.CounterService;
import com.tongji.search.api.dto.SearchResponse;
import com.tongji.search.api.dto.SuggestResponse;
import com.tongji.search.service.SearchService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * SearchServiceImpl 是搜索模块的"读"端——负责关键词检索和搜索补全，是前端搜索框的两个 API 的后端实现。和 SearchIndexService（写）是镜像关系。
 * 搜索服务实现：ES 查询层，负责关键词检索和搜索补全，是搜索模块的"读"端。
 *
 * 面试自述：这个类是我搜索模块的查询入口，前端调过来的搜索请求最终都落在这里。
 * 和 SearchIndexService（写）是镜像关系——一个负责把数据写入 ES，一个负责从 ES 查出来。
 *
 * 搜索链路我做了四件事：
 * 1、宽召回：用 multi_match 同时搜索 title（权重 3 倍）和 body，保证相关文章都能命中；
 * 2、业务加权：用 function_score 把点赞数和浏览量作为加分因子，热门内容排名更靠前；
 * 3、高亮片段：ES 返回匹配关键词的上下文片段，拼成 snippet 展示给用户；
 * 4、游标分页：用 search_after 代替传统的 from+size，避免深度分页时 ES 性能暴跌。
 *
 * 搜索补全（suggest）用的是 ES 的 Completion Suggester，基于 title_suggest 字段做前缀匹配，
 * 用户输入前几个字就能实时补全出完整标题，类似百度搜索框的体验。
 *
 * 设计亮点：
 * - search_after 游标分页：用 Base64 编码上一页最后一条的 sort 值，传给 ES 做翻页，
 *   比 from+size 方案好在不跳页，ES 不需要遍历前 N 页的数据，深分页性能稳定；
 * - function_score 加权：点赞数用 log1p 做平滑处理（避免极端值垄断），再乘以权重 2.0，
 *   浏览量权重 1.0，追加模式（Sum），保证相关性为主、热度为辅；
 * - 高亮合并：title 和 body 的高亮片段都可能包含匹配词，按 title 先、body 后拼接，
 *   优先展示标题中的匹配，正文补充上下文。
 */
@Service
@RequiredArgsConstructor
public class SearchServiceImpl implements SearchService {

    private final ElasticsearchClient es;
    private final CounterService counterService;
    // ES 索引名：和 SearchIndexService 中写入的索引保持一致
    private static final String INDEX = "zhiguang_content_index";

    /**
     * 关键词检索：相关性 + 互动数据加权，支持游标分页与高亮。
     *
     * 面试自述：这是搜索的核心方法。前端传来关键词 q、分页参数 size、可选标签过滤 tagsCsv、
     * 上一页游标 after、当前用户 ID，我组装 ES DSL 查询，返回搜索结果列表 + 下一页游标。
     *
     * 查询结构（从内到外）：
     * 1. bool 查询：must（multi_match 关键词搜索）+ filter（status=published + 可选 tags）
     * 2. function_score 包裹：对 like_count 和 view_count 加权，热门内容排名更高
     * 3. highlight 高亮：title 和 body 中匹配的关键词用 <em> 标签包裹
     * 4. sort 排序：相关性 > 发布时间 > 点赞数 > 浏览量 > content_id（稳定排序）
     * 5. search_after 游标：用上一页最后一条的 sort 值做翻页
     *
     * @param q             搜索关键词
     * @param size          每页条数
     * @param tagsCsv       逗号分隔的标签过滤条件，如 "Java,Spring"
     * @param after         上一页游标（Base64 编码的 sort 值），首页传 null
     * @param currentUserIdNullable 当前用户 ID，用于判断是否已点赞/收藏
     * @return 搜索结果（含 snippet、下一页游标、是否有更多）
     */
    @SuppressWarnings("unchecked")
    public SearchResponse search(String q, int size, String tagsCsv, String after, Long currentUserIdNullable) {
        // 解析逗号分隔的标签字符串为 List，如 "Java,Spring" → ["Java", "Spring"]
        List<String> tags = parseCsv(tagsCsv);
        // 解析 Base64 游标为 FieldValue 列表，用于 search_after 分页
        List<FieldValue> afterValues = parseAfter(after);

        // 复合排序：优先相关性（_score），其次发布时间与互动数据，最后按 content_id 保证稳定排序
        // 为什么需要 content_id 兜底？防止两条数据的 score/publish_time/like/view 完全相同时排序不稳定
        List<SortOptions> sorts = new ArrayList<>();
        sorts.add(SortOptions.of(s -> s.score(o -> o.order(SortOrder.Desc))));           // 1. 相关性评分
        sorts.add(SortOptions.of(s -> s.field(f -> f.field("publish_time").order(SortOrder.Desc)))); // 2. 发布时间
        sorts.add(SortOptions.of(s -> s.field(f -> f.field("like_count").order(SortOrder.Desc))));   // 3. 点赞数
        sorts.add(SortOptions.of(s -> s.field(f -> f.field("view_count").order(SortOrder.Desc))));   // 4. 浏览量
        sorts.add(SortOptions.of(s -> s.field(f -> f.field("content_id").order(SortOrder.Desc))));   // 5. ID 稳定排序

        // 完整包名避免和自定义的 SearchResponse 冲突
        co.elastic.clients.elasticsearch.core.SearchResponse<Map<String, Object>> resp;
        try {
            resp = es.search(s -> {
                var b = s.index(INDEX)
                        .size(size)
                        // ========== 召回与加权 ==========
                        // 外层 function_score 包裹内层 bool 查询，
                        // 对已召回的结果做点赞数和浏览量的加权加分
                        .query(qb -> qb.functionScore(fs -> fs
                                .query(qb2 -> qb2.bool(bq -> {
                                    // must：关键词必须在 title 或 body 中匹配
                                    // title^3 表示 title 的权重是 body 的 3 倍，标题命中更相关
                                    bq.must(m -> m.multiMatch(mm -> mm.query(q)
                                            .fields("title^3", "body")));
                                    // filter：只查已发布的文章，过滤掉草稿和已删除的
                                    bq.filter(f -> f.term(t -> t.field("status")
                                            .value(v -> v.stringValue("published"))));

                                    // 如果传了标签过滤条件，追加 filter
                                    if (tags != null && !tags.isEmpty()) {
                                        bq.filter(f -> f.terms(t -> t.field("tags")
                                                .terms(tv -> tv.value(tags.stream().map(FieldValue::of).toList()))));
                                    }
                                    return bq;
                                }))
                                // 对点赞数加权：log1p 平滑处理（点赞 10000 不会比 1000 高 10 倍），权重 2.0
                                .functions(fn -> fn.fieldValueFactor(fvf -> fvf.field("like_count")
                                        .modifier(FieldValueFactorModifier.Log1p))
                                        .weight(2.0))
                                // 对浏览量加权：同样 log1p 平滑，权重 1.0（点赞比浏览更重要）
                                .functions(fn -> fn.fieldValueFactor(fvf -> fvf.field("view_count")
                                        .modifier(FieldValueFactorModifier.Log1p))
                                        .weight(1.0))
                                // boostMode=Sum：将加权分数追加到原始相关性分数上，而非替换
                                .boostMode(FunctionBoostMode.Sum)
                        ))
                        // ========== 高亮 ==========
                        // 返回 title 和 body 中匹配关键词的上下文片段，用 <em> 标签包裹
                        .highlight(h -> h
                                .fields(new NamedValue<>("title", HighlightField.of(f -> f)))
                                .fields(new NamedValue<>("body", HighlightField.of(f -> f)))
                        )
                        .sort(sorts);
                // ========== 游标分页 ==========
                // search_after：用上一页最后一条的 sort 值，ES 直接定位到下一页起始位置
                // 比 from+size 好在不跳页，深分页性能稳定
                if (afterValues != null && !afterValues.isEmpty()) {
                    b = b.searchAfter(afterValues);
                }

                return b;
            }, (Class<Map<String, Object>>)(Class<?>) Map.class);
        } catch (Exception e) {
            // ES 查询异常（超时、集群不可用等），返回空结果，不抛异常影响前端
            return new SearchResponse(Collections.emptyList(), null, false);
        }

        // ========== 结果映射：ES 结果 → 前端 FeedItemResponse ==========
        List<FeedItemResponse> items = new ArrayList<>();
        List<Hit<Map<String, Object>>> hits = resp.hits() == null ? Collections.emptyList() : resp.hits().hits();

        for (Hit<Map<String, Object>> hit : hits) {
            Map<String, Object> source = hit.source();
            if (source == null) {
                continue;
            }
            // 从 ES 文档中提取各字段，asString/asLong/asStringList 做类型安全转换
            String id = asString(source.get("content_id"));
            String title = asString(source.get("title"));
            String descriptionFromDoc = asString(source.get("description"));
            // 优先用高亮片段（snippet），没有高亮才用原始 description
            String snippet = buildSnippet(hit);
            String description = (snippet != null && !snippet.isBlank()) ? snippet : descriptionFromDoc;
            List<String> tagList = asStringList(source.get("tags"));
            List<String> imgs = asStringList(source.get("img_urls"));
            String cover = imgs.isEmpty() ? null : imgs.getFirst();  // 封面取第一张图片
            String authorAvatar = asString(source.get("author_avatar"));
            String authorNickname = asString(source.get("author_nickname"));
            String tagJson = asString(source.get("author_tag_json"));
            Long likeCount = asLong(source.get("like_count"));
            Long favoriteCount = asLong(source.get("favorite_count"));
            // 如果传了当前用户 ID，查 Redis 判断该用户是否已点赞/收藏这篇文章
            Boolean liked = currentUserIdNullable != null && counterService.isLiked("knowpost", id, currentUserIdNullable);
            Boolean faved = currentUserIdNullable != null && counterService.isFaved("knowpost", id, currentUserIdNullable);
            items.add(new FeedItemResponse(
                    id,
                    title,
                    description,
                    cover,
                    tagList,
                    authorAvatar,
                    authorNickname,
                    tagJson,
                    likeCount,
                    favoriteCount,
                    liked,
                    faved,
                    null
            ));
        }

        // ========== 构造下一页游标 ==========
        String nextAfter = null;
        // 如果返回结果数 >= 请求的 size，说明可能还有下一页
        boolean hasMore = items.size() >= size;

        if (!hits.isEmpty()) {
            // 取最后一条命中的 sort 值，编码为 Base64 游标返回给前端
            List<FieldValue> sv = hits.getLast().sort();
            if (sv != null && !sv.isEmpty()) {
                // 将 sort 值列表转为逗号分隔的字符串，再 Base64 编码
                List<String> parts = sv.stream().map(this::fieldValueToString).collect(Collectors.toList());
                nextAfter = Base64.getUrlEncoder().withoutPadding().encodeToString(String.join(",", parts).getBytes());
            }
        }

        return new SearchResponse(items, nextAfter, hasMore);
    }

    /**
     * 搜索补全（Suggest）：基于 ES Completion Suggester 做前缀匹配。
     *
     * 面试自述：用户在搜索框输入"Jav"时，前端实时调用这个接口，返回"Java"、"Java基础"等补全词。
     * 底层用的是 ES 的 Completion Suggester，不是 term/match 查询，因为它基于 FST（有限状态机）
     * 数据结构，前缀匹配速度极快（毫秒级），远快于普通倒排索引查询。
     *
     * 为什么 SearchIndexService 写入时要同时存 title_suggest 字段？
     * Completion Suggester 要求字段类型为 completion，需要单独的 mapping 定义。
     * 写入时把 title 同时存入 title_suggest，ES 内部会构建 FST 索引，查询时直接前缀匹配。
     *
     * @param prefix 用户输入的前缀，如 "Jav"
     * @param size   返回补全词的数量
     * @return 补全词列表，如 ["Java", "Java基础", "Java进阶"]
     */
    @SuppressWarnings("unchecked")
    public SuggestResponse suggest(String prefix, int size) {
        co.elastic.clients.elasticsearch.core.SearchResponse<Map<String, Object>> resp;
        try {
            resp = es.search(s -> s.index(INDEX)
                    .suggest(sug -> sug.suggesters("title_suggest",
                            sc -> sc.prefix(prefix).completion(c -> c.field("title_suggest").size(size))))
                    , (Class<Map<String, Object>>)(Class<?>) Map.class);
        } catch (Exception e) {
            // ES 异常返回空列表，不阻塞前端交互
            return new SuggestResponse(Collections.emptyList());
        }
        List<String> items = new ArrayList<>();
        try {
            var sugg = resp.suggest();
            // 从 suggest 响应中提取 title_suggest 的 options
            List<Suggestion<Map<String, Object>>> entry = sugg == null ? null : sugg.get("title_suggest");
            if (entry != null) {
                for (var s : entry) {
                    var comp = s.completion();
                    if (comp != null && comp.options() != null) {
                        for (var opt : comp.options()) {
                            String text = opt.text();
                            if (text != null && !text.isBlank()) {
                                items.add(text);
                            }
                        }
                    }
                }
            }
        } catch (Exception ignored) {
            // 解析异常不影响返回，最多返回空列表
        }
        return new SuggestResponse(items);
    }

    /**
     * 解析逗号分隔标签字符串为 List<String>，如 "Java,Spring" → ["Java", "Spring"]。
     * 空字符串返回 null，表示不做标签过滤。
     */
    private List<String> parseCsv(String csv) {
        if (csv == null || csv.isBlank()) {
            return null;
        }

        String[] parts = csv.split(",");
        List<String> out = new ArrayList<>();

        for (String p : parts) {
            String t = p.trim();
            if (!t.isEmpty()) {
                out.add(t);
            }
        }
        return out;
    }

    /**
     * 解析 Base64URL 编码的游标，还原为 ES search_after 所需的 FieldValue 列表。
     *
     * 游标编码逻辑：前端拿到本方法返回的 nextAfter，下次查询传给 after，
     * 这里解码成上一页最后一条的 sort 值数组，ES 从这个位置往后翻页。
     * 为什么用 Base64 编码？把多个排序字段打包成一个字符串，对前端友好，
     * 避免暴露内部排序逻辑，同时兼容 URL 传输。
     *
     * 解码顺序：第 0 位是相关性评分（Double），第 1 位是发布时间（Long），
     * 第 2 位点赞数，第 3 位浏览量，第 4 位 content_id，和排序顺序一致。
     */
    private List<FieldValue> parseAfter(String after) {
        if (after == null || after.isBlank()) {
            return null;
        }

        try {
            String decoded = new String(Base64.getUrlDecoder().decode(after));
            String[] parts = decoded.split(",");
            List<FieldValue> out = new ArrayList<>(parts.length);

            for (int i = 0; i < parts.length; i++) {
                String p = parts[i];
                if (i == 0) {
                    out.add(FieldValue.of(Double.parseDouble(p)));
                } else if (i == 1) {
                    out.add(FieldValue.of(Long.parseLong(p)));
                } else {
                    out.add(FieldValue.of(Long.parseLong(p)));
                }
            }

            return out;
        } catch (Exception e) {
            // 编码解析失败（可能是用户篡改参数），返回 null 按首页处理
            return null;
        }
    }

    /**
     * 合并 title 和 body 的高亮片段为一个 snippet（摘要）。
     *
     * 为什么需要合并？ES 会分别对 title 和 body 做高亮，返回多个片段。
     * 前端展示搜索结果需要一段包含关键词的摘要，所以我们自己拼接。
     * 拼接顺序：title 片段在前，body 片段在后——优先展示标题中的关键词，
     * 正文片段补充上下文，符合用户阅读习惯。
     *
     * 如果没有找到任何高亮片段，返回 null，上层用原始 description 兜底。
     */
    private String buildSnippet(Hit<Map<String, Object>> hit) {
        StringBuilder sb = new StringBuilder();

        if (hit.highlight() != null) {
            List<String> ht = hit.highlight().get("title");
            if (ht != null && !ht.isEmpty()) {
                sb.append(String.join(" ", ht));
            }

            List<String> hb = hit.highlight().get("body");
            if (hb != null && !hb.isEmpty()) {
                if (!sb.isEmpty()) {
                    sb.append(" ");
                }
                sb.append(String.join(" ", hb));
            }
        }

        return sb.isEmpty() ? null : sb.toString();
    }

    /**
     * 将 ES 的 FieldValue 安全转换为字符串，便于 Base64 编码游标。
     * 支持 Double、Long、String、Boolean 四种类型，兜底用 _get() 拿到原始值。
     */
    private String fieldValueToString(FieldValue fv) {
        if (fv.isDouble()) {
            return String.valueOf(fv.doubleValue());
        }
        if (fv.isLong()) {
            return String.valueOf(fv.longValue());
        }
        if (fv.isString()) {
            return fv.stringValue();
        }
        if (fv.isBoolean()) {
            return String.valueOf(fv.booleanValue());
        }

        return String.valueOf(fv._get());
    }

    /**
     * 从 ES 文档中安全提取字符串，null 保护。
     * ES 返回的 Map 中，字段可能不存在，所以所有提取都必须做 null 检查。
     */
    private String asString(Object o) {
        return o == null ? null : String.valueOf(o);
    }

    /**
     * 从 ES 文档中安全提取 Long，支持 Number 和 String 两种来源，容错解析。
     * ES JSON 反序列化到 Map 时，整数可能是 Integer 也可能是 Long，
     * 用 Number 统一处理，再转 Long，保证不出错。
     */
    private Long asLong(Object o) {
        if (o == null) {
            return null;
        }
        if (o instanceof Number n) {
            return n.longValue();
        }

        try {
            return Long.parseLong(String.valueOf(o));
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 从 ES 文档中安全提取 Boolean，支持 Boolean、Number、String 三种来源。
     * 容错处理：0/false → false，非 0/true → true。
     */
    private Boolean asBoolean(Object o) {
        switch (o) {
            case null -> {
                return null;
            }
            case Boolean b -> {
                return b;
            }
            case Number n -> {
                return n.intValue() != 0;
            }
            default -> {
            }
        }

        String s = String.valueOf(o).toLowerCase();
        if ("true".equals(s)) {
            return Boolean.TRUE;
        }
        if ("false".equals(s)) {
            return Boolean.FALSE;
        }
        return null;
    }

    /**
     * 从 ES 文档中安全提取 List<String>，支持两种格式：
     * 1. 原生 List：直接遍历转换为 List<String>
     * 2. JSON 数组字符串：如 "[\"Java\",\"Spring\"]"，解析拆分后返回
     *
     * 为什么需要处理第二种？因为 tags/img_urls 在 MySQL 存的是 JSON 字符串，
     * ES 写入时直接保存，读取时可能以字符串形式存在 Map 中。
     */
    private List<String> asStringList(Object o) {
        if (o == null) {
            return Collections.emptyList();
        }

        if (o instanceof List<?> l) {
            List<String> out = new ArrayList<>(l.size());
            for (Object e : l) {
                if (e != null) {
                    out.add(String.valueOf(e));
                }
            }
            return out;
        }

        String s = String.valueOf(o);
        if (s.startsWith("[") && s.endsWith("]")) {
            s = s.substring(1, s.length() - 1);
            if (s.isBlank()) {
                return Collections.emptyList();
            }

            String[] parts = s.split(",");
            List<String> out = new ArrayList<>();
            for (String p : parts) {
                String t = p.trim();
                if (t.startsWith("\"") && t.endsWith("\"")) {
                    t = t.substring(1, t.length() - 1);
                }

                if (!t.isEmpty()) {
                    out.add(t);
                }
            }
            return out;
        }
        return Collections.emptyList();
    }
}