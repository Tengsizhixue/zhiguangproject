package com.tongji.llm.rag;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.tongji.knowpost.mapper.KnowPostMapper;
import com.tongji.knowpost.model.KnowPostDetailRow;
import com.tongji.config.EsProperties;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.document.Document;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;

import java.util.*;

/**
 * 面试自述：这个 RagIndexService 是我负责的 RAG 检索增强生成系统的索引构建服务。
 *
 * 它的核心职责很简单：把用户发布的公开知识文章自动转成向量索引，存入 Elasticsearch。
 * 这样当用户向 AI 提问时，我就能从向量库中检索出最相关的文章片段作为上下文，
 * 喂给大模型，生成更准确的回答。
 *
 * 整个入口方法是 reindexSinglePost()，我把它拆成了五个步骤：
 *
 * 第一步，前置校验。我从数据库查出文章，然后做三道检查——文章是否存在？
 * 是否已发布且公开？内容地址是否有效？任何一道不通过，我直接返回 0，
 * 不浪费后续资源。
 *
 * 第二步，指纹判重。文章可能被反复触发索引，
 * 但我不能每次都重建。所以我用 SHA256 和 ETag 双重指纹做幂等判断——
 * 先去 ES 里搜一条该文章已有的旧切片，取出上次索引时存的指纹，
 * 跟数据库当前的指纹做对比，一致就跳过，避免重复调用 Embedding 模型这种重操作。
 * 优先级上 SHA256 大于 ETag，而且就算 ES 挂了我也会 catch 住异常返回 false，
 * 我的原则是宁可多建一次，也不能漏建。
 *
 * 第三步，拉取加两级切片。我先通过 HTTP 从对象存储拉取 Markdown 原文，
 * 然后做两级切片——第一级按 # 标题行切段落，保证每个段落在语义上是完整的，
 * 不会把一个标题下面的内容撕成两半；第二级对超长段落做固定 800 字符的滑动窗口切片，
 * 相邻切片之间保留 100 字符的重叠，防止关键信息正好卡在切分边界上导致检索时漏掉。
 *
 * 第四步，先删后写。在写入新切片之前，我用 ES 的 delete-by-query 按 postId
 * 把该文章的所有旧切片先删干净，然后才写入新切片。这样保证新旧数据不混杂，
 * 无论触发多少次重建，最终 ES 里只有一套最新的切片，实现了幂等 upsert。
 *
 * 第五步，批量写入。我把每个切片封装成 Spring AI 的 Document 对象，
 * content 放切片文本给 Embedding 模型做向量化，
 * metadata 放 postId、title、position 这些业务字段给 ES 做精确过滤和排序，
 * 最后调用 VectorStore.add() 批量写入。
 *
 * 如果面试官问我这段代码有什么技术亮点，我会重点说两个：
 *
 * 一个是刚才提到的指纹幂等机制。通过 SHA256 和 ETag 双重校验，
 * 我避免了不必要的重建，既节省了 Embedding 调用的成本，
 * 又保证了索引的最终一致性，而且异常时走降级兜底策略，系统的鲁棒性更好。
 *
 * 另一个是我使用了 Spring AI 的 VectorStore 接口，而不是直接耦合 Elasticsearch。
 * 这意味着将来如果要把向量数据库从 ES 换成 Milvus 或者 Pinecone，
 * 我只需要换一个实现类，业务代码完全不用动，扩展性非常好。
 *
 * 从设计模式的角度来看，reindexSinglePost() 定义了一个固定的五步骨架，
 * 但每一步的具体实现是独立的 private 方法，这其实是一种模板方法模式；
 * 而两级切片策略则是策略模式的体现，未来我可以轻松替换成基于语义的切分方案。
 *
 * 如果要说改进空间，目前索引是同步触发的，长文章拉取内容加 Embedding 调用
 * 可能要几秒钟，后续我会考虑改成 Kafka 异步消费，不阻塞用户请求。
 *
 * 总结一下：我用「指纹幂等 + 两级切片 + 先删后写」这三个核心机制，
 * 保证了 RAG 索引的正确性和效率，这个类是整个 AI 问答系统最底层的数据库底座。
 */
@Service
@RequiredArgsConstructor
public class RagIndexService {
    private static final Logger log = LoggerFactory.getLogger(RagIndexService.class);
    // 向量库封装（Elasticsearch VectorStore），负责写入/检索向量
    private final VectorStore vectorStore;
    // 数据访问：根据 postId 查询知文详情（含 contentUrl、指纹等）
    private final KnowPostMapper knowPostMapper;
    // 拉取 Markdown 正文内容
    private final RestTemplate http = new RestTemplate();
    // 直接使用 ES 客户端做指纹判断和删除旧切片
    private final ElasticsearchClient es;
    // ES 相关配置（索引名等）
    private final EsProperties esProps;

    public void ensureIndexed(long postId) {
        // 当前策略：在问答前直接尝试重建（指纹未变化时会跳过）
        reindexSinglePost(postId);
    }

    /**
     * 对单篇知文重新构建 RAG 索引（幂等）。
     * 完整流程：
     *
     *   从数据库查询知文详情
     *   前置校验：状态、可见性、内容地址、指纹
     *   通过 HTTP 拉取 Markdown 正文
     *   将正文切分为多个文本块（chunk）
     *   删除该知文在向量库中的旧切片
     *   为每个切片构建 Document 并写入向量库
     *
     *
     * @param postId 知文 ID
     * @return 本次写入的切片数量，如果跳过或失败则返回 0
     */
    public int reindexSinglePost(long postId) {
        // 1. 从数据库查询知文详情：根据 postId 获取知文的完整信息
        //    - 包含标题、状态、可见性、内容地址、指纹（SHA256/ETag）等
        //    - 这些信息用于后续的状态检查和索引构建
        KnowPostDetailRow row = knowPostMapper.findDetailById(postId);

        // 2. 检查知文是否存在：如果数据库中没有该记录，记录警告并返回
        //    - 可能原因：postId 无效、知文已被删除
        if (row == null) {
            log.warn("Post {} not found", postId);
            return 0;
        }

        // 3. 检查知文状态和可见性：只有"已发布"且"公开"的知文才能被索引
        //    - status 必须为 "published"（已发布），排除草稿、审核中的知文
        //    - visible 必须为 "public"（公开），排除私有、仅粉丝可见的知文
        //    - 不满足条件的知文不会被索引到向量库中，RAG 检索时也无法搜到
        if (!"published".equalsIgnoreCase(row.getStatus()) || !"public".equalsIgnoreCase(row.getVisible())) {
            log.warn("Post {} is not public/published, skip indexing", postId);
            return 0;
        }

        // 4. 检查内容地址是否存在：contentUrl 是知文 Markdown 内容的存储地址
        //    - 如果 contentUrl 为空，无法获取正文内容，无法构建索引
        //    - 记录警告日志，方便运维排查
        if (!StringUtils.hasText(row.getContentUrl())) {
            log.warn("Post {} missing contentUrl or not found", postId);
            return 0;
        }

        // 5. 指纹检测：判断知文内容是否发生变化，避免重复索引
        //    - currentSha：内容文件的 SHA256 哈希值，内容变化时哈希值也会变化
        //    - currentEtag：HTTP ETag 值，用于检测文件是否被修改
        //    - 如果指纹与上次索引时一致，说明内容没有变化，跳过重建
        //    - 这是幂等设计的关键：相同内容只索引一次
        String currentSha = row.getContentSha256();
        String currentEtag = row.getContentEtag();
        if (isUpToDate(postId, currentSha, currentEtag)) {
            log.info("Post {} already indexed with same fingerprint, skip", postId);
            return 0;
        }

        // 6. 抓取 Markdown 正文：通过 HTTP GET 请求从 contentUrl 获取知文内容
        //    - 使用 RestTemplate 发起 HTTP 请求
        //    - 获取到的内容是 Markdown 格式的原始文本
        String text = fetchContent(row.getContentUrl());

        // 7. 检查正文内容是否为空：如果抓取到的内容为空，无法构建索引
        //    - 可能原因：URL 失效、文件被删除、网络异常
        if (!StringUtils.hasText(text)) {
            log.warn("Post {} content empty", postId);
            return 0;
        }

        // 8. 文本切片：将完整的 Markdown 文本切分为多个小段（chunk）
        //    - 先按 Markdown 标题（## 等）进行段落分割，保持语义完整性
        //    - 再对每个段落做固定长度的滑动窗口切片（带重叠），确保上下文不丢失
        //    - 每个切片后续会被单独向量化，检索时按切片匹配
        List<String> chunks = chunkMarkdown(text);

        // 9. 删除旧切片：先删除该知文在向量库中的所有旧切片
        //    - 这是幂等 upsert 的关键：先删后写，保证新旧数据不冲突
        //    - 使用 delete-by-query 按 postId 删除 ES 中对应的所有文档
        deleteExistingChunks(postId);

        // 10. 组装 Document 对象：为每个切片构建 Spring AI 的 Document 对象
        //     - Document 包含两部分：文本内容（chunk）和元数据（metadata）
        //     - 文本内容：切片后的知文片段
        //     - 元数据：用于检索时的过滤和排序
        List<Document> docs = new ArrayList<>(chunks.size());
        for (int i = 0; i < chunks.size(); i++) {
            // 10.1 生成切片唯一标识：postId + 序号，如 "42#0"、"42#1"
            String cid = postId + "#" + i;

            // 10.2 构建元数据 Map：存储业务信息，用于检索过滤
            //meta 不适合放大文本：meta 里的字段是给 ES 做 term 精确匹配、排序用的
            Map<String, Object> meta = new HashMap<>();
            meta.put("postId", String.valueOf(postId));    // 知文 ID，用于按文章过滤
            meta.put("chunkId", cid);                      // 切片唯一标识
            meta.put("position", i);                       // 切片在原文中的位置，用于排序
            meta.put("contentEtag", currentEtag);           // 内容 ETag，用于指纹比对
            meta.put("contentSha256", currentSha);          // 内容 SHA256，用于指纹比对
            meta.put("contentUrl", row.getContentUrl());    // 内容源地址，用于溯源
            meta.put("title", row.getTitle());              // 知文标题，用于展示

            // 10.3 将切片内容和元数据封装为 Document 对象
            //Embedding 模型只对 Document.content 生成向量，不会读 meta，所以 chunks 必须放在第一个参数里。
            docs.add(new Document(chunks.get(i), meta));
        }

        // 11. 批量写入向量库：将构建好的 Document 列表写入 Elasticsearch VectorStore
        //     - Spring AI VectorStore 会自动调用 Embedding 模型将文本转为向量
        //     - 向量和元数据一起存入 ES，支持后续的相似度检索
        try {
            vectorStore.add(docs);
        } catch (Exception e) {
            // 11.1 写入失败时记录错误日志并返回 0
            //     - 可能原因：ES 不可用、向量模型调用失败、网络超时
            log.error("VectorStore add failed: {}", e.getMessage());
            return 0;
        }

        // 12. 返回本次写入的切片数量：调用方可以据此判断索引是否成功
        //     - 返回 0 表示没有切片被写入（跳过或失败）
        //     - 返回大于 0 表示成功写入的切片数量
        return docs.size();
    }

    /**
     * 指纹判断是否需要重建：
     * - 以 postId 查询任意一条已索引文档的 metadata
     * - 优先比较 SHA256，其次比较 ETag；一致则视为无需重建
     */
    private boolean isUpToDate(long postId, String currentSha, String currentEtag) {
        try {
            // 1. 检查索引名是否配置：如果 ES 索引名未配置，无法进行指纹比对
            //    - 直接返回 false，表示"无法判断是否最新，需要重建"
            //    - 这是一种安全策略：宁可多重建一次，也不能漏掉需要更新的内容
            if (!StringUtils.hasText(esProps.getIndex())) {
                return false;
            }

            // 2. 查询 ES 中该知文已索引的文档：从向量库中搜索该知文的任意一条切片
            //    - 使用 term 精确查询 metadata.postId 字段
            //    - 只取 1 条结果（size=1），因为只需检查指纹，不需要全部切片
            //    - 每条切片都存储了相同的 contentSha256 和 contentEtag 元数据
            SearchResponse<Map> resp = es.search(
                    // 【第一层：发起搜索请求】
                    s -> s
                            .index(esProps.getIndex())          // 设置：去哪个“数据库表”搜索
                            .size(1)                            // 设置：只要 1 条结果
                            .query(                             // 设置：搜索条件
                                    // 【第二层：构造查询条件】
                                    q -> q.term(                    // 选择“精确匹配”查询类型
                                            // 【第三层：构造精确匹配的具体规则】
                                            t -> t
                                                    .field("metadata.postId") // 条件1：字段名
                                                    .value(                   // 条件2：字段值
                                                            // 【第四层：构造值的类型】
                                                            v -> v.stringValue(String.valueOf(postId)) // 明确指定值为 String 类型
                                                    )
                                    )
                            ),
                    // 第二个参数：期望返回的数据格式（Map 表示转为 Key-Value 结构）
                    Map.class
            );

            // 3. 获取搜索结果中的命中记录
            List<Hit<Map>> hits = resp.hits().hits();

            // 4. 检查是否有命中结果：如果 ES 中没有该知文的任何切片
            //    - 说明该知文从未被索引过，需要重建
            //    - 返回 false，让调用方执行索引
            if (hits == null || hits.isEmpty()) return false;

            // 5. 提取第一条命中的文档内容（source）：ES 中存储的原始 JSON 数据
            //    - source 包含：切片文本（content）和元数据（metadata）
            Map source = hits.getFirst().source();

            // 6. 检查 source 是否为空：如果文档存在但内容为空（异常情况）
            //    - 返回 false，视为需要重建
            if (source == null) return false;

            // 7. 提取 metadata 对象：从 source 中获取元数据部分
            //    - metadata 是一个嵌套的 Map，包含 postId、chunkId、contentSha256、contentEtag 等字段
            Object metaObj = source.get("metadata");

            // 8. 类型检查：确保 metadata 是 Map 类型
            //    - 如果类型不对（比如是 null 或 String），说明数据结构异常，返回 false
            //    - 使用 Java 16 的模式匹配：instanceof + 直接赋值给变量 meta
            if (!(metaObj instanceof Map<?, ?> meta)) return false;

            // 9. 提取上次索引时记录的指纹值：从 metadata 中获取存储的 SHA256 和 ETag
            //    - asString() 方法安全地将 Object 转为 String，null 时返回 null
            String indexedSha = asString(meta.get("contentSha256"));  // 上次索引时的 SHA256
            String indexedEtag = asString(meta.get("contentEtag"));    // 上次索引时的 ETag

            // 10. 指纹比对：优先使用 SHA256，其次使用 ETag
            //     SHA256 优先级更高，因为它是内容的完整哈希，更可靠
            if (StringUtils.hasText(currentSha) && StringUtils.hasText(indexedSha)) {
                // 10.1 如果数据库和 ES 中都有 SHA256，直接比较
                //     - 相等：内容没变，返回 true（跳过索引）
                //     - 不等：内容变了，返回 false（需要重建索引）
                return Objects.equals(currentSha, indexedSha);
            }

            // 10.2 降级方案：如果 SHA256 不可用，使用 ETag 比较
            if (StringUtils.hasText(currentEtag) && StringUtils.hasText(indexedEtag)) {
                // ETag 相同：内容大概率没变，返回 true
                // ETag 不同：内容变了，返回 false
                return Objects.equals(currentEtag, indexedEtag);
            }

            // 10.3 两种指纹都不可用：无法判断，保守策略，返回 false（需要重建）
            return false;

        } catch (Exception e) {
            // 11. 异常处理：指纹检查过程中出现任何异常（ES 不可用、网络超时等）
            //     - 不抛出异常，记录警告日志
            //     - 返回 false，让调用方继续执行索引（保守策略：宁可多索引，不能漏掉）
            //     - 这样即使 ES 暂时不可用，系统也能降级运行
            log.warn("Fingerprint check failed for post {}: {}", postId, e.getMessage());
            return false;
        }
    }

    /**
     * 删除旧切片：按 metadata.postId 精确删除，确保 upsert 幂等。
     * <p>
     * 在重新索引一篇文章之前，必须先将该文章在 ES 中已有的旧切片全部清除，
     * 避免新旧数据混杂导致检索结果中出现重复或过时的内容。
     * 采用"先删后写"策略，保证每次索引操作的结果都是确定且一致的。
     *
     * @param postId 文章 ID，对应 ES 中 metadata.postId 字段的值
     */
    private void deleteExistingChunks(long postId) {
        try {
            // 检查 ES 索引名是否已配置，未配置则跳过删除（可能处于测试环境或索引未初始化）
            if (!StringUtils.hasText(esProps.getIndex())) return;
            // 按条件删除：删除 ES 中所有 metadata.postId 等于当前 postId 的文档
            // term 查询是精确匹配，不会做分词处理，确保只删除属于该文章的所有 chunk
            es.deleteByQuery(d -> d
                    .index(esProps.getIndex())       // 指定目标 ES 索引
                    .query(q -> q.term(t -> t        // 构造 term（精确匹配）查询
                            .field("metadata.postId")           // 匹配字段：元数据中的文章 ID
                            .value(v -> v.stringValue(String.valueOf(postId))))));  // 匹配值：将 postId 转为字符串
        } catch (Exception e) {
            // 删除失败时仅记录警告日志，不向上抛出异常
            // 原因：删除失败不影响后续索引写入（最坏情况是产生重复 chunk，但不会丢数据）
            log.warn("Delete old chunks failed for post {}: {}", postId, e.getMessage());
        }
    }

    private static String asString(Object o) {
        // 统一处理 null → String 的转换
        return o == null ? null : String.valueOf(o);
    }

    /**
     * 拉取正文内容（Markdown 文本）。
     */
    private String fetchContent(String url) {
        try {
            return http.getForObject(url, String.class);
        } catch (Exception e) {
            log.error("Fetch content failed: {}", e.getMessage());
            return null;
        }
    }

    /**
     * 按 Markdown 标题切段，再交由固定长度切片策略处理。
     * <p>
     * 核心思路：
     * <ol>
     *   <li>将原始 Markdown 文本按行拆分，逐行遍历；</li>
     *   <li>以 "#" 开头的行作为段落边界（即 Markdown 标题），遇到新标题时收束上一段；</li>
     *   <li>非标题行持续追加到当前缓冲区，维持段落内语义完整性；</li>
     *   <li>切段完成后，将每个段落交给 {@link #getChunks(List)} 做固定长度切片。</li>
     * </ol>
     * <p>
     * 注意：该切分策略仅依赖一级标题标记（#），不区分标题层级（##, ### 等），
     * 所有标题行均视为段落分隔符。这样做的目的是在保证语义完整的前提下，
     * 尽可能减少段落数量，避免过度碎片化影响检索召回效果。
     *
     * @param text 原始 Markdown 格式文本
     * @return 切片后的文本片段列表，每个片段长度不超过 800 字符
     */
    private List<String> chunkMarkdown(String text) {
        // 用于存放按标题切分后的段落
        List<String> paras = new ArrayList<>();
        // 按换行符拆分文本为一行行的文本数组
        String[] lines = text.split("\r?\n");
        // 缓冲区：逐行拼接当前段落内容
        StringBuilder buf = new StringBuilder();
        for (String line : lines) {
            // 判断当前行是否为 Markdown 标题行（以 "#" 开头）
            boolean isHeader = line.startsWith("#");
            // 遇到新标题且缓冲区非空时，说明上一段落已完整，将其保存并清空缓冲区
            if (isHeader && !buf.isEmpty()) {
                paras.add(buf.toString());
                buf.setLength(0); // 清空 buf，准备接收下一个段落的内容
            }
            // 追加当前行到缓冲区，如果上一段落已完整，当前行作为新段落的开头
            buf.append(line).append('\n');
        }
        // 遍历结束后，将最后一段残留内容加入段落列表
        if (!buf.isEmpty()) paras.add(buf.toString());

        // 将按标题切好的段落，进一步按固定长度策略切片
        return getChunks(paras);
    }

    /**
     * 将段落列表按固定长度切分为更小的文本块（chunk），以便后续向量化与检索。
     *
     * 切分策略：
     * - 每个 chunk 最大长度为 800 字符（约 400 个中文汉字），保证向量模型的输入不超限。
     * - 相邻 chunk 之间保留 100 字符的重叠区域，避免关键信息在切分边界处断裂，
     *   确保检索时上下文的语义连续性。
     * - 对于长度 ≤ 800 的段落，直接作为一个完整的 chunk，不做切分。
     *
     * 使用场景：本方法用于 RAG（检索增强生成）流程中的文档索引阶段，将原始文档
     * 拆分为适合向量检索的细粒度文本片段。
     *
     * @param paras 原始段落列表，每个元素为一段完整的文本段落
     * @return 切分后的文本块列表，每个文本块长度不超过 800 字符
     */
    private static List<String> getChunks(List<String> paras) {
        // 存储所有切分后的文本块
        List<String> chunks = new ArrayList<>();

        // 遍历每个段落，根据长度决定是否切分
        for (String p : paras) {
            if (p.length() <= 800) {
                // 段落长度未超过限制，直接作为独立 chunk 加入结果集
                chunks.add(p);
            } else {
                // 段落过长，需按固定长度切分，相邻片段之间有 100 字符重叠
                int start = 0; // 当前截取片段的起始位置
                while (start < p.length()) {
                    // 计算当前片段的结束位置，不超过段落总长度
                    int end = Math.min(start + 800, p.length());
                    // substring 截取 [start, end) 范围的子串作为一个 chunk
                    chunks.add(p.substring(start, end));

                    // 如果已经到达段落末尾，结束循环
                    if (end >= p.length()) break;

                    // 下一片段的起始位置 = 当前结束位置 - 重叠量（100 字符）
                    // 使用 Math.max 确保 start 至少前进 1，避免死循环
                    start = Math.max(end - 100, start + 1);
                }
            }
        }
        return chunks;
    }
}