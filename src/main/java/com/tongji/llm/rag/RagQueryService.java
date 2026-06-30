package com.tongji.llm.rag;

import lombok.RequiredArgsConstructor;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.deepseek.DeepSeekChatOptions;
import org.springframework.ai.document.Document;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;

/**
 * 面试自述：这个 RagQueryService 是我负责的 RAG 问答查询服务，它是用户提问和
 * 大模型回答之间的桥梁，负责把「检索到的上下文」和「用户问题」拼成提示词喂给 DeepSeek，
 * 然后以 SSE 流式的方式把回答逐字推给前端，用户不用等完整答案生成就能看到内容。
 *
 * 整个请求链路我拆成了四步，都在 streamAnswerFlux() 这一个方法里串联：
 *
 * 第一步，索引保障。调用 indexService.ensureIndexed(postId)，确保当前文章
 * 已经建好了向量索引。这里有个设计细节：ensureIndexed 内部做了指纹判重，
 * 如果文章内容没变就直接跳过，不会每次提问都重建索引，避免浪费 Embedding 算力。
 *
 * 第二步，语义检索。调用 searchContexts() 向 Elasticsearch 向量库发起语义检索。
 * 初版代码有一个严重的设计缺陷——我先把向量库的数据全量拉到 Java 内存里，
 * 再用 for 循环按 postId 手动过滤。这在数据量小的时候看不出问题，但向量库里
 * 有全站切片时，语义最相似的几条可能全部来自别人的文章，过滤后返回空列表，
 * 大模型拿不到任何上下文，这是典型的「召回截断」灾难。
 * 修复方案是三个改动：一是把 postId 过滤条件通过 filterExpression 直接下推给
 * 向量数据库，让数据库在检索阶段就限定范围，返回的每一条都 100% 属于当前文章，
 * 彻底消除召回截断风险；二是去掉魔法数字 20，既然过滤已下推，不需要宽召回兜底，
 * topK 就是 topK；三是增加 similarityThreshold(0.7)，过滤掉相似度低于 0.7 的切片，
 * 防止大模型拿到垃圾上下文后产生幻觉、胡编乱造。
 *
 * 第三步，提示词拼接。我用 String.join 把多个切片文本拼成一段上下文，
 * 分隔符是 "\n\n---\n\n"，让大模型能区分不同切片。系统提示词我限定它是
 * 「中文知识助手，只能依据提供的上下文回答」，这是一个防幻觉的约束——
 * 即使大模型知道答案，如果上下文中没有，它也应该说「不确定」。
 *
 * 第四步，流式输出。通过 ChatClient 的 stream().content() 返回 Flux&lt;String&gt;，
 * 底层是 WebFlux 的 SSE 流。我设置 temperature 为 0.2，这是偏保守的温度值，
 * 让回答更稳健、少发散，适合知识问答场景；maxTokens 由调用方传入，灵活控制。
 *
 * 从设计模式角度看，searchContexts() 内部把「检索 + 过滤 + 阈值兜底」封装成
 * 一个独立方法，streamAnswerFlux() 则定义了「保障 → 检索 → 拼接 → 输出」的
 * 四步骨架，这是一种模板方法模式的思想。
 *
 * 如果要说改进方向，目前提示词拼接是纯字符串拼接，当上下文很长时可能超出模型的
 * 上下文窗口。后续可以做一个 token 估算器，在拼接前预估总 token 数，超限时
 * 自动截断或做上下文压缩，避免请求被模型拒绝。
 */
@Service
@RequiredArgsConstructor
public class RagQueryService {
    // 向量检索接口（Elasticsearch 向量库封装）
    private final VectorStore vectorStore;
    // 大模型对话客户端（在 LlmConfig 中通过 @Qualifier 绑定 deepSeekChatModel）
    private final ChatClient chatClient;
    // 索引服务：确保帖子在问答前已建立/更新索引
    private final RagIndexService indexService;

    /**
     * RAG 问答的主入口，以 SSE 流式方式返回大模型的回答内容。
     *
     * 面试自述：这个方法是我整个 RAG 问答链路的编排层，把「索引保障 → 语义检索 →
     * 提示词拼接 → 流式输出」四步串联成一个完整的问答流程。前端拿到的是 Flux&lt;String&gt;，
     * 底层走 WebFlux 的 SSE 协议，用户能实时看到逐字输出的效果，体验很好。
     *
     * @param postId    当前文章 ID，用于检索该文章内的相关切片作为上下文
     * @param question  用户输入的自然语言问题
     * @param topK      检索的上下文切片数量，兜底至少为 1
     * @param maxTokens 大模型输出的最大 token 数，防止回答过长、成本失控
     * @return Flux&lt;String&gt; 流式回答，每段是一个 SSE 事件
     */
    public Flux<String> streamAnswerFlux(long postId, String question, int topK, int maxTokens) {
        // ========== 第一步：索引保障 ==========
        // 触发索引的懒加载检查：如果文章尚未索引或内容已变更，则先重建索引再检索
        // ensureIndexed 内部做了 SHA256 指纹判重，内容没变就直接跳过，不会浪费 Embedding 算力
        indexService.ensureIndexed(postId);

        // ========== 第二步：语义检索 ==========
        // 向 Elasticsearch 向量库发起语义检索，拿到属于当前文章、与问题最相关的切片
        // Math.max(1, topK) 兜底：即使调用方传了 0 或负数，也至少检索 1 条
        List<String> contexts = searchContexts(String.valueOf(postId), question, Math.max(1, topK));

        // ========== 第三步：提示词拼接 ==========
        // 用 "\n\n---\n\n" 作为分隔符，把多个切片文本拼成一段上下文
        // 分隔符的作用是让大模型在阅读时能区分不同切片的边界，避免把两段内容混为一谈
        String context = String.join("\n\n---\n\n", contexts);

        // 系统提示词：限定角色为「中文知识助手」，并约束只能依据上下文回答
        String system = "你是中文知识助手。只能依据提供的知文上下文回答；无法确定的请说明不确定。";

        // 用户消息：把问题和上下文拼接成一个完整的 prompt
        // 结构是「问题 + 上下文 + 指令」，大模型会按顺序理解，先看问题再找相关上下文
        String user = "问题：" + question + "\n\n上下文如下（可能不完整）：\n" + context + "\n\n请基于以上上下文作答。";

        // ========== 第四步：流式输出 ==========
        return chatClient
                .prompt()                                        // 构建一次对话请求
                .system(system)                                  // 设置系统角色和约束
                .user(user)                                      // 设置用户消息（含问题和上下文）
                .options(DeepSeekChatOptions.builder()
                        .model("deepseek-v4-flash")               // 模型选型：DeepSeek 快版，成本低、速度快
                        .temperature(0.2)                         // 温度 0.2：偏保守，输出更确定、少发散
                        .maxTokens(maxTokens)                     // 最大输出 token 数，由调用方控制
                        .build())
                .stream()                                        // 开启 SSE 流式输出，不等待完整回答
                .content();                                      // 提取回答内容，返回 Flux<String>
    }

    /**
     * 语义检索上下文，是整个 RAG 链路中承上启下的核心检索方法。
     *
     * 面试自述：这个方法我经历过一次重要的重构。初版代码是先把向量库数据拉到 Java 内存，
     * 再用 for 循环按 postId 手动过滤——这在数据量小时没问题，但向量库里有全站切片时，
     * 语义最相似的几条可能全部来自别人的文章，过滤后返回空列表，这就是「召回截断」灾难。
     *
     * 修复后的方案做了三个改动：
     * 1、filterExpression 下推过滤：把 postId 条件直接传给向量数据库，检索阶段就限定范围，
     *    返回的每条结果 100% 属于当前文章，彻底消除召回截断风险；
     * 2、去掉魔法数字 20：既然过滤已下推，不需要宽召回兜底，topK 就是要几条就检索几条；
     * 3、similarityThreshold 阈值兜底：低于 0.7 的切片直接丢弃，防止垃圾上下文导致大模型幻觉。
     *
     * @param postId 当前文章 ID，通过 filterExpression 下推给向量数据库做精确过滤
     * @param query  用户输入的问题，作为向量检索的查询文本
     * @param topK   需要返回的上下文切片数量
     * @return 属于当前文章、且与问题相似度 >= 0.7 的切片文本列表
     */
    private List<String> searchContexts(String postId, String query, int topK) {
        // 构建向量检索请求，把过滤条件和阈值都下推给数据库
        // filterExpression 是 Spring AI 提供的元数据过滤 DSL，底层会翻译成 ES 的 query filter
        // similarityThreshold 是客户端后处理，Spring AI 在拿到结果后会自动过滤掉低分切片
        SearchRequest request = SearchRequest.builder()
                .query(query)                                              // 用户问题，由 Embedding 模型转成向量
                .topK(topK)                                                // 需要返回的切片数量
                .similarityThreshold(0.7)                                  // 相似度及格线，低于 0.7 视为无关内容
                .filterExpression("postId == '" + postId + "'")            // 将 postId 过滤下推到向量数据库
                .build();

        // 发起语义检索，此时返回的每条切片都已通过 postId 过滤和相似度阈值筛选
        List<Document> docs = vectorStore.similaritySearch(request);

        // 预分配容量：用 docs.size() 而非 topK 作为初始容量，避免扩容开销
        // 因为经过阈值过滤后，实际返回数量可能小于 topK
        List<String> out = new ArrayList<>(docs.size());

        for (Document d : docs) {
            String txt = d.getText();
            // 最后一道防线：过滤空文本或纯空白文本，防止无效上下文污染提示词
            // 用 trim() 而非 isEmpty()，因为有些切片可能只有换行符和空格
            if (txt != null && !txt.trim().isEmpty()) {
                out.add(txt);
            }
        }

        return out;
    }
}