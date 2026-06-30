package com.tongji.llm.service.impl;

import com.tongji.llm.service.KnowPostDescriptionService;
import com.tongji.common.exception.BusinessException;
import com.tongji.common.exception.ErrorCode;
import lombok.RequiredArgsConstructor;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.deepseek.DeepSeekChatOptions;
import org.springframework.stereotype.Service;

import java.text.Normalizer;

@Service
@RequiredArgsConstructor
public class KnowPostDescriptionServiceImpl implements KnowPostDescriptionService {

    private final ChatClient chatClient;

    /**
     * 基于知文正文，调用大模型生成不超过 50 字的中文描述。
     *
     * 面试自述：这个方法的业务场景是——用户发布知文后，我们需要一个简洁的摘要展示在列表页。
     * 我选择用大模型来生成而不是简单截取正文前 50 字，因为模型能理解语义，生成的描述更有吸引力。
     * 整个流程是「入参校验 → 提示词拼接 → 模型调用 → 后处理清洗」。
     *
     * @param content 知文正文（Markdown 格式），不能为空
     * @return 不超过 50 个汉字的中文描述，已去除换行、多余标点
     * @throws BusinessException 正文为空时抛 BAD_REQUEST，模型调用失败时抛 INTERNAL_ERROR
     */
    public String generateDescription(String content) {
        // ========== 入参校验 ==========
        // 用 trim() 而非 isEmpty()，防止用户传入纯空格或全空白字符
        if (content == null || content.trim().isEmpty()) {
            throw new BusinessException(ErrorCode.BAD_REQUEST, "正文内容不能为空");
        }

        // ========== 提示词拼接 ==========
        // 系统提示词：限定角色为「中文文案编辑」，约束输出格式和长度
        // 关键指令：不输出解释或多段，只输出结果——防止模型自作主张加前缀如「这段文字描述了...」
        String system = "你是中文文案编辑。请基于用户提供的知文正文，生成一个中文描述，简洁有吸引力，且不超过50个汉字。不输出解释或多段，只输出结果。";

        // 用户消息：把正文嵌入 prompt，再次强调 50 字限制
        // 双重强调（system + user 都提 50 字）能显著提高模型遵守字数限制的概率
        String user = "正文如下：\n\n" + content + "\n\n请直接给出不超过50字的中文描述。";

        // ========== 模型调用 ==========
        try {
            String result = chatClient
                    .prompt()                                    // 构建一次对话请求
                    .system(system)                              // 设置系统角色
                    .user(user)                                  // 设置用户消息
                    .options(DeepSeekChatOptions.builder()
                            .model("deepseek-chat")              // 模型选型：DeepSeek 通用版
                            .temperature(0.8)                    // 温度 0.8：偏高，让描述更有创意和吸引力
                            .maxTokens(120)                      // 最大输出 token（50 汉字约 100-150 token，120 留余量）
                            .build())
                    .call()                                      // 同步调用，等待完整回答
                    .content();                                  // 提取回答文本
            // ========== 后处理清洗 ==========
            return postProcess(result);
        } catch (Exception e) {
            // 模型调用异常统一包装为业务异常，避免暴露底层技术细节给调用方
            throw new BusinessException(ErrorCode.INTERNAL_ERROR, "大模型调用失败: " + e.getMessage());
        }
    }

    /**
     * 对大模型返回的描述文本做后处理清洗。
     *
     * 面试自述：大模型的输出不可控——它可能返回多行文本、带各种引号、超过字数限制。
     * 这个方法就是最后一道防线，确保返回给前端的描述是干净、单行、不超过 50 字的。
     *
     * 处理步骤分四步：
     * 1、NFKC 规范化：把全角字符转半角，避免中文引号等特殊字符干扰后续正则；
     * 2、换行符归一化：把所有换行符（\r\n、\r、\n）统一替换为空格；
     * 3、标点清洗：去掉首尾的引号（中英文各种引号）和尾部的句号、感叹号等；
     * 4、按 code point 截断：精确截断至 50 字，避免 emoji 等辅助平面字符被截成乱码。
     *
     * @param text 大模型原始返回文本，可能为 null
     * @return 清洗后的纯文本描述，不超过 50 字
     */
    private String postProcess(String text) {
        // 空值兜底：大模型返回 null 时（极端情况），返回空字符串避免 NPE
        if (text == null) {
            return "";
        }

//        String.length() 和 substring() 按 Java 的 char 单元操作，遇到 emoji 会截出乱码。
//        这两段代码用 codePointAt + charCount 按 Unicode 字符逐字截取，保证截出来的 50 个字每一个都是完整的，不会出现 ``。
        // NFKC 规范化：兼容全角字母、数字、标点，统一转为半角，方便后续正则处理
        // 换行符归一化：\r\n → 空格，\r → 空格，\n → 空格，确保最终结果是单行文本
        // 连续空白压缩：多个空格/制表符合并为一个空格
        // trim：去掉首尾空白
        String t = Normalizer.normalize(text, Normalizer.Form.NFKC)
                .replaceAll("\r\n|\r|\n", " ")
                .replaceAll("\\s+", " ")
                .trim();

        // 去掉首尾的引号（中英文各种引号），防止模型输出如 "xxx" 或「xxx」
        // 去掉尾部的句末标点（句号、感叹号、问号、分号、顿号），让描述更干净
        t = t.replaceAll("^[\"'“”'']+|[\"'“”'']+$", "")
             .replaceAll("[。!！?？；;、]+$", "");

        // 按 code point 精确截断至 50 字
        // 用 codePointCount 而非 String.length()，因为 emoji 和某些中文字符
        // 在 Java 中占 2 个 char（代理对），用 length() 会导致计数偏大、截断不准
        int limit = 50;
        int count = t.codePointCount(0, t.length());
        if (count <= limit) {
            return t;  // 不超过 50 字，直接返回
        }

        // 逐 code point 遍历，精确截取前 50 个字符
        StringBuilder sb = new StringBuilder();
        int i = 0, added = 0;
        while (i < t.length() && added < limit) {
            int cp = t.codePointAt(i);               // 获取当前位置的 Unicode code point
            sb.appendCodePoint(cp);                  // 追加到结果中
            i += Character.charCount(cp);            // 跳过代理对（1 或 2 个 char）
            added++;                                  // 已添加字符数 +1
        }
        return sb.toString();
    }
}