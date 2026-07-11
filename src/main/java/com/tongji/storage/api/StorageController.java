package com.tongji.storage.api;

import com.tongji.common.exception.BusinessException;
import com.tongji.common.exception.ErrorCode;
import com.tongji.auth.token.JwtService;
import com.tongji.knowpost.mapper.KnowPostMapper;
import com.tongji.knowpost.model.KnowPost;
import com.tongji.storage.OssStorageService;
import com.tongji.storage.api.dto.StoragePresignRequest;
import com.tongji.storage.api.dto.StoragePresignResponse;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;

/**
 * StorageController 是文件上传的"签证官"——不直接接收文件，而是签发一个带签名的临时 URL，让前端绕过服务器直传 OSS。后端零带宽消耗，只负责权限校验。
 * 存储控制器：提供 OSS 直传预签名 URL，让前端不经过后端直接上传文件到阿里云 OSS。
 *
 * 面试自述：这个类解决的是文件上传的"流量绕行"问题。
 * 传统方案是前端把文件传给后端，后端再上传到 OSS——文件流经过后端服务器，
 * 10MB 的图片 × 100 个并发用户 = 1GB 带宽瞬间打满，后端服务器直接垮掉。
 *
 * 我这里的方案是"客户端直传"：
 * 1. 前端调 POST /api/v1/storage/presign，后端返回一个带签名的 PUT URL
 * 2. 前端直接用这个 URL 把文件 PUT 到 OSS，文件流不经过后端
 * 3. 后端不碰文件流，只负责签发 URL + 权限校验
 *
 * 这样做的好处：
 * - 后端零带宽消耗：文件流直接从前端到 OSS，不经过服务器
 * - 安全性不妥协：预签名 URL 有时效性（10 分钟），且后端校验了 postId 的归属权
 * - 上传体验好：前端拿到 URL 立刻就能传，不需要等后端转发
 *
 * 支持两种上传场景：
 * - knowpost_content：知文正文（Markdown/HTML/TXT/JSON），固定路径 posts/{postId}/content
 * - knowpost_image：知文配图，按日期分目录 posts/{postId}/images/{yyyyMMdd}/{随机8位}
 *   图片用日期分目录是为了防止单目录文件过多导致 OSS 性能下降
 *
 * 文件扩展名规整策略（normalizeExt）：
 * - 前端明确传了 ext → 直接用（补上前面的点）
 * - 前端没传 ext → 根据 contentType 自动推断，MIME 类型映射到文件扩展名
 */
@RestController
@RequestMapping("/api/v1/storage")
@Validated
@RequiredArgsConstructor
public class StorageController {

    private final OssStorageService ossStorageService;
    private final JwtService jwtService;
    private final KnowPostMapper knowPostMapper;

    /**
     * 获取 OSS 直传预签名 URL。
     *
     * 面试自述：前端调这个接口时，传 postId、上传场景（正文/图片）、文件扩展名和 MIME 类型。
     * 后端做三件事：
     * 1. 权限校验：这个 postId 必须是当前登录用户创建的，防止越权上传
     * 2. 生成 OSS 对象路径（objectKey）：正文固定路径，图片按日期分目录 + 随机后缀防冲突
     * 3. 签发预签名 URL：调用 OSS SDK 生成一个带签名的临时 PUT URL，有效期 10 分钟
     *
     * 返回给前端的 URL 类似于：
     * https://bucket.oss-cn-hangzhou.aliyuncs.com/posts/100/content.md
     *   ?Expires=1700000000&OSSAccessKeyId=xxx&Signature=yyy
     *
     * 前端拿到后直接 fetch(url, { method: 'PUT', body: file }) 就上传完成了。
     *
     * @param request 包含 postId、scene（场景）、ext（扩展名）、contentType（MIME 类型）
     * @param jwt     当前登录用户的 JWT，用于提取 userId 做权限校验
     * @return 预签名 URL + objectKey + 请求头 + 有效期
     */
    @PostMapping("/presign")
    public StoragePresignResponse presign(@Valid @RequestBody StoragePresignRequest request,
                                          @AuthenticationPrincipal Jwt jwt) {
        // 从 JWT 中提取当前登录用户 ID，用于后续权限校验
        long userId = jwtService.extractUserId(jwt);

        // 前端传的 postId 是 String 类型（URL 参数统一用 String），这里转 Long
        long postId;
        try {
            postId = Long.parseLong(request.postId());
        } catch (NumberFormatException e) {
            throw new BusinessException(ErrorCode.BAD_REQUEST, "postId 非法");
        }

        // 权限校验：确保 postId 对应的文章确实属于当前用户
        // 防止用户 A 拿到用户 B 的 postId 后越权上传文件覆盖 B 的内容
        KnowPost post = knowPostMapper.findById(postId);
        if (post == null || post.getCreatorId() == null || post.getCreatorId() != userId) {
            throw new BusinessException(ErrorCode.BAD_REQUEST, "草稿不存在或无权限");
        }

        String scene = request.scene();
        // 规整文件扩展名：优先用前端传的 ext，没有则根据 contentType 推断
        String ext = normalizeExt(request.ext(), request.contentType(), scene);
        // OSS 对象路径（objectKey），决定文件在 OSS 中的存储位置
        String objectKey;

        if ("knowpost_content".equals(scene)) {
            // 正文文件：固定路径，同一篇文章多次上传会覆盖旧文件
            // 路径格式：posts/{postId}/content.md
            objectKey = "posts/" + postId + "/content" + ext;
        } else if ("knowpost_image".equals(scene)) {
            // 图片文件：按日期分目录 + 8 位随机串，防止文件名冲突
            // 路径格式：posts/{postId}/images/20260627/a1b2c3d4.jpg
            // 为什么按日期分目录？OSS 单目录文件数过多会影响 ListObjects 性能
            String date = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneId.of("UTC")).format(Instant.now());
            String rand = UUID.randomUUID().toString().replaceAll("-", "").substring(0, 8);
            objectKey = "posts/" + postId + "/images/" + date + "/" + rand + ext;
        } else {
            throw new BusinessException(ErrorCode.BAD_REQUEST, "不支持的上传场景");
        }

        // 签发预签名 URL，有效期 10 分钟（600 秒）
        // 前端必须在 10 分钟内完成上传，超时 URL 失效
        int expiresIn = 600;
        // 调用 OSS SDK 生成带签名的 PUT URL
        String putUrl = ossStorageService.generatePresignedPutUrl(objectKey, request.contentType(), expiresIn);
        // 预签名 URL 只签了 PUT 权限，前端上传时必须在请求头中带上 Content-Type
        Map<String, String> headers = Map.of("Content-Type", request.contentType());
        return new StoragePresignResponse(objectKey, putUrl, headers, expiresIn);
    }

    /**
     * 规整文件扩展名：优先使用前端明确指定的扩展名，否则根据 MIME 类型推断。
     *
     * 为什么需要这个方法？前端传的 ext 可能不带点（如 "md"），
     * 也可能完全没传（依赖 contentType 推断）。这里统一处理，保证 objectKey 格式一致。
     *
     * 推断规则：根据上传场景（scene）分两套映射表
     * - 正文场景：MIME 类型 → 扩展名（text/markdown → .md）
     * - 图片场景：MIME 类型 → 扩展名（image/jpeg → .jpg）
     * 都无法匹配时，正文兜底 .bin，图片兜底 .img
     *
     * @param ext         前端指定的扩展名（可能为 null 或空）
     * @param contentType MIME 类型，如 "text/markdown"、"image/png"
     * @param scene       上传场景：knowpost_content 或 knowpost_image
     * @return 带点的扩展名，如 ".md"、".jpg"
     */
    private String normalizeExt(String ext, String contentType, String scene) {
        // 前端明确传了扩展名 → 直接用，补上前面的点（如果没带的话）
        if (ext != null && !ext.isBlank()) {
            return ext.startsWith(".") ? ext : "." + ext;
        }
        // 正文场景：根据 MIME 类型映射文件扩展名
        if ("knowpost_content".equals(scene)) {
            return switch (contentType) {
                case "text/markdown" -> ".md";
                case "text/html" -> ".html";
                case "text/plain" -> ".txt";
                case "application/json" -> ".json";
                default -> ".bin";   // 未知 MIME 类型兜底为 .bin
            };
        } else {
            // 图片场景：根据 MIME 类型映射图片扩展名
            return switch (contentType) {
                case "image/jpeg" -> ".jpg";
                case "image/png" -> ".png";
                case "image/webp" -> ".webp";
                default -> ".img";   // 未知图片格式兜底为 .img
            };
        }
    }
}