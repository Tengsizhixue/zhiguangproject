package com.tongji.auth.token;

import lombok.RequiredArgsConstructor;
import com.tongji.auth.config.AuthProperties;
import com.tongji.user.domain.User;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtClaimsSet;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtEncoder;
import org.springframework.security.oauth2.jwt.JwtEncoderParameters;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;
import java.util.UUID;

/**
 * JWT 令牌服务。
 * <p>
 * 功能：签发 Access/Refresh Token（RS256），解码 JWT，提取用户 ID、令牌类型与令牌 ID。
 * 声明：
 * - `token_type`：标识 access 或 refresh；
 * - `uid`：用户 ID；
 * - `jti`：令牌 ID（用作 Refresh Token 的白名单键）。
 * 过期时间：来自 `AuthProperties.jwt.accessTokenTtl` 与 `refreshTokenTtl`。
 */
@Service
@RequiredArgsConstructor
public class JwtService {

    private static final String CLAIM_TOKEN_TYPE = "token_type";
    private static final String CLAIM_USER_ID = "uid";

    private final JwtEncoder jwtEncoder;
    private final JwtDecoder jwtDecoder;
    private final AuthProperties properties;
    private final Clock clock = Clock.systemUTC();

    /**
     * 为指定用户签发一对 Access/Refresh Token。
     * <p>
     * 令牌类型通过 `token_type` 声明区分；Refresh Token 的 `jti` 用于白名单存储与撤销。
     * 过期时间取自配置 `AuthProperties.jwt`。
     *
     * @param user 用户实体。
     * @return 令牌对与对应过期时间及刷新令牌 ID。
     */
    public TokenPair issueTokenPair(User user) {
        // 1. 生成刷新令牌的唯一标识符
        //    - 使用UUID生成全局唯一的标识符
        //    - 这个ID将用于刷新令牌的追踪和撤销管理
        //    - 存储在Redis白名单中，用于验证刷新令牌的有效性
        String refreshTokenId = UUID.randomUUID().toString();

        // 2. 获取当前时间作为令牌签发时间
        //    - 使用clock而不是System.currentTimeMillis()，便于单元测试
        //    - Instant表示UTC时间戳，避免时区问题
        //    - 所有令牌都使用相同的签发时间，保证时间一致性
        Instant issuedAt = Instant.now(clock);

        // 3. 计算访问令牌的过期时间
        //    - 从配置文件读取访问令牌的有效期（通常较短，如15分钟）
        //    - 签发时间 + 有效期 = 过期时间
        //    - 访问令牌有效期短是为了提高安全性，减少令牌泄露的风险
        Instant accessExpiresAt = issuedAt.plus(properties.getJwt().getAccessTokenTtl());

        // 4. 计算刷新令牌的过期时间
        //    - 从配置文件读取刷新令牌的有效期（通常较长，如7天）
        //    - 刷新令牌有效期长是为了提供更好的用户体验
        //    - 用户不需要频繁登录，可以通过刷新令牌获取新的访问令牌
        Instant refreshExpiresAt = issuedAt.plus(properties.getJwt().getRefreshTokenTtl());

        // 5. 生成访问令牌
        //    - 调用encodeToken方法生成JWT格式的访问令牌
        //    - 令牌类型为"access"，用于访问受保护的API资源
        //    - 为访问令牌生成新的UUID作为jti（JWT ID）
        //    - 访问令牌包含用户基本信息（ID、昵称等）
        String accessToken = encodeToken(user, issuedAt, accessExpiresAt, "access", UUID.randomUUID().toString());

        // 6. 生成刷新令牌
        //    - 调用encodeRefreshToken方法生成JWT格式的刷新令牌
        //    - 使用前面生成的refreshTokenId作为令牌ID
        //    - 令牌类型为"refresh"，仅用于获取新的访问令牌
        //    - 刷新令牌存储在Redis白名单中，支持撤销操作
        String refreshToken = encodeRefreshToken(user, issuedAt, refreshExpiresAt, refreshTokenId);

        // 7. 构建并返回令牌对
        //    - TokenPair包含两个令牌及其过期时间
        //    - accessToken: 访问令牌字符串
        //    - accessExpiresAt: 访问令牌过期时间
        //    - refreshToken: 刷新令牌字符串
        //    - refreshExpiresAt: 刷新令牌过期时间
        //    - refreshTokenId: 刷新令牌的唯一标识符
        //    - 客户端需要保存这两个令牌，访问令牌用于API调用，刷新令牌用于续期
        return new TokenPair(accessToken, accessExpiresAt, refreshToken, refreshExpiresAt, refreshTokenId);
    }


    /**
     * 解码 JWT 字符串为 {@link Jwt}。
     *
     * @param token JWT 字符串。
     * @return 解析后的 JWT 对象。
     */
    public Jwt decode(String token) {
        return jwtDecoder.decode(token);
    }

    /**
     * 编码访问令牌。
     *
     * @param user      用户实体，作为 subject 与自定义声明来源。
     * @param issuedAt  签发时间。
     * @param expiresAt 过期时间。
     * @param tokenType 令牌类型（"access"）。
     * @param tokenId   令牌 ID（jti）。
     * @return 编码后的 JWT 字符串。
     */
    private String encodeToken(User user, Instant issuedAt, Instant expiresAt, String tokenType, String tokenId) {
        // 1. 构建JWT声明集合（Claims）
        //    JwtClaimsSet是JWT载荷（Payload）的标准化表示
        //    使用Builder模式构建，支持链式调用，代码清晰易读
        //    Claims包含标准声明和自定义声明两部分
        JwtClaimsSet claims = JwtClaimsSet.builder()
                // 2. 设置签发者（iss声明）
                //    - 从配置文件读取签发者标识，通常是系统名称或域名
                //    - 用于标识令牌的签发方，接收方可以验证令牌来源
                //    - 例如："https://api.example.com" 或 "my-auth-service"
                .issuer(properties.getJwt().getIssuer())

                // 3. 设置签发时间（iat声明）
                //    - 使用传入的签发时间，通常是当前UTC时间
                //    - 格式为Unix时间戳（秒），便于跨平台使用
                //    - 用于验证令牌是否在合理的时间范围内签发
                .issuedAt(issuedAt)

                // 4. 设置过期时间（exp声明）
                //    - 令牌失效的绝对时间，超过此时间令牌无效
                //    - 访问令牌通常较短（如15分钟），刷新令牌较长（如7天）
                //    - 客户端应在令牌过期前主动刷新，避免用户体验中断
                .expiresAt(expiresAt)

                // 5. 设置主题（sub声明）
                //    - 令牌的主体标识，通常是用户的唯一标识符
                //    - 使用用户ID作为字符串形式，便于后续查询用户信息
                //    - 这是JWT最重要的声明之一，用于标识令牌归属的用户
                .subject(String.valueOf(user.getId()))

                // 6. 设置令牌ID（jti声明）
                //    - JWT的唯一标识符，用于令牌的追踪和撤销
                //    - 每个令牌都有唯一的jti，可以用于黑名单机制
                //    - 访问令牌使用随机UUID，刷新令牌使用固定的refreshTokenId
                .id(tokenId)

                // 7. 添加自定义声明：令牌类型
                //    - 使用自定义声明标识令牌类型："access" 或 "refresh"
                //    - CLAIM_TOKEN_TYPE是常量，避免字符串硬编码
                //    - 用于区分访问令牌和刷新令牌，实现不同的验证逻辑
                .claim(CLAIM_TOKEN_TYPE, tokenType)

                // 8. 添加自定义声明：用户ID
                //    - 虽然sub声明已经包含用户ID，但为了方便使用再次添加
                //    - 某些客户端或中间件可能直接访问此声明
                //    - 保持与sub声明的一致性，避免混淆
                //TODO 目前不知道多余的有什么用，后续再看
                .claim(CLAIM_USER_ID, user.getId())

                // 9. 添加自定义声明：用户昵称
                //    - 存储用户的显示名称，便于前端展示
                //    - 避免每次请求都查询数据库获取用户信息
                //    - 注意：敏感信息不应存储在JWT中，因为JWT可以被解码
                .claim("nickname", user.getNickname())

                // 10. 构建Claims对象
                //     - 完成所有声明的设置，生成不可变的JwtClaimsSet对象
                //     - 一旦构建完成，Claims内容不可修改，保证安全性
                .build();

        // 11. 编码并签名JWT令牌
        //     - 将Claims对象转换为JWT编码器参数
        //     - 使用RSA私钥对JWT进行数字签名
        //     - 签名过程：Base64Url(Header) + "." + Base64Url(Payload) + "." + Base64Url(Signature)
        //     - 返回完整的JWT字符串，格式：header.payload.signature
//        JwtEncoderParameters.from(claims)：将之前构建的 JwtClaimsSet（载荷内容）包装成编码参数对象。
//        jwtEncoder.encode(...)：使用配置的签名算法（如 RSA256、HMAC256）和密钥，生成完整的 JWT。
//        .getTokenValue()：从 Jwt 对象中提取这个字符串。

        return jwtEncoder.encode(JwtEncoderParameters.from(claims)).getTokenValue();
    }

    /**
     * 编码刷新令牌。
     *
     * @param user      用户实体。
     * @param issuedAt  签发时间。
     * @param expiresAt 过期时间。
     * @param tokenId   刷新令牌 ID（jti）。
     * @return 编码后的刷新令牌字符串。
     */
    private String encodeRefreshToken(User user, Instant issuedAt, Instant expiresAt, String tokenId) {
        // 构建JWT的载荷（Claims）
        JwtClaimsSet claims = JwtClaimsSet.builder()
                // 签发者（iss）：通常为系统名称或域名，表明令牌由谁颁发
                .issuer(properties.getJwt().getIssuer())
                // 签发时间（iat）：令牌生成的时间戳，用于时间验证
                .issuedAt(issuedAt)
                // 过期时间（exp）：令牌失效的绝对时间，超过则不可用
                .expiresAt(expiresAt)
                // 主体（sub）：令牌归属的用户唯一标识（通常是用户ID）
                .subject(String.valueOf(user.getId()))
                // JWT ID（jti）：令牌的唯一标识符，用于撤销（黑名单/白名单）
                .id(tokenId)
                // 自定义声明：令牌类型，此处固定为 "refresh"
                .claim(CLAIM_TOKEN_TYPE, "refresh")
                // 自定义声明：用户ID（冗余但便于某些框架直接读取）
                .claim(CLAIM_USER_ID, user.getId())
                .build();

        // 使用JWT编码器对载荷进行签名，生成完整的JWT字符串
        return jwtEncoder.encode(JwtEncoderParameters.from(claims)).getTokenValue();
    }

    /**
     * 从 JWT 中提取用户 ID。
     *
     * @param jwt 已解析的 JWT。
     * @return 用户 ID（long）。
     * @throws IllegalArgumentException 当声明类型不合法时抛出。
     */
    public long extractUserId(Jwt jwt) {
        Object claim = jwt.getClaims().get(CLAIM_USER_ID);
        if (claim instanceof Number number) {
            return number.longValue();
        }
        if (claim instanceof String text) {
            return Long.parseLong(text);
        }
        throw new IllegalArgumentException("Invalid user id in token");
    }

    /**
     * 提取令牌类型声明。
     *
     * @param jwt 已解析的 JWT。
     * @return 令牌类型字符串（例如："access" 或 "refresh"）。
     */
    public String extractTokenType(Jwt jwt) {
        Object claim = jwt.getClaims().get(CLAIM_TOKEN_TYPE);
        return claim != null ? claim.toString() : "";
    }

    /**
     * 提取令牌 ID（jti）。
     *
     * @param jwt 已解析的 JWT。
     * @return 令牌 ID。
     */
    public String extractTokenId(Jwt jwt) {
        return jwt.getId();
    }
}
