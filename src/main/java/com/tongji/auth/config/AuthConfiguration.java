package com.tongji.auth.config;

import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.source.ImmutableJWKSet;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.proc.SecurityContext;
import com.tongji.auth.util.PemUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtEncoder;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.jwt.NimbusJwtEncoder;

import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;

/**
 * 认证配置类
 * <p>
 * 该类是整个认证系统的核心配置，负责创建和注册 Spring Security 和 OAuth2 JWT 相关的核心 Bean。
 * 主要功能包括用户密码加密、JWT 令牌签发和验证。
 * <p>
 * 认证流程图：
 * <pre>
 * ┌─────────────────────────────────────────────────────────────────────────────┐
 * │                           用户认证完整流程                                    │
 * └─────────────────────────────────────────────────────────────────────────────┘
 *
 * 【登录阶段 - 生成 JWT 令牌】
 *
 * 用户输入账号密码
 *       ↓
 * Controller 接收登录请求
 *       ↓
 * 调用 AuthService 进行认证
 *       ↓
 * 使用 PasswordEncoder 验证密码（BCrypt 加密比对）
 *       ↓
 * 验证成功后，调用 JwtEncoder 签发 JWT 令牌
 *       ↓
 * 使用 RSA 私钥对令牌进行数字签名
 *       ↓
 * 返回 JWT 令牌给客户端
 *
 * 【访问受保护资源 - 验证 JWT 令牌】
 *
 * 客户端携带 JWT 令牌访问受保护接口
 *       ↓
 * Spring Security 过滤器链拦截请求
 *       ↓
 * 调用 JwtDecoder 验证令牌
 *       ↓
 * 使用 RSA 公钥验证令牌签名
 *       ↓
 * 检查令牌是否过期、格式是否正确
 *       ↓
 * 解析令牌中的用户信息（用户名、权限等）
 *       ↓
 * 将用户信息存入 SecurityContext
 *       ↓
 * 继续处理业务逻辑
 *
 * 【核心组件关系图】
 *
 * ┌──────────────────┐         ┌──────────────────┐
 * │   AuthProperties │────────→│ AuthConfiguration │
 * │  (配置属性类)     │  注入   │  (配置类)         │
 * └──────────────────┘         └────────┬─────────┘
 *                                       │
 *                                       ├────────→ PasswordEncoder (BCrypt)
 *                                       │         用于密码加密和验证
 *                                       │
 *                                       ├────────→ JwtEncoder (RSA私钥)
 *                                       │         用于签发 JWT 令牌
 *                                       │
 *                                       └────────→ JwtDecoder (RSA公钥)
 *                                                 用于验证 JWT 令牌
 *
 * 【密钥管理】
 *
 * PEM 文件存储 RSA 密钥对：
 * - 私钥：用于 JWT 签名（仅认证服务持有）
 * - 公钥：用于 JWT 验证（可分发给所有需要验证的服务）
 *
 * 通过 PemUtils 工具类读取 PEM 格式的密钥文件
 * 转换为 JWK (JSON Web Key) 格式供 Nimbus 库使用
 * </pre>
 * <p>
 * 配置说明：
 * - 密码加密强度：通过 {@code auth.password.bcrypt-strength} 配置
 * - RSA 密钥路径：通过 {@code auth.jwt.private-key} 和 {@code auth.jwt.public-key} 配置
 * - 密钥标识符：通过 {@code auth.jwt.key-id} 配置，用于密钥轮换
 *
 * @see AuthProperties
 * @see PemUtils
 */
@Configuration
@EnableConfigurationProperties(AuthProperties.class)
@RequiredArgsConstructor
public class AuthConfiguration {

    /**
     * 认证配置属性
     * 通过构造函数注入，包含密码加密强度、JWT 密钥路径等配置信息
     */
    private final AuthProperties properties;

    /**
     * 密码加密器 Bean
     * <p>
     * 功能说明：
     * <pre>
     * ┌─────────────────────────────────────────────────────────┐
     * │                  密码加密流程                            │
     * └─────────────────────────────────────────────────────────┘
     *
     * 【用户注册/修改密码】
     * 明文密码 → BCryptPasswordEncoder.encode() → 加密后的密码 → 存入数据库
     *
     * 【用户登录验证】
     * 用户输入明文密码
     *       ↓
     * 从数据库读取加密后的密码
     *       ↓
     * BCryptPasswordEncoder.matches(明文, 加密密码)
     *       ↓
     * 返回 true/false（验证结果）
     * </pre>
     * <p>
     * BCrypt 算法特点：
 * - 自动生成随机盐值（salt），防止彩虹表攻击
 * - 盐值已包含在加密结果中，无需单独存储
 * - 可通过 strength 参数调整加密强度（默认为 10，表示 2^10 轮哈希）
 * - 计算成本可控，可随硬件性能提升调整强度
 * <p>
 * 使用场景：
 * - 用户注册时：对用户设置的密码进行加密后存储
 * - 用户登录时：验证用户输入的密码是否正确
 * - 修改密码时：对新密码进行加密后更新数据库
     * <p>
     * Spring Security 会自动使用此 PasswordEncoder 进行密码验证，
     * 开发者无需手动调用加密和比对方法。
     *
     * @return BCryptPasswordEncoder 实例，加密强度由配置文件决定
     */
    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder(properties.getPassword().getBcryptStrength());
    }

    /**
     * JWT 编码器 Bean
     * <p>
     * 功能说明：
     * <pre>
     * ┌─────────────────────────────────────────────────────────┐
     * │              JWT 令牌签发流程                            │
     * └─────────────────────────────────────────────────────────┘
     *
     * 用户认证成功
     *       ↓
     * 构建 JWT 载荷（用户名、权限、过期时间等）
     *       ↓
     * 调用 JwtEncoder.encode()
     *       ↓
     * 读取 RSA 私钥和公钥（PEM 格式）
     *       ↓
     * 构建 RSAKey 对象（JWK 格式）
     *       ↓
     * 使用私钥对 JWT 进行数字签名
     *       ↓
     * 生成完整的 JWT 令牌
     *       ↓
     * 返回给客户端
     *
     * JWT 结构：header.payload.signature
     * - header：算法类型（RS256）、密钥 ID
     * - payload：用户信息、权限、过期时间等
     * - signature：使用 RSA 私钥签名的哈希值
     * </pre>
     * <p>
     * 实现细节：
 * 1. 从配置中读取 PEM 格式的 RSA 私钥和公钥文件路径
 * 2. 使用 PemUtils 工具类解析 PEM 文件，获得 RSAPrivateKey 和 RSAPublicKey 对象
 * 3. 构建 RSAKey 对象（Nimbus 库的 JWK 实现），设置 keyID 用于密钥识别
 * 4. 将 RSAKey 封装为 JWKSet，再创建不可变的 JWKSource
 * 5. 构造 NimbusJwtEncoder，内部使用私钥对 JWT 进行签名
 * <p>
 * 安全特性：
 * - 使用 RSA 非对称加密，私钥签名，公钥验证
 * - 私钥仅保存在认证服务中，防止泄露
 * - 签名保证令牌不可伪造和篡改
 * - keyID 支持密钥轮换，提高安全性
 * <p>
 * 使用场景：
 * - 用户登录成功后生成 access_token
 * - 刷新令牌时生成新的 access_token
 * - 第三方授权时生成授权码
     *
     * @return NimbusJwtEncoder 实例，使用 RSA 私钥签名 JWT
     */
    @Bean
    public JwtEncoder jwtEncoder() {
        // 获取 JWT 相关配置（密钥路径、keyID 等）
        AuthProperties.Jwt jwtProps = properties.getJwt();

        // 从 PEM 文件中读取 RSA 密钥对
        // 私钥用于签名，公钥用于构建 JWK
        RSAPrivateKey privateKey = PemUtils.readPrivateKey(jwtProps.getPrivateKey());
        RSAPublicKey publicKey = PemUtils.readPublicKey(jwtProps.getPublicKey());

        // 构建 RSA 类型的 JWK (JSON Web Key)
        // JWK 是密钥的标准化表示格式，便于在不同系统间传递
        RSAKey jwk = new RSAKey.Builder(publicKey)
                .privateKey(privateKey)  // 设置私钥用于签名
                .keyID(jwtProps.getKeyId())  // 设置密钥标识符，用于密钥轮换
                .build();

        // 将 JWK 封装为不可变的 JWKSet，再创建 JWKSource
        // JWKSource 是 Nimbus 库的密钥源抽象，支持多个密钥
        JWKSource<SecurityContext> jwkSource = new ImmutableJWKSet<>(new JWKSet(jwk));

        // 创建 JWT 编码器，使用 JWKSource 中的密钥进行签名
        return new NimbusJwtEncoder(jwkSource);
    }

    /**
     * JWT 解码器 Bean
     * <p>
     * 功能说明：
     * <pre>
     * ┌─────────────────────────────────────────────────────────┐
     * │              JWT 令牌验证流程                            │
     * └─────────────────────────────────────────────────────────┘
     *
     * 客户端请求携带 JWT 令牌
     *       ↓
     * Spring Security 过滤器链拦截
     *       ↓
     * JwtDecoder.decode(token)
     *       ↓
     * 解析 JWT 的 header 和 payload
     *       ↓
     * 读取 RSA 公钥
     *       ↓
     * 使用公钥验证签名
     *       ↓
     * 检查签名是否匹配
     *       ↓
     * 检查令牌是否过期（exp）
     *       ↓
     * 检查令牌是否生效（nbf）
     *       ↓
     * 解析载荷中的用户信息
     *       ↓
     * 构建认证对象存入 SecurityContext
     *       ↓
     * 继续处理业务逻辑
     * </pre>
     * <p>
     * 验证内容：
 * - 签名验证：使用 RSA 公钥验证令牌是否由对应的私钥签发
 * - 格式验证：检查 JWT 格式是否正确（header.payload.signature）
 * - 过期检查：验证 exp（过期时间）声明，拒绝过期令牌
 * - 生效检查：验证 nbf（生效时间）声明，拒绝未生效令牌
 * - 签发者检查：验证 iss（签发者）声明是否匹配
 * <p>
 * 实现细节：
 * 1. 从配置中读取 PEM 格式的 RSA 公钥文件路径
 * 2. 使用 PemUtils 工具类解析 PEM 文件，获得 RSAPublicKey 对象
 * 3. 使用 NimbusJwtDecoder.withPublicKey() 构建解码器
 * 4. 解码器会自动处理签名验证和过期检查
 * <p>
 * 安全特性：
 * - 仅使用公钥验证，无需私钥，降低安全风险
 * - 自动拒绝过期和无效令牌
 * - 解析出的用户信息可用于后续授权判断
 * - 支持分布式部署，多个服务可共享同一公钥
 * <p>
 * 使用场景：
 * - 每次访问受保护资源时验证令牌
 * - 微服务架构中各服务验证令牌
 * - API 网关统一验证令牌
 * <p>
 * 与 JwtEncoder 的区别：
 * - JwtEncoder 使用私钥签发令牌（仅在认证服务）
 * - JwtDecoder 使用公钥验证令牌（可在多个服务部署）
 * - 这种非对称加密设计保证了签名的安全性和验证的便利性
     *
     * @return NimbusJwtDecoder 实例，使用 RSA 公钥验证 JWT 签名
     */
    @Bean
    public JwtDecoder jwtDecoder() {
        // 获取 JWT 相关配置
        AuthProperties.Jwt jwtProps = properties.getJwt();

        // 从 PEM 文件中读取 RSA 公钥
        // 验证签名只需要公钥，不需要私钥
        RSAPublicKey publicKey = PemUtils.readPublicKey(jwtProps.getPublicKey());

        // 使用 Nimbus 提供的便捷构造器创建解码器
        // 解码器会自动处理签名验证、过期检查等逻辑
        return NimbusJwtDecoder.withPublicKey(publicKey).build();
    }
}