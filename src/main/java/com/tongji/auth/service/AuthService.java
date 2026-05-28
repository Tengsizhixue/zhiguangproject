package com.tongji.auth.service;

import lombok.RequiredArgsConstructor;
import com.tongji.auth.api.dto.AuthResponse;
import com.tongji.auth.api.dto.AuthUserResponse;
import com.tongji.auth.api.dto.LoginRequest;
import com.tongji.auth.api.dto.PasswordResetRequest;
import com.tongji.auth.api.dto.RegisterRequest;
import com.tongji.auth.api.dto.SendCodeRequest;
import com.tongji.auth.api.dto.SendCodeResponse;
import com.tongji.auth.api.dto.TokenRefreshRequest;
import com.tongji.auth.api.dto.TokenResponse;
import com.tongji.auth.audit.LoginLogService;
import com.tongji.auth.config.AuthProperties;
import com.tongji.common.exception.BusinessException;
import com.tongji.common.exception.ErrorCode;
import com.tongji.auth.model.ClientInfo;
import com.tongji.auth.model.IdentifierType;
import com.tongji.auth.token.JwtService;
import com.tongji.auth.token.RefreshTokenStore;
import com.tongji.auth.token.TokenPair;
import com.tongji.user.domain.User;
import com.tongji.user.service.UserService;
import com.tongji.auth.util.IdentifierValidator;
import com.tongji.auth.verification.SendCodeResult;
import com.tongji.auth.verification.VerificationCheckResult;
import com.tongji.auth.verification.VerificationCodeStatus;
import com.tongji.auth.verification.VerificationScene;
import com.tongji.auth.verification.VerificationService;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.springframework.security.oauth2.jwt.JwtException;

/**
 * 认证业务服务。
 * <p>
 * 职责：发送验证码、注册、登录、刷新令牌、登出、重置密码、查询当前用户信息。
 * 安全策略：
 * - 账号格式校验（手机号/邮箱）；
 * - 验证码状态检查（过期/错误/尝试超限）；
 * - 密码复杂度校验（长度与字符类型）；
 * - Refresh Token 白名单存储与轮换，登出/重置密码后失效旧令牌；
 * 审计：记录注册/登录成功与失败，包含渠道、IP、UA。
 * 令牌：签发 RS256 的 Access/Refresh JWT，携带 uid、token_type、jti。
 * 依赖：UserService、VerificationService、PasswordEncoder、JwtService、RefreshTokenStore、LoginLogService、AuthProperties。
 */
@Service
@RequiredArgsConstructor
public class AuthService {

    private final UserService userService;
    private final VerificationService verificationService;
    private final PasswordEncoder passwordEncoder;
    private final JwtService jwtService;
    private final RefreshTokenStore refreshTokenStore;
    private final LoginLogService loginLogService;
    private final AuthProperties authProperties;

    /**
     * 发送验证码并返回过期信息。
     * <p>
     * 注册场景要求标识不存在；登录/重置密码场景要求标识存在。
     *
     * @param request 请求体，包含：标识类型与值、场景。
     * @return 响应体，包含目标标识、场景与验证码过期秒数。
     * @throws BusinessException 当标识格式错误或存在性不符合场景要求时抛出。
     */
public SendCodeResponse sendCode(SendCodeRequest request) {
    // 1. 验证标识格式：检查手机号或邮箱格式是否正确
    //    - 如果是手机号，调用 IdentifierValidator.isValidPhone() 验证
    //    - 如果是邮箱，调用 IdentifierValidator.isValidEmail() 验证
    //    - 格式不正确时抛出 BusinessException，返回错误信息

    validateIdentifier(request.identifierType(), request.identifier());

    // 2. 标准化标识：将标识转换为统一格式
    //    - 手机号：去除前后空格
    //    - 邮箱：去除前后空格并转为小写（避免大小写不一致）
    //    - 标准化后的标识用于后续的数据库查询和验证码发送
    String normalized = normalizeIdentifier(request.identifierType(), request.identifier());

    // 3. 检查标识是否存在：查询数据库判断该标识是否已注册
    //    - 如果是手机号，调用 userService.existsByPhone() 查询
    //    - 如果是邮箱，调用 userService.existsByEmail() 查询
    //    - 返回 true 表示已存在，false 表示不存在
    boolean exists = identifierExists(request.identifierType(), normalized);

    // 4. 场景校验：注册场景要求标识不存在
    //    - 如果是注册场景且标识已存在，抛出异常
    //    - 防止用户重复注册，保护账号唯一性
    if (request.scene() == VerificationScene.REGISTER && exists) {
        throw new BusinessException(ErrorCode.IDENTIFIER_EXISTS);
    }

    // 5. 场景校验：登录和重置密码场景要求标识必须存在
    //    - 登录场景：标识不存在则无法登录
    //    - 重置密码场景：标识不存在则无法重置密码
    //    - 如果标识不存在，抛出异常提示用户先注册
    if ((request.scene() == VerificationScene.LOGIN || request.scene() == VerificationScene.RESET_PASSWORD) && !exists) {
        throw new BusinessException(ErrorCode.IDENTIFIER_NOT_FOUND);
    }

    // 6. 发送验证码：调用验证码服务发送验证码
    //    - 传入场景（注册/登录/重置密码）和标准化后的标识
    //    - 返回发送结果，包含标识、场景和过期秒数
    SendCodeResult result = verificationService.sendCode(request.scene(), normalized);

    // 7. 构造响应：将发送结果转换为响应对象返回给客户端
    //    - 包含目标标识、场景和验证码过期时间
    //    - 客户端可根据过期时间显示倒计时，提升用户体验
    return new SendCodeResponse(result.identifier(), result.scene(), result.expireSeconds());
}


    /**
     * 注册用户并签发令牌。
     * <p>
     * 验证标识与验证码，创建用户（可选设置密码），记录审计，签发令牌对并保存刷新令牌白名单。
     *
     * @param request    注册请求，包含：标识类型与值、验证码、可选密码、是否同意协议。
     * @param clientInfo 客户端信息（IP/UA），用于登录审计。
     * @return 认证响应，包含用户信息与令牌对。
     * @throws BusinessException 当未同意协议、标识冲突、验证码失败、密码不合规时抛出。
     */
    public AuthResponse register(RegisterRequest request, ClientInfo clientInfo) {
        // 1. 检查用户协议同意状态：确保用户同意了服务条款和隐私政策
        //    - 这是法律合规要求，保护平台和用户双方的权益
        //    - 如果用户未同意协议，抛出异常要求用户先同意
        if (!request.agreeTerms()) {
            throw new BusinessException(ErrorCode.TERMS_NOT_ACCEPTED);
        }

        // 2. 验证标识格式：检查手机号或邮箱格式是否正确
        validateIdentifier(request.identifierType(), request.identifier());

        // 3. 标准化标识：将标识转换为统一格式
        String identifier = normalizeIdentifier(request.identifierType(), request.identifier());

        // 4. 检查标识是否已存在：防止重复注册
        if (identifierExists(request.identifierType(), identifier)) {
            throw new BusinessException(ErrorCode.IDENTIFIER_EXISTS);
        }

        // 5. 先简单非空校验，再调用redis校验
        ensureVerificationSuccess(verificationService.verify(VerificationScene.REGISTER, identifier, request.code()));

        // 6. 构建用户对象：使用建造者模式创建用户实体
        //    - 根据标识类型设置手机号或邮箱（二选一）
        //    - 生成默认昵称和头像
        //    - 初始化其他用户信息
        User user = User.builder()
                .phone(request.identifierType() == IdentifierType.PHONE ? identifier : null)  // 如果是手机号类型，设置手机号字段
                .email(request.identifierType() == IdentifierType.EMAIL ? identifier : null)  // 如果是邮箱类型，设置邮箱字段
                .nickname(generateNickname())  // 生成随机昵称，如"用户_abc123"
                .avatar("https://static.zhiguang.cn/default-avatar.png")  // 设置默认头像URL
                .bio(null)  // 个人简介初始化为空
                .tagsJson("[]")  // 用户标签初始化为空数组JSON格式
                .build();

        // 7. 可选密码设置：如果用户提供了密码，则进行密码处理
        //    - 密码是可选的，用户可以只通过验证码注册
        //    - 设置密码后，用户可以使用密码或验证码两种方式登录
        if (StringUtils.hasText(request.password())) {
            // 7.1 验证密码复杂度：确保密码符合安全要求
            //    - 检查密码长度、字符类型等
            //    - 不符合要求时抛出异常，提示用户修改密码
            validatePassword(request.password());
            // 7.2 密码加密存储：使用密码编码器对密码进行哈希处理
            //    - 使用 BCrypt 等安全算法对密码进行单向加密
            //    - 永远不要存储明文密码，即使数据库泄露也不会暴露用户密码
            //    - trim() 去除密码前后空格，避免用户输入错误
            user.setPasswordHash(passwordEncoder.encode(request.password().trim()));
        }

        // 8. 创建用户：将用户信息持久化到数据库
        //    - 调用 userService 保存用户记录
        //    - 数据库会为用户分配唯一的主键ID
        //    - 创建成功后，用户可以正常使用系统功能
        userService.createUser(user);

        // 9. 生成JWT令牌对：注册成功后自动登录，生成访问令牌和刷新令牌
        //    - 访问令牌（Access Token）：用于API访问，有效期较短（如15分钟）
        //    - 刷新令牌（Refresh Token）：用于获取新的访问令牌，有效期较长（如7天）
        //    - 令牌中包含用户ID、用户名、角色等信息
        //    - 注册成功后立即签发令牌，提供良好的用户体验（无需再次登录）
        TokenPair tokenPair = jwtService.issueTokenPair(user);

        // 10. 存储刷新令牌到白名单：将刷新令牌存储到Redis中
        //     - 键格式：auth:rt:{userId}:{tokenId}
        //     - 值固定为"1"，设置TTL控制过期时间
        //     - 用于令牌撤销和有效性验证
        //     - 支持撤销单个令牌或撤销某用户全部令牌
        storeRefreshToken(user.getId(), tokenPair);

        // 11. 记录注册日志：用于安全审计和用户行为分析
        //     - 记录用户ID、标识、注册通道（REGISTER）、客户端IP、User Agent、成功状态
        //     - 可以用于检测异常注册行为、统计分析、合规审计等
        //     - 通道标记为"REGISTER"，与登录（"PASSWORD"/"CODE"）区分
        loginLogService.record(user.getId(), identifier, "REGISTER", clientInfo.ip(), clientInfo.userAgent(), "SUCCESS");

        // 12. 构造并返回认证响应：将用户信息和令牌信息封装成响应对象
        //     - 用户信息：包含用户ID、昵称、头像、个人简介等
        //     - 令牌信息：包含访问令牌、刷新令牌、令牌类型等
        //     - 客户端收到响应后，将令牌存储在本地，用于后续API请求
        //     - 注册成功后用户立即获得登录状态，可以直接使用系统功能
        return new AuthResponse(mapUser(user), mapToken(tokenPair));
    }

    /**
     * 登录并签发令牌。
     * <p>
     * 支持密码或验证码通道；成功后记录审计，签发令牌对并保存刷新令牌白名单。
     *
     * @param request    登录请求，包含：标识类型与值、密码或验证码（二选一）。
     * @param clientInfo 客户端信息（IP/UA），用于登录审计。
     * @return 认证响应，包含用户信息与令牌对。
     * @throws BusinessException 当用户不存在、凭证错误或请求不合法时抛出。
     */
    public AuthResponse login(LoginRequest request, ClientInfo clientInfo) {
        // 1. 验证标识格式：检查手机号或邮箱格式是否正确
        //    - 如果是手机号，调用 IdentifierValidator.isValidPhone() 验证
        //    - 如果是邮箱，调用 IdentifierValidator.isValidEmail() 验证
        //    - 格式不正确时抛出 BusinessException，返回错误信息
        validateIdentifier(request.identifierType(), request.identifier());

        // 2. 标准化标识：将标识转换为统一格式
        //    - 手机号：去除前后空格
        //    - 邮箱：去除前后空格并转为小写（避免大小写不一致）
        //    - 标准化后的标识用于后续的数据库查询和验证码验证
        String identifier = normalizeIdentifier(request.identifierType(), request.identifier());

        // 3. 查找用户：根据标识类型和标准化后的标识查询用户
        //    - 如果是手机号，调用 userService.findByPhone() 查询
        //    - 如果是邮箱，调用 userService.findByEmail() 查询
        //    - 返回 Optional<User>，可能为空
        Optional<User> userOptional = findUserByIdentifier(request.identifierType(), identifier);

        // 4. 检查用户是否存在：如果用户不存在，抛出异常
        //    - 用户不存在说明该标识未注册，无法登录
        //    - 抛出 IDENTIFIER_NOT_FOUND 异常，提示用户先注册
        if (userOptional.isEmpty()) {
            throw new BusinessException(ErrorCode.IDENTIFIER_NOT_FOUND);
        }

        // 5. 获取用户对象：从 Optional 中提取用户信息
        //    - 此时可以确定用户存在，安全调用 get()
        User user = userOptional.get();

        // 6. 确定登录通道：记录用户使用的登录方式（密码或验证码）
        //    - 用于后续的登录日志记录和统计分析
        String channel;

        // 7. 判断登录方式：支持密码登录和验证码登录两种通道
        if (StringUtils.hasText(request.password())) {
            // 7.1 密码登录通道：用户提供了密码
            channel = "PASSWORD";
            
            // 7.2 验证密码：检查用户是否设置了密码以及密码是否匹配
            //    - 首先检查用户是否设置了密码（user.getPasswordHash()不为空）
            //    - 然后使用 passwordEncoder.matches() 比较输入密码和存储的密码哈希
            //    - 密码不匹配时记录失败日志并抛出异常
            if (!StringUtils.hasText(user.getPasswordHash()) || !passwordEncoder.matches(request.password(), user.getPasswordHash())) {
                // 7.2.1 记录登录失败日志：用于安全审计和异常检测
                //    - 记录用户ID、标识、登录通道、客户端IP、User Agent、失败状态
                loginLogService.record(user.getId(), identifier, channel, clientInfo.ip(), clientInfo.userAgent(), "FAILED");
                // 7.2.2 抛出凭证无效异常：提示用户密码错误
                throw new BusinessException(ErrorCode.INVALID_CREDENTIALS);
            }
        } else if (StringUtils.hasText(request.code())) {
            // 7.3 验证码登录通道：用户提供了验证码
            channel = "CODE";
            
            // 7.4 验证验证码：调用验证码服务验证用户输入的验证码
            //    - 传入场景（LOGIN）、标识（手机号/邮箱）、用户输入的验证码
            //    - ensureVerificationSuccess 方法会检查验证结果
            //    - 验证失败时会抛出相应的异常（验证码不匹配、已过期、尝试次数过多等）
            //    - 只有验证成功才会继续执行后续流程
            ensureVerificationSuccess(verificationService.verify(VerificationScene.LOGIN, identifier, request.code()));
        } else {
            // 7.5 既没有提供密码也没有提供验证码：请求不合法
            //    - 抛出异常提示用户必须提供密码或验证码中的一种
            throw new BusinessException(ErrorCode.BAD_REQUEST, "请提供验证码或密码");
        }

        // 8. 生成JWT令牌对：凭证验证成功后，生成访问令牌和刷新令牌
        //    - 访问令牌（Access Token）：用于API访问，有效期较短（如15分钟）
        //    - 刷新令牌（Refresh Token）：用于获取新的访问令牌，有效期较长（如7天）
        //    - 令牌中包含用户ID、用户名、角色等信息
        TokenPair tokenPair = jwtService.issueTokenPair(user);

        // 9. 存储刷新令牌到白名单：将刷新令牌存储到Redis中
        //    - 键格式：auth:rt:{userId}:{tokenId}
        //    - 值固定为"1"，设置TTL控制过期时间
        //    - 用于令牌撤销和有效性验证
        //    - 支持撤销单个令牌或撤销某用户全部令牌
        storeRefreshToken(user.getId(), tokenPair);

        // 10. 记录登录成功日志：用于安全审计和用户行为分析
        //     - 记录用户ID、标识、登录通道、客户端IP、User Agent、成功状态
        //     - 可以用于检测异常登录行为、统计分析等
        loginLogService.record(user.getId(), identifier, channel, clientInfo.ip(), clientInfo.userAgent(), "SUCCESS");

        // 11. 构造并返回认证响应：将用户信息和令牌信息封装成响应对象
        //     - 用户信息：包含用户ID、昵称、头像、个人简介等
        //     - 令牌信息：包含访问令牌、刷新令牌、令牌类型等
        //     - 客户端收到响应后，将令牌存储在本地，用于后续API请求
        return new AuthResponse(mapUser(user), mapToken(tokenPair));
    }

    /**
     * 使用刷新令牌获取新的令牌对。
     * <p>
     * 校验刷新令牌类型与白名单有效性，签发新令牌后撤销旧刷新令牌并存储新令牌。
     *
     * @param request 刷新请求，包含：refreshToken。
     * @return 新的令牌响应。
     * @throws BusinessException 当刷新令牌无效或用户不存在时抛出。
     */
    public TokenResponse refresh(TokenRefreshRequest request) {
        Jwt jwt = decodeRefreshToken(request.refreshToken());

        if (!Objects.equals("refresh", jwtService.extractTokenType(jwt))) {
            throw new BusinessException(ErrorCode.REFRESH_TOKEN_INVALID);
        }

        long userId = jwtService.extractUserId(jwt);
        String tokenId = jwtService.extractTokenId(jwt);

        if (!refreshTokenStore.isTokenValid(userId, tokenId)) {
            throw new BusinessException(ErrorCode.REFRESH_TOKEN_INVALID);
        }

        User user = findUserById(userId).orElseThrow(() -> new BusinessException(ErrorCode.IDENTIFIER_NOT_FOUND));
        TokenPair tokenPair = jwtService.issueTokenPair(user);
        refreshTokenStore.revokeToken(userId, tokenId);
        storeRefreshToken(userId, tokenPair);

        return mapToken(tokenPair);
    }

    /**
     * 登出：撤销指定刷新令牌。
     *
     * @param refreshToken 刷新令牌字符串；若解析为合法刷新令牌则撤销其白名单记录。
     */
    public void logout(String refreshToken) {
        decodeRefreshTokenSafely(refreshToken).ifPresent(jwt -> {
            if (Objects.equals("refresh", jwtService.extractTokenType(jwt))) {
                long userId = jwtService.extractUserId(jwt);
                String tokenId = jwtService.extractTokenId(jwt);
                refreshTokenStore.revokeToken(userId, tokenId);
            }
        });
    }

    /**
     * 使用验证码重置密码并使刷新令牌失效。
     *
     * @param request 重置请求，包含：标识类型与值、验证码、新密码。
     * @throws BusinessException 当标识不存在、验证码失败或密码策略不满足时抛出。
     */
    public void resetPassword(PasswordResetRequest request) {
        validateIdentifier(request.identifierType(), request.identifier());
        validatePassword(request.newPassword());
        String identifier = normalizeIdentifier(request.identifierType(), request.identifier());
        User user = findUserByIdentifier(request.identifierType(), identifier)
                .orElseThrow(() -> new BusinessException(ErrorCode.IDENTIFIER_NOT_FOUND));
        ensureVerificationSuccess(verificationService.verify(VerificationScene.RESET_PASSWORD, identifier, request.code()));
        user.setPasswordHash(passwordEncoder.encode(request.newPassword().trim()));
        userService.updatePassword(user);
        refreshTokenStore.revokeAll(user.getId());
    }

    /**
     * 查询用户概要信息。
     *
     * @param userId 用户 ID。
     * @return 用户概要响应。
     * @throws BusinessException 当用户不存在时抛出。
     */
    public AuthUserResponse me(long userId) {
        User user = findUserById(userId)
                .orElseThrow(() -> new BusinessException(ErrorCode.IDENTIFIER_NOT_FOUND));
        return mapUser(user);
    }

    /**
     * 保证验证码校验成功，否则按状态抛出对应业务异常。
     *
     * @param result 验证码校验结果。
     */
    private void ensureVerificationSuccess(VerificationCheckResult result) {
        if (result.isSuccess()) {
            return;
        }
        VerificationCodeStatus status = result.status();
        if (status == VerificationCodeStatus.NOT_FOUND || status == VerificationCodeStatus.EXPIRED) {
            throw new BusinessException(ErrorCode.VERIFICATION_NOT_FOUND);
        }
        if (status == VerificationCodeStatus.MISMATCH) {
            throw new BusinessException(ErrorCode.VERIFICATION_MISMATCH);
        }
        if (status == VerificationCodeStatus.TOO_MANY_ATTEMPTS) {
            throw new BusinessException(ErrorCode.VERIFICATION_TOO_MANY_ATTEMPTS);
        }
        throw new BusinessException(ErrorCode.BAD_REQUEST, "验证码校验失败");
    }

    /**
     * 校验标识（手机号/邮箱）的格式。
     *
     * @param type       标识类型：PHONE 或 EMAIL。
     * @param identifier 标识值。
     * @throws BusinessException 当格式不合法时抛出。
     */
    private void validateIdentifier(IdentifierType type, String identifier) {
        //判断是否为大陆手机号
        if (type == IdentifierType.PHONE && !IdentifierValidator.isValidPhone(identifier)) {
            throw new BusinessException(ErrorCode.BAD_REQUEST, "手机号格式错误");
        }
        //是否匹配邮箱正则
        if (type == IdentifierType.EMAIL && !IdentifierValidator.isValidEmail(identifier)) {
            throw new BusinessException(ErrorCode.BAD_REQUEST, "邮箱格式错误");
        }
    }

    /**
 * 校验密码策略：非空、最小长度、必须包含字母和数字。
 *
 * @param password 明文密码。
 * @throws BusinessException 当密码不满足策略时抛出。
 */
private void validatePassword(String password) {
    // 第一步：非空校验
    // 使用StringUtils.hasText()检查密码是否为null、空字符串或仅包含空白字符
    // 这比直接使用password == null或password.isEmpty()更全面，能处理各种空值情况
    if (!StringUtils.hasText(password)) {
        throw new BusinessException(ErrorCode.PASSWORD_POLICY_VIOLATION, "密码不能为空");
    }

    // 第二步：去除前后空格
    // trim()去除密码前后的空白字符，避免用户输入时的意外空格影响验证
    // 注意：这里不修改原始密码字符串，只是创建一个去除空格的副本用于验证
    String trimmed = password.trim();

    // 第三步：最小长度校验
    // 从配置中获取密码最小长度要求，进行动态验证
    // 这样可以灵活调整密码策略而无需修改代码
    if (trimmed.length() < authProperties.getPassword().getMinLength()) {
        throw new BusinessException(ErrorCode.PASSWORD_POLICY_VIOLATION,
            "密码长度至少" + authProperties.getPassword().getMinLength() + "位");
    }

    // 第四步：字符类型校验 - 检查是否包含字母
    // trimmed.chars()将字符串转换为IntStream（字符的Unicode码点流）
    // anyMatch(Character::isLetter)检查流中是否存在任意字母字符，anyMatch会短路，一旦找到一个字母字符就返回true
    // Character::isLetter是方法引用，等价于 c -> Character.isLetter(c)
    // 这个方法能正确处理各种语言的字母字符（包括中文、日文等）
    boolean hasLetter = trimmed.chars().anyMatch(Character::isLetter);

    // 第五步：字符类型校验 - 检查是否包含数字
    // 同样使用流式API检查是否存在任意数字字符
    // Character::isDigit能正确识别各种数字字符（包括阿拉伯数字、罗马数字等）
    boolean hasDigit = trimmed.chars().anyMatch(Character::isDigit);

    // 第六步：综合校验
    // 必须同时包含字母和数字，否则抛出异常
    // 使用逻辑或运算符，只要缺少字母或数字中的任意一种就验证失败
    if (!hasLetter || !hasDigit) {
        throw new BusinessException(ErrorCode.PASSWORD_POLICY_VIOLATION, "密码需包含字母和数字");
    }
}


    /**
     * 判断标识是否已存在。
     *
     * @param type       标识类型：PHONE 或 EMAIL。
     * @param identifier 标识值（需为标准化格式）。
     * @return 是否存在。
     */
    private boolean identifierExists(IdentifierType type, String identifier) {
        return switch (type) {
            case PHONE -> userService.existsByPhone(identifier);
            case EMAIL -> userService.existsByEmail(identifier);
        };
    }

    /**
     * 根据标识查找用户。
     *
     * @param type       标识类型：PHONE 或 EMAIL。
     * @param identifier 标识值（需为标准化格式）。
     * @return 用户 Optional。
     */
    private Optional<User> findUserByIdentifier(IdentifierType type, String identifier) {
        return switch (type) {
            case PHONE -> userService.findByPhone(identifier);
            case EMAIL -> userService.findByEmail(identifier);
        };
    }

    /**
     * 根据 ID 查找用户。
     *
     * @param userId 用户 ID。
     * @return 用户 Optional。
     */
    private Optional<User> findUserById(long userId) {
        return userService.findById(userId);
    }

    /**
     * 标准化标识文本：手机号去空格、邮箱转小写并去空格。
     *
     * @param type       标识类型：PHONE 或 EMAIL。
     * @param identifier 原始标识文本。
     * @return 标准化后的标识文本。
     */
    private String normalizeIdentifier(IdentifierType type, String identifier) {
        return switch (type) {
            case PHONE -> identifier.trim();
            case EMAIL -> identifier.trim().toLowerCase(Locale.ROOT);
        };
    }

    /**
     * 存储刷新令牌白名单记录。
     *
     * @param userId    用户 ID。
     * @param tokenPair 令牌对（含刷新令牌 ID 与过期时间）。
     */
    private void storeRefreshToken(Long userId, TokenPair tokenPair) {
    // 1. 计算刷新令牌的剩余有效时间（TTL - Time To Live）
    //    Duration.between()计算两个时间点之间的时间差
    //    Instant.now()：当前时间（UTC时间戳）
    //    tokenPair.refreshTokenExpiresAt()：刷新令牌的过期时间
    //    结果：从现在到过期时间的时长，例如：PT168H（168小时 = 7天）
    //    这个时长将作为Redis键的过期时间，实现自动过期机制
    Duration ttl = Duration.between(Instant.now(), tokenPair.refreshTokenExpiresAt());

    // 2. 处理边界情况：TTL为负数
    //    如果当前时间已经超过了令牌过期时间，TTL会变成负数
    //    例如：过期时间是昨天，TTL = -PT24H
    //    负数TTL会导致Redis操作失败，因此需要特殊处理
    //    将负数TTL设置为Duration.ZERO，表示立即过期
    //    这种情况可能发生在：
    //    - 系统时间不同步
    //    - 令牌生成和存储之间有较大延迟
    //    - 并发处理导致的时间竞争
    if (ttl.isNegative()) {
        ttl = Duration.ZERO;
    }

    // 3. 将刷新令牌存储到Redis白名单
    //    refreshTokenStore：刷新令牌存储服务，封装了Redis操作
    //    userId：用户ID，用于构建Redis键的一部分
    //    tokenPair.refreshTokenId()：刷新令牌的唯一标识符（jti声明）
    //    ttl：键的过期时间，与令牌过期时间保持一致
    //
    //    Redis键格式：auth:rt:{userId}:{refreshTokenId}
    //    例如：auth:rt:12345:550e8400-e29b-41d4-a716-446655440000
    //
    //    Redis值：固定为"1"，表示令牌有效
    //
    //    白名单机制的作用：
    //    - 令牌验证时检查白名单，只有白名单中的令牌才有效
    //    - 支持主动撤销令牌（从白名单中删除）
    //    - 支持撤销用户所有令牌（删除用户的所有白名单键）
    //    - 防止被盗用的刷新令牌继续使用
    //    - 实现令牌的黑名单/白名单管理
    //
    //    TTL自动过期机制：
    //    - Redis会在TTL到期后自动删除键
    //    - 无需手动清理过期的令牌
    //    - 与令牌的过期时间保持一致，确保白名单和令牌同步失效
    refreshTokenStore.storeToken(userId, tokenPair.refreshTokenId(), ttl);
}


    /**
     * 映射用户实体到响应对象。
     *
     * @param user 用户实体。
     * @return 用户响应。
     */
    private AuthUserResponse mapUser(User user) {
        return new AuthUserResponse(
                user.getId(),
                user.getNickname(),
                user.getAvatar(),
                user.getPhone(),
                user.getZgId(),
                user.getBirthday(),
                user.getSchool(),
                user.getBio(),
                user.getGender(),
                user.getTagsJson()
        );
    }

    /**
     * 映射令牌对到响应对象。
     *
     * @param tokenPair 令牌对。
     * @return 令牌响应。
     */
    private TokenResponse mapToken(TokenPair tokenPair) {
        return new TokenResponse(tokenPair.accessToken(), tokenPair.accessTokenExpiresAt(), tokenPair.refreshToken(), tokenPair.refreshTokenExpiresAt());
    }

    /**
     * 生成默认昵称。
     *
     * @return 随机昵称字符串。
     */
    private String generateNickname() {
        return "知光用户" + UUID.randomUUID().toString().substring(0, 8);
    }

    /**
     * 解码刷新令牌，失败时抛业务异常。
     *
     * @param refreshToken 刷新令牌字符串。
     * @return 解析得到的 JWT。
     * @throws BusinessException 当刷新令牌无法解析时抛出。
     */
    private Jwt decodeRefreshToken(String refreshToken) {
        try {
            return jwtService.decode(refreshToken);
        } catch (JwtException ex) {
            throw new BusinessException(ErrorCode.REFRESH_TOKEN_INVALID);
        }
    }

    /**
     * 安全解码刷新令牌，失败时返回空 Optional。
     *
     * @param refreshToken 刷新令牌字符串。
     * @return 成功时返回 JWT，失败时返回 Optional.empty()。
     */
    private Optional<Jwt> decodeRefreshTokenSafely(String refreshToken) {
        try {
            return Optional.of(jwtService.decode(refreshToken));
        } catch (JwtException ex) {
            return Optional.empty();
        }
    }
}