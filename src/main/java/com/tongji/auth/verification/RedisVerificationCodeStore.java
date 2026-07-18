package com.tongji.auth.verification;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

/**
 * 基于 Redis 的验证码存储实现。
 * <p>
 * 使用 Hash 结构保存 `code`、`maxAttempts` 与 `attempts`，TTL 控制有效期。
 * 校验时支持尝试计数与错误状态返回，成功后删除键以防重用。
 */
@Component
public class RedisVerificationCodeStore implements VerificationCodeStore {

    private static final String FIELD_CODE = "code";
    private static final String FIELD_MAX_ATTEMPTS = "maxAttempts";
    private static final String FIELD_ATTEMPTS = "attempts";

    private final StringRedisTemplate redisTemplate;

    public RedisVerificationCodeStore(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /**
     * 保存验证码到 Redis Hash，并设置 TTL。
     *
     * @param scene       场景名称。
     * @param identifier  标识（手机号或邮箱）。
     * @param code        验证码字符串。
     * @param ttl         有效期。
     * @param maxAttempts 最大尝试次数。
     * @throws RedisSystemException 保存失败时抛出。
     */
@Override
public void saveCode(String scene, String identifier, String code, Duration ttl, int maxAttempts) {
    // 根据场景和标识符构建Redis键，格式通常为 "场景:标识符"
    // 例如：REGISTER:13812345678 或 LOGIN:user@example.com
    // 这样可以将不同场景和用户的验证码隔离存储
    String key = buildKey(scene, identifier);

    // 获取Redis Hash操作对象，用于操作Hash数据结构
    // Hash结构允许我们在同一个键下存储多个字段，适合存储验证码相关的多个属性
    HashOperations<String, String, String> ops = redisTemplate.opsForHash();

    try {
        // 将验证码字符串保存到Hash的"code"字段中
        // FIELD_CODE = "code"，这是实际用于校验的验证码值
        //Redis 的三种层次
        //第一层：Redis 的全局字典，通过 key 找到对应的数据结构（String、Hash、List、Set、ZSet 等）。
        //第二层：如果这个 key 背后的数据结构是 Hash，那么它内部又维护了一个 field→value 的映射表。
        //第三层：value 只能是字符串（或能序列化为字符串的数据），不能再嵌套 Hash 或 List。
        ops.put(key, FIELD_CODE, code);

        // 将最大尝试次数保存到Hash的"maxAttempts"字段中
        // FIELD_MAX_ATTEMPTS = "maxAttempts"，用于限制用户尝试次数
        // 需要将int类型转换为String类型存储
        ops.put(key, FIELD_MAX_ATTEMPTS, String.valueOf(maxAttempts));

        // 初始化当前尝试次数为0，保存到Hash的"attempts"字段中
        // FIELD_ATTEMPTS = "attempts"，记录用户已尝试的次数
        // 每次验证失败时这个值会递增，达到maxAttempts时验证码失效
        ops.put(key, FIELD_ATTEMPTS, "0");

        // 设置整个Hash键的过期时间（TTL）
        // 验证码在指定时间后自动失效，提高安全性
        // 例如：Duration.ofMinutes(5) 表示5分钟后过期
//        expire 方法为 Redis 中的某个 key 设置生存时间
        redisTemplate.expire(key, ttl);

    } catch (DataAccessException ex) {
        // 捕获Redis数据访问异常，可能是网络问题、Redis服务不可用等
        // 将底层异常包装为RedisSystemException抛出，便于上层统一处理
        // 保持异常链，方便问题排查
        throw new RedisSystemException("Failed to save verification code", ex);
    }
}


    /**
     * 校验验证码是否匹配，更新尝试计数并在成功时删除记录。
     * @param scene      场景名称。
     * @param identifier 标识（手机号或邮箱）。
     * @param code       用户输入的验证码。
     * @return 校验结果（成功、未找到、错误、尝试过多）。
     */
    @Override
    public VerificationCheckResult verify(String scene, String identifier, String code) {
        // 1. 构建Redis键名：根据场景和标识符生成唯一的键名
        //    格式为：auth:code:{场景}:{标识符}
        //    例如：auth:code:LOGIN:13800138000
        String key = buildKey(scene, identifier);

        // 2. 获取Redis Hash操作对象：用于操作Hash数据结构
        //    Hash结构允许我们在同一个键下存储多个字段
        HashOperations<String, String, String> ops = redisTemplate.opsForHash();

        // 3. 从Redis中获取验证码的所有字段数据
        //    返回的Map包含：code（验证码）、maxAttempts（最大尝试次数）、attempts（当前尝试次数）
        Map<String, String> data = ops.entries(key);

        // 4. 检查验证码是否存在：如果数据为空，说明验证码不存在或已过期
        //    可能原因：验证码已过期（TTL到期）、验证码已被删除、验证码从未生成
        if (data.isEmpty()) {
            return new VerificationCheckResult(VerificationCodeStatus.NOT_FOUND, 0, 0);
        }

        // 5. 从Hash数据中提取各个字段的值
        String storedCode = data.get(FIELD_CODE);              // 存储的验证码值
        int maxAttempts = parseInt(data.get(FIELD_MAX_ATTEMPTS), 5);  // 最大尝试次数，默认为5
        int attempts = parseInt(data.get(FIELD_ATTEMPTS), 0);         // 当前已尝试次数，默认为0

        // 6. 检查尝试次数是否已超过限制：防止暴力破解
        //    如果当前尝试次数 >= 最大尝试次数，直接返回失败
        if (attempts >= maxAttempts) {
            return new VerificationCheckResult(VerificationCodeStatus.TOO_MANY_ATTEMPTS, attempts, maxAttempts);
        }

        // 7. 比对验证码：使用Objects.equals进行安全的字符串比较
        //    避免空指针异常，同时比较两个字符串的内容是否相等
        if (Objects.equals(storedCode, code)) {
            // 7.1 验证成功：删除Redis中的验证码记录
            //    防止验证码被重复使用，提高安全性
            redisTemplate.delete(key);
            // 7.2 返回成功结果：包含验证成功的状态和尝试次数统计
            return new VerificationCheckResult(VerificationCodeStatus.SUCCESS, attempts, maxAttempts);
        }

        // 8. 验证失败处理：增加尝试次数
        int updatedAttempts = attempts + 1;
        // 8.1 更新Redis中的尝试次数：将新的尝试次数写回Hash结构
        ops.put(key, FIELD_ATTEMPTS, String.valueOf(updatedAttempts));

        // 8.2 检查更新后的尝试次数是否达到上限
        if (updatedAttempts >= maxAttempts) {
            // 8.2.1 设置额外的过期时间：防止验证码记录长期占用Redis内存
            //    当尝试次数达到上限时，设置30分钟的过期时间
            //    这样即使验证码本身还有效，也会在30分钟后自动清理
            //TODO 这里很多不是原子操作，额外30分钟存疑？
            redisTemplate.expire(key, Duration.ofMinutes(30));
            // 8.2.2 返回尝试次数过多的结果
            return new VerificationCheckResult(VerificationCodeStatus.TOO_MANY_ATTEMPTS, updatedAttempts, maxAttempts);
        }

        // 9. 验证码不匹配但未超限：返回不匹配结果
        //    用户可以继续尝试，直到达到最大尝试次数
        return new VerificationCheckResult(VerificationCodeStatus.MISMATCH, updatedAttempts, maxAttempts);
    }

    /**
     * 使验证码失效（删除存储记录）。
     *
     * @param scene      场景名称。
     * @param identifier 标识（手机号或邮箱）。
     */
    @Override
    public void invalidate(String scene, String identifier) {
        redisTemplate.delete(buildKey(scene, identifier));
    }

    /**
     * 生成验证码的 Redis 键名。
     *
     * @param scene      场景名称。
     * @param identifier 标识（手机号或邮箱）。
     * @return 键名字符串。
     */
    private static String buildKey(String scene, String identifier) {
        return "auth:code:%s:%s".formatted(scene, identifier);
    }

    /**
     * 解析整数字符串，失败返回默认值。
     *
     * @param value        待解析字符串。
     * @param defaultValue 解析失败时的默认值。
     * @return 整数值。
     */
    private static int parseInt(String value, int defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException ex) {
            return defaultValue;
        }
    }
}