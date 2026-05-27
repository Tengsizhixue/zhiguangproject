package com.tongji.auth.verification;

import lombok.RequiredArgsConstructor;
import com.tongji.auth.config.AuthProperties;
import com.tongji.common.exception.BusinessException;
import com.tongji.common.exception.ErrorCode;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.security.SecureRandom;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * 验证码业务服务。
 * <p>
 * 负责发送与校验验证码：
 * - 速率限制与日限额；
 * - 随机码生成与存储；
 * - 调用发送器进行实际发送；
 * 配置来源于 `AuthProperties.Verification`。
 */
@Service
@RequiredArgsConstructor
public class VerificationService {

    private static final SecureRandom RANDOM = new SecureRandom();
    private static final DateTimeFormatter DAY_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");

    private final VerificationCodeStore codeStore;
    private final CodeSender codeSender;
    private final StringRedisTemplate stringRedisTemplate;
    private final AuthProperties properties;

    /**
     * 发送验证码到指定标识。
     * <p>
     * 执行发送间隔与日次数限制，生成随机数字验证码，保存到存储并调用发送器。
     *
     * @param scene      验证码场景（REGISTER/LOGIN/RESET_PASSWORD）。
     * @param identifier 标识（手机号或邮箱）。
     * @return 发送结果，包含标识、场景与过期秒数。
     * @throws BusinessException 参数不完整或触发速率/日限额时抛出。
     */
    public SendCodeResult sendCode(VerificationScene scene, String identifier) {
        // 1. 参数校验：检查场景和标识是否为空
        //    - scene不能为null，identifier不能为空或空白字符串
        //    - 如果参数无效，抛出业务异常，提示用户提供正确的参数
        // 等价于检查以下任一情况：
// 1. identifier == null
// 2. identifier.equals("")
// 3. identifier.trim().equals("")
        if (scene == null || !StringUtils.hasText(identifier)) {
            throw new BusinessException(ErrorCode.BAD_REQUEST, "请提供正确的验证码发送参数");
        }

        // 2. 获取验证码配置：从配置文件中读取验证码相关配置
        //    - 包含验证码长度、有效期、发送间隔、每日限额等参数
        //    - 这些配置决定了验证码的安全性和用户体验
        AuthProperties.Verification cfg = properties.getVerification();

        // 3. 强制执行发送间隔限制：防止短时间内重复发送验证码
        //    - 检查该标识在指定场景下是否在冷却期内
        //    - 如果在冷却期内，抛出异常提示用户稍后再试
        //    - 防止验证码轰炸攻击，保护系统安全
        enforceSendInterval(scene, identifier, cfg.getSendInterval());

        // 4. 强制执行每日发送限额：防止恶意大量发送验证码
        //    - 检查该标识在指定场景下今日已发送次数
        //    - 如果超过每日限额，抛出异常提示用户明日再试
        //    - 保护短信/邮件服务资源，控制成本
        enforceDailyLimit(scene, identifier, cfg.getDailyLimit());

        // 5. 生成随机数字验证码：根据配置的长度生成验证码
        //    - 使用随机数生成器生成指定位数的数字验证码
        //    - 验证码长度通常为4-6位，平衡安全性和用户体验
        String code = generateNumericCode(cfg.getCodeLength());

        // 6. 保存验证码到存储：将验证码信息存储到缓存或数据库
        //    - 存储场景名称、标识、验证码、有效期、最大尝试次数
        //    - 用于后续验证码校验，确保验证码的正确性和时效性
        //    - 记录最大尝试次数，防止暴力破解
        codeStore.saveCode(scene.name(), identifier, code, cfg.getTtl(), cfg.getMaxAttempts());

        // 7. 发送验证码：调用验证码发送器实际发送验证码
        //    - 根据标识类型（手机号/邮箱）选择合适的发送方式
        //    - 将验证码发送到用户的手机或邮箱
        //    - 传入过期时间（分钟），方便用户了解验证码有效期
        codeSender.sendCode(scene, identifier, code, (int) cfg.getTtl().toMinutes());

        // 8. 构造并返回发送结果：封装发送结果返回给调用方
        //    - 包含标识、场景、过期时间（秒）
        //    - 调用方可以根据返回结果进行后续处理，如显示倒计时
        return new SendCodeResult(identifier, scene, (int) cfg.getTtl().toSeconds());
    }


    /**
     * 校验验证码是否正确且未超限。
     *
     * @param scene      验证码场景。
     * @param identifier 标识（手机号或邮箱）。
     * @param code       用户输入的验证码。
     * @return 校验结果，包含状态与尝试次数统计。
     * @throws BusinessException 参数不完整时抛出。
     */
    public VerificationCheckResult verify(VerificationScene scene, String identifier, String code) {
        if (scene == null || !StringUtils.hasText(identifier) || !StringUtils.hasText(code)) {
            throw new BusinessException(ErrorCode.BAD_REQUEST, "验证码校验参数不完整");
        }
        return codeStore.verify(scene.name(), identifier, code);
    }

    /**
     * 使验证码失效（删除存储记录）。
     *
     * @param scene      验证码场景。
     * @param identifier 标识（手机号或邮箱）。
     */
    public void invalidate(VerificationScene scene, String identifier) {
        codeStore.invalidate(scene.name(), identifier);
    }

    /**
     * 发送间隔限制：同一标识在指定间隔内只能发送一次。
     *
     * @param scene      验证码场景。
     * @param identifier 标识（手机号或邮箱）。
     * @param interval   发送间隔。
     */

    //Duration 是Java 8在 java.time 包中引入的类，用于表示时间段的长度。
    private void enforceSendInterval(VerificationScene scene, String identifier, Duration interval) {
        // 检查间隔时间是否有效：如果间隔时间为0或负数，说明不需要限制，直接返回
//        为了配置灵活性：
//        interval = 0：不限制发送间隔（开发测试环境）
//        interval = -1：不限制发送间隔（特殊场景）
//        interval = 60s：正常限制（生产环境）
        if (interval.isZero() || interval.isNegative()) {
            return;
        }

        // 构建Redis缓存key，格式为：auth:code:last:场景名:标识符
        // 用于记录该场景下该标识符最后一次发送验证码的时间
        String key = "auth:code:last:" + scene.name() + ":" + identifier;

        // 如果 key 不存在，Redis 会写入 "1" 并设置过期时间，返回 true
        // 如果 key 已存在，Redis 什么都不做，直接返回 false
//        为什么 value 只用 "1" 就够了？
//        因为在这个限频场景里，你只关心某个 key 是否存在，而不关心存的具体内容是什么。
//        opsForValue() 作用：获取字符串操作的对象，只提供方法，你需要通过 key 参数指定要操作哪个键。
        Boolean isSuccess = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", interval);

        // 如果Redis中存在该key，说明在间隔时间内重复发送，抛出频率限制异常
        if (Boolean.FALSE.equals(isSuccess)) {
            throw new BusinessException(ErrorCode.VERIFICATION_RATE_LIMIT);
        }

    }


    /**
     * 每日发送次数限制：超过上限则抛出限额异常。
     *
     * @param scene      验证码场景。
     * @param identifier 标识（手机号或邮箱）。
     * @param limit      每日上限次数。
     */
private void enforceDailyLimit(VerificationScene scene, String identifier, int limit) {
    // 1. 检查限制参数：如果每日限制小于等于0，表示不限制发送次数
    //    - limit <= 0：配置为不限制，直接返回，跳过后续检查
    //    - limit > 0：需要进行每日发送次数限制
    if (limit <= 0) {
        return;
    }

    // 2. 生成当前日期字符串：使用格式化器将当前日期转换为yyyyMMdd格式
    //    - 例如：2025年5月27日转换为"20250527"
    //    - 用于区分不同日期的发送计数，实现每日重置
    //    - DAY_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd")
    String date = DAY_FORMAT.format(LocalDate.now());

    // 3. 构造Redis计数键：格式为 auth:code:count:场景名:标识符:日期
    //    - 场景名：REGISTER/LOGIN/RESET_PASSWORD
    //    - 标识符：手机号或邮箱
    //    - 日期：yyyyMMdd格式的当前日期
    //    - 示例：auth:code:count:REGISTER:13800138000:20250527
    //    - 不同场景、不同标识、不同日期都有独立的计数
    String key = "auth:code:count:" + scene.name() + ":" + identifier + ":" + date;

    // 4. 原子递增计数：使用Redis的INCR命令原子性地增加计数器
    //    - 如果键不存在，自动创建并初始化为1
    //    - 如果键存在，值加1
    //    - 返回递增后的新值
    //    - 原子操作确保并发安全，避免计数错误
    /*隐患：非原子操作导致的 Redis 内存泄漏（僵尸 Key）
动作 A（递增）和 动作 B（设置过期时间）是两次独立的网络请求。
假设今天用户第一次发短信，系统刚执行完动作 A（Redis 里生成了 count=1），突然！就在这一毫秒，你的 Java 服务器宕机了、重启了，或者网络断了。
动作 B 就永远不会被执行了。
结果就是：Redis 里留下了一个永远没有过期时间的 Key（...:2023-10-25）。
因为到了 10 月 26 号代码会自动去查新的 Key，旧的 Key 再也没有人去访问，它就变成了“僵尸 Key”，一直吃着 Redis 的宝贵内存。日积月累，Redis 内存可能会被撑爆。*/
    Long count = stringRedisTemplate.opsForValue().increment(key);

    // 5. 设置过期时间：如果是第一次创建该键（count == 1），则设置过期时间
    //    - 只在第一次创建时设置过期时间，避免重复设置
    //    - 过期时间为1天，确保第二天自动重置计数
    //    - 利用Redis的TTL机制实现每日自动清理
    if (count != null && count == 1L) {
        stringRedisTemplate.expire(key, Duration.ofDays(1));
    }

    // 6. 检查是否超限：如果当前计数超过每日限制，抛出异常
    //    - count > limit：今日发送次数已超过配置的上限
    //    - 抛出业务异常，提示用户已达到每日发送限制
    //    - 保护系统资源，防止恶意大量发送验证码
    if (count != null && count > limit) {
        throw new BusinessException(ErrorCode.VERIFICATION_DAILY_LIMIT);
    }
}


    /**
     * 生成指定长度的纯数字验证码。
     *
     * @param length 验证码长度。
     * @return 数字字符串。
     */
    private static String generateNumericCode(int length) {
        StringBuilder builder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            builder.append(RANDOM.nextInt(10));
        }
        return builder.toString();
    }
}
