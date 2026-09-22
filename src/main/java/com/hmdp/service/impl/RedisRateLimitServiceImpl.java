package com.hmdp.service.impl;

import com.hmdp.constant.SeckillRateLimitResult;
import com.hmdp.service.IRateLimitService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Arrays;

/**
 * 基于 Redis Lua 固定窗口算法的限流实现。
 */
@Service
public class RedisRateLimitServiceImpl implements IRateLimitService {

    private static final Logger log = LoggerFactory.getLogger(RedisRateLimitServiceImpl.class);
    private static final DefaultRedisScript<Long> RATE_LIMIT_SCRIPT;

    static {
        RATE_LIMIT_SCRIPT = new DefaultRedisScript<>();
        RATE_LIMIT_SCRIPT.setLocation(new ClassPathResource("lua/seckill_rate_limit.lua"));
        RATE_LIMIT_SCRIPT.setResultType(Long.class);
    }

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public SeckillRateLimitResult tryAcquire(
            String userKey,
            long userMaxCount,
            long userWindowSeconds,
            String voucherKey,
            long voucherMaxCount,
            long voucherWindowSeconds) {
        validateArguments(
                userKey, userMaxCount, userWindowSeconds,
                voucherKey, voucherMaxCount, voucherWindowSeconds);

        Long resultCode = stringRedisTemplate.execute(
                RATE_LIMIT_SCRIPT,
                Arrays.asList(userKey, voucherKey),
                String.valueOf(userMaxCount),
                String.valueOf(userWindowSeconds),
                String.valueOf(voucherMaxCount),
                String.valueOf(voucherWindowSeconds));

        if (resultCode == null) {
            throw new IllegalStateException("执行秒杀限流 Lua 脚本失败");
        }

        SeckillRateLimitResult result;
        try {
            result = SeckillRateLimitResult.fromCode(resultCode);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("秒杀限流 Lua 返回未知结果：" + resultCode, e);
        }

        if (result != SeckillRateLimitResult.PASS) {
            log.warn("秒杀请求触发限流，result={}，userKey={}，voucherKey={}",
                    result, userKey, voucherKey);
        }
        return result;
    }

    private void validateArguments(
            String userKey,
            long userMaxCount,
            long userWindowSeconds,
            String voucherKey,
            long voucherMaxCount,
            long voucherWindowSeconds) {
        if (isBlank(userKey) || isBlank(voucherKey)) {
            throw new IllegalArgumentException("限流 Key 不能为空");
        }
        if (userMaxCount <= 0 || voucherMaxCount <= 0) {
            throw new IllegalArgumentException("限流最大请求数必须大于 0");
        }
        if (userWindowSeconds <= 0 || voucherWindowSeconds <= 0) {
            throw new IllegalArgumentException("限流窗口必须大于 0");
        }
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
