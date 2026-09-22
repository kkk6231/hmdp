package com.hmdp.service.impl;

import com.hmdp.service.IRateLimitService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;

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
    public boolean tryAcquire(String limitKey, long maxCount, long windowSeconds) {
        validateArguments(limitKey, maxCount, windowSeconds);

        Long result = stringRedisTemplate.execute(
                RATE_LIMIT_SCRIPT,
                Collections.singletonList(limitKey),
                String.valueOf(maxCount),
                String.valueOf(windowSeconds));

        if (result == null) {
            throw new IllegalStateException("执行秒杀限流 Lua 脚本失败");
        }

        boolean allowed = result == 1L;
        if (!allowed) {
            log.warn("秒杀请求触发限流，limitKey={}，maxCount={}，windowSeconds={}",
                    limitKey, maxCount, windowSeconds);
        }
        return allowed;
    }

    private void validateArguments(String limitKey, long maxCount, long windowSeconds) {
        if (limitKey == null || limitKey.trim().isEmpty()) {
            throw new IllegalArgumentException("限流 Key 不能为空");
        }
        if (maxCount <= 0) {
            throw new IllegalArgumentException("限流最大请求数必须大于 0");
        }
        if (windowSeconds <= 0) {
            throw new IllegalArgumentException("限流窗口必须大于 0");
        }
    }
}
