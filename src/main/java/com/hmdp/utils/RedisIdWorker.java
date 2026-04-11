package com.hmdp.utils;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import javax.annotation.Resource;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

@Component
public class RedisIdWorker {
    
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 开始时间戳
     */
    private static final long BEGIN_TIMESTAMP = 1640995200;

    /**
     * 时间戳左移位数（序列号长度）
     */
    private static final int COUNT_BITS = 32;

    // 为了增加ID的安全性，不直接使用Redis自增的数值，而是拼接一些其他信息
    // 全局唯一ID：符号位（1位）+ 时间戳（31位）+ 序列号（32位，Redis自增值）

    public long nextId(String keyPrefix) {
        // 1.生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        long timestamp = nowSecond - BEGIN_TIMESTAMP;
        // 2.生成序列号
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        // 自增序列号的Key由业务前缀 keyPrefix 加日期组成，即使是同一个业务，每过一天，就从1开始自增，防止序列号过大
        Long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + date);
        // 3.拼接并返回（先左移时间戳，再或运算拼接count值）
        return timestamp << COUNT_BITS | count;
    }

}
