package com.hmdp.service.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import java.lang.reflect.Field;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RedisRateLimitServiceImplTest {

    private final StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
    private final RedisRateLimitServiceImpl rateLimitService = new RedisRateLimitServiceImpl();

    @BeforeEach
    void setUp() throws Exception {
        Field field = RedisRateLimitServiceImpl.class.getDeclaredField("stringRedisTemplate");
        field.setAccessible(true);
        field.set(rateLimitService, redisTemplate);
    }

    @Test
    void shouldAllowWhenLuaReturnsOne() {
        when(redisTemplate.execute(
                any(RedisScript.class),
                eq(Collections.singletonList("rate:key")),
                eq("5"),
                eq("10"))).thenReturn(1L);

        assertTrue(rateLimitService.tryAcquire("rate:key", 5, 10));
    }

    @Test
    void shouldRejectWhenLuaReturnsZero() {
        when(redisTemplate.execute(
                any(RedisScript.class),
                eq(Collections.singletonList("rate:key")),
                eq("5"),
                eq("10"))).thenReturn(0L);

        assertFalse(rateLimitService.tryAcquire("rate:key", 5, 10));
    }

    @Test
    void shouldFailWhenLuaReturnsNull() {
        when(redisTemplate.execute(
                any(RedisScript.class),
                eq(Collections.singletonList("rate:key")),
                eq("5"),
                eq("10"))).thenReturn(null);

        assertThrows(
                IllegalStateException.class,
                () -> rateLimitService.tryAcquire("rate:key", 5, 10));
    }
}
