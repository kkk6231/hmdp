package com.hmdp.service.impl;

import com.hmdp.constant.SeckillRateLimitResult;
import com.hmdp.constant.SeckillRedisKeys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RedisRateLimitServiceImplTest {

    private static final String USER_KEY = "seckill:rate:user:10:7";
    private static final String VOUCHER_KEY = "seckill:rate:voucher:10";
    private static final List<String> KEYS = Arrays.asList(USER_KEY, VOUCHER_KEY);

    private final StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
    private final RedisRateLimitServiceImpl rateLimitService = new RedisRateLimitServiceImpl();

    @BeforeEach
    void setUp() throws Exception {
        Field field = RedisRateLimitServiceImpl.class.getDeclaredField("stringRedisTemplate");
        field.setAccessible(true);
        field.set(rateLimitService, redisTemplate);
    }

    @Test
    void shouldReturnPassWhenLuaReturnsZero() {
        mockLuaResult(0L);
        assertEquals(SeckillRateLimitResult.PASS, tryAcquire());
    }

    @Test
    void shouldReturnUserLimitedWhenLuaReturnsOne() {
        mockLuaResult(1L);
        assertEquals(SeckillRateLimitResult.USER_LIMITED, tryAcquire());
    }

    @Test
    void shouldReturnVoucherLimitedWhenLuaReturnsTwo() {
        mockLuaResult(2L);
        assertEquals(SeckillRateLimitResult.VOUCHER_LIMITED, tryAcquire());
    }

    @Test
    void shouldFailWhenLuaReturnsNull() {
        mockLuaResult(null);
        assertThrows(IllegalStateException.class, this::tryAcquire);
    }

    @Test
    void shouldFailWhenLuaReturnsUnknownCode() {
        mockLuaResult(3L);
        assertThrows(IllegalStateException.class, this::tryAcquire);
    }

    @Test
    void differentUsersAndVouchersShouldUseIndependentKeys() {
        assertNotEquals(
                SeckillRedisKeys.rateLimitUserKey(10L, 7L),
                SeckillRedisKeys.rateLimitUserKey(10L, 8L));
        assertNotEquals(
                SeckillRedisKeys.rateLimitVoucherKey(10L),
                SeckillRedisKeys.rateLimitVoucherKey(11L));
    }

    private SeckillRateLimitResult tryAcquire() {
        return rateLimitService.tryAcquire(USER_KEY, 5, 10, VOUCHER_KEY, 3, 1);
    }

    private void mockLuaResult(Long result) {
        when(redisTemplate.execute(
                any(RedisScript.class),
                eq(KEYS),
                eq("5"),
                eq("10"),
                eq("3"),
                eq("1"))).thenReturn(result);
    }
}
