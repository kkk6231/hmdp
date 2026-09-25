package com.hmdp.service.impl;

import com.hmdp.constant.SeckillRedisKeys;
import com.hmdp.constant.SeckillResultMessages;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SeckillVoucherServiceImplTest {

    private final StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
    @SuppressWarnings("unchecked")
    private final HashOperations<String, Object, Object> hashOperations = mock(HashOperations.class);
    @SuppressWarnings("unchecked")
    private final ValueOperations<String, String> valueOperations = mock(ValueOperations.class);
    private final SeckillVoucherServiceImpl service = new SeckillVoucherServiceImpl();

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(service, "stringRedisTemplate", redisTemplate);
        when(redisTemplate.opsForHash()).thenReturn(hashOperations);
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
    }

    @Test
    void validatesActivityUsingPreheatedTimeWindow() {
        long now = System.currentTimeMillis();
        Map<Object, Object> meta = new HashMap<>();
        meta.put(SeckillRedisKeys.VOUCHER_BEGIN_TIME_FIELD, String.valueOf(now - 60_000));
        meta.put(SeckillRedisKeys.VOUCHER_END_TIME_FIELD, String.valueOf(now + 60_000));
        when(hashOperations.entries(SeckillRedisKeys.voucherMetaKey(10L))).thenReturn(meta);

        assertNull(service.validateActivityTime(10L));
    }

    @Test
    void rejectsActivityWhenPreheatDataIsMissing() {
        when(hashOperations.entries(SeckillRedisKeys.voucherMetaKey(10L)))
                .thenReturn(new HashMap<>());

        Result result = service.validateActivityTime(10L);
        assertEquals(Boolean.FALSE, result.getSuccess());
        assertEquals(SeckillResultMessages.ACTIVITY_NOT_READY, result.getErrorMsg());
    }

    @Test
    void passesQualificationKeysInLuaContractOrder() {
        when(redisTemplate.execute(
                any(RedisScript.class),
                eq(Arrays.asList(
                        SeckillRedisKeys.stockKey(10L),
                        SeckillRedisKeys.userOrderKey(10L),
                        SeckillRedisKeys.transactionKey(11L),
                        SeckillRedisKeys.orderResultKey(11L),
                        SeckillRedisKeys.ORDER_PENDING_KEY,
                        SeckillRedisKeys.voucherMetaKey(10L))),
                eq("7"), eq("11"), eq("10"),
                eq(String.valueOf(SeckillRedisKeys.MQ_TRANSACTION_TTL_SECONDS)),
                any(String.class)))
                .thenReturn(0L);

        assertEquals(0L, service.reserveQualification(11L, 7L, 10L));
    }

    @Test
    void preheatsStockAndActivityWithRecoveryWindow() {
        LocalDateTime beginTime = LocalDateTime.of(2026, 9, 25, 10, 0);
        LocalDateTime endTime = beginTime.plusHours(1);
        SeckillVoucher voucher = new SeckillVoucher()
                .setVoucherId(10L)
                .setStock(5)
                .setBeginTime(beginTime)
                .setEndTime(endTime);

        service.preheatSeckillVoucher(voucher);

        String stockKey = SeckillRedisKeys.stockKey(10L);
        String metaKey = SeckillRedisKeys.voucherMetaKey(10L);
        verify(valueOperations).set(stockKey, "5");
        Map<String, String> meta = new HashMap<>();
        meta.put(SeckillRedisKeys.VOUCHER_BEGIN_TIME_FIELD,
                String.valueOf(beginTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()));
        meta.put(SeckillRedisKeys.VOUCHER_END_TIME_FIELD,
                String.valueOf(endTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()));
        verify(hashOperations).putAll(metaKey, meta);
        Date expireAt = Date.from(endTime.plusSeconds(SeckillRedisKeys.VOUCHER_KEY_GRACE_SECONDS)
                .atZone(ZoneId.systemDefault()).toInstant());
        verify(redisTemplate).expireAt(stockKey, expireAt);
        verify(redisTemplate).expireAt(metaKey, expireAt);
    }
}
