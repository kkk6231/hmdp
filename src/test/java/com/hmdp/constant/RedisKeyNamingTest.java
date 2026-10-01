package com.hmdp.constant;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RedisKeyNamingTest {

    @Test
    void buildsGeneralBusinessKeys() {
        assertEquals("auth:user:13800138000:code", AuthRedisKeys.code("13800138000"));
        assertEquals("auth:token:token-1:login", AuthRedisKeys.login("token-1"));
        assertEquals("user:user:7:sign:202609", UserRedisKeys.sign(7L, "202609"));
        assertEquals("user:user:7:following", UserRedisKeys.following(7L));
        assertEquals("shop:shop:10:cache", ShopRedisKeys.cache(10L));
        assertEquals("shop:shop:10:cache-lock", ShopRedisKeys.cacheLock(10L));
        assertEquals("shop:type:3:geo", ShopRedisKeys.geo(3));
        assertEquals("blog:blog:20:likes", BlogRedisKeys.likes(20L));
        assertEquals("blog:user:7:feed", BlogRedisKeys.feed(7L));
        assertEquals("id:order:20260930:sequence",
                IdRedisKeys.sequence("order", "20260930"));
    }

    @Test
    void buildsSeckillKeys() {
        assertEquals("seckill:voucher:10:stock", SeckillRedisKeys.stockKey(10L));
        assertEquals("seckill:voucher:10:time", SeckillRedisKeys.voucherTimeKey(10L));
        assertEquals("seckill:voucher:10:user:7:order",
                SeckillRedisKeys.userOrderKey(10L, 7L));
        assertEquals("seckill:voucher:10:user:7:rate",
                SeckillRedisKeys.rateLimitUserKey(10L, 7L));
        assertEquals("seckill:voucher:10:rate",
                SeckillRedisKeys.rateLimitVoucherKey(10L));
        assertEquals("seckill:order:11:mq-transaction-status",
                SeckillRedisKeys.transactionKey(11L));
        assertEquals("seckill:order:11:state", SeckillRedisKeys.orderStateKey(11L));
        assertEquals("seckill:order:11:lock", SeckillRedisKeys.orderLockKey(11L));
        assertEquals("seckill:order:pending", SeckillRedisKeys.ORDER_PENDING_KEY);
    }
}
