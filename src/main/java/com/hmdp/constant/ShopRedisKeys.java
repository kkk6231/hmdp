package com.hmdp.constant;

/** 店铺相关 Redis Key。 */
public final class ShopRedisKeys {

    public static final long NULL_CACHE_TTL_MINUTES = 2L;
    public static final long CACHE_TTL_MINUTES = 30L;
    public static final long CACHE_LOCK_TTL_SECONDS = 10L;

    private ShopRedisKeys() {
    }

    public static String cache(Object shopId) {
        return "shop:shop:" + shopId + ":cache";
    }

    public static String cacheLock(Object shopId) {
        return "shop:shop:" + shopId + ":cache-lock";
    }

    public static String geo(Integer typeId) {
        return "shop:type:" + typeId + ":geo";
    }
}
