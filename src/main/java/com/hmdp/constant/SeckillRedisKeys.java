package com.hmdp.constant;

/**
 * 秒杀链路 Redis Key 的统一定义与构造入口。
 */
public final class SeckillRedisKeys {

    private static final String VOUCHER_PREFIX = "seckill:voucher:";
    private static final String STOCK_SUFFIX = ":stock";
    private static final String META_SUFFIX = ":meta";
    private static final String USER_ORDER_SUFFIX = ":user-order";
    private static final String MQ_TRANSACTION_PREFIX = "seckill:mq:tx:";
    private static final String ORDER_RESULT_PREFIX = "seckill:order:result:";
    private static final String ORDER_LOCK_PREFIX = "lock:seckill:order:";
    private static final String RATE_LIMIT_USER_PREFIX = "seckill:rate:user:";
    private static final String RATE_LIMIT_VOUCHER_PREFIX = "seckill:rate:voucher:";

    public static final String ORDER_PENDING_KEY = "seckill:order:pending";
    public static final String VOUCHER_BEGIN_TIME_FIELD = "beginTime";
    public static final String VOUCHER_END_TIME_FIELD = "endTime";

    public static final long MQ_TRANSACTION_TTL_SECONDS = 60 * 60L;
    public static final long ORDER_SUCCESS_TTL_SECONDS = 60 * 60L;
    public static final long ORDER_FAILED_TTL_SECONDS = 24 * 60 * 60L;
    public static final long VOUCHER_KEY_GRACE_SECONDS = 60 * 60L;

    private SeckillRedisKeys() {
    }

    public static String stockKey(Long voucherId) {
        return VOUCHER_PREFIX + voucherId + STOCK_SUFFIX;
    }

    public static String voucherMetaKey(Long voucherId) {
        return VOUCHER_PREFIX + voucherId + META_SUFFIX;
    }

    public static String userOrderKey(Long voucherId) {
        return VOUCHER_PREFIX + voucherId + USER_ORDER_SUFFIX;
    }

    public static String transactionKey(Long orderId) {
        return MQ_TRANSACTION_PREFIX + orderId;
    }

    public static String orderResultKey(Long orderId) {
        return ORDER_RESULT_PREFIX + orderId;
    }

    public static String orderLockKey(Long orderId) {
        return ORDER_LOCK_PREFIX + orderId;
    }

    /**
     * 同一用户对同一优惠券的秒杀请求限流 Key。
     */
    public static String rateLimitUserKey(Long voucherId, Long userId) {
        return RATE_LIMIT_USER_PREFIX + voucherId + ":" + userId;
    }

    /**
     * 同一优惠券进入秒杀核心链路的全局限流 Key。
     */
    public static String rateLimitVoucherKey(Long voucherId) {
        return RATE_LIMIT_VOUCHER_PREFIX + voucherId;
    }
}
