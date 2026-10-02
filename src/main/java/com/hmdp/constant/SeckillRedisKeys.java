package com.hmdp.constant;

/**
 * 秒杀链路 Redis Key 的统一定义与构造入口。
 */
public final class SeckillRedisKeys {

    public static final String VOUCHER_BEGIN_TIME_FIELD = "beginTime";
    public static final String VOUCHER_END_TIME_FIELD = "endTime";

    public static final long MQ_TRANSACTION_TTL_SECONDS = 60 * 60L;
    public static final long ORDER_SUCCESS_TTL_SECONDS = 60 * 60L;
    public static final long ORDER_FAILED_TTL_SECONDS = 24 * 60 * 60L;
    public static final long VOUCHER_KEY_GRACE_SECONDS = 60 * 60L;

    private SeckillRedisKeys() {
    }

    public static String stockKey(Long voucherId) {
        return "seckill:voucher:" + voucherId + ":stock";
    }

    public static String voucherTimeKey(Long voucherId) {
        return "seckill:voucher:" + voucherId + ":time";
    }

    public static String userOrderKey(Long voucherId, Long userId) {
        return "seckill:voucher:" + voucherId + ":user:" + userId + ":order";
    }

    public static String transactionKey(Long orderId) {
        return "seckill:order:" + orderId + ":mq-transaction-status";
    }

    public static String orderStateKey(Long orderId) {
        return "seckill:order:" + orderId + ":state";
    }

    public static String orderLockKey(Long orderId) {
        return "seckill:order:" + orderId + ":lock";
    }

    /** 同一用户对同一优惠券的秒杀请求限流 Key。 */
    public static String rateLimitUserKey(Long voucherId, Long userId) {
        return "seckill:voucher:" + voucherId + ":user:" + userId + ":rate";
    }

    /** 同一优惠券进入秒杀核心链路的全局限流 Key。 */
    public static String rateLimitVoucherKey(Long voucherId) {
        return "seckill:voucher:" + voucherId + ":rate";
    }
}
