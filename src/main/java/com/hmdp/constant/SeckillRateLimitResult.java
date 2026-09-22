package com.hmdp.constant;

/**
 * 秒杀两级限流 Lua 返回结果。
 */
public enum SeckillRateLimitResult {

    PASS(0),
    USER_LIMITED(1),
    VOUCHER_LIMITED(2);

    private final long code;

    SeckillRateLimitResult(long code) {
        this.code = code;
    }

    public long getCode() {
        return code;
    }

    public static SeckillRateLimitResult fromCode(long code) {
        for (SeckillRateLimitResult result : values()) {
            if (result.code == code) {
                return result;
            }
        }
        throw new IllegalArgumentException("未知的秒杀限流结果码：" + code);
    }
}
