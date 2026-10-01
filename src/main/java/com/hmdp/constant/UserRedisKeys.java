package com.hmdp.constant;

/** 用户相关 Redis Key。 */
public final class UserRedisKeys {

    private UserRedisKeys() {
    }

    public static String sign(Long userId, String yearMonth) {
        return "user:user:" + userId + ":sign:" + yearMonth;
    }

    public static String following(Long userId) {
        return "user:user:" + userId + ":following";
    }
}
