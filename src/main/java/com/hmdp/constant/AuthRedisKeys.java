package com.hmdp.constant;

/** 登录认证相关 Redis Key。 */
public final class AuthRedisKeys {

    public static final long CODE_TTL_MINUTES = 2L;
    public static final long LOGIN_TTL_MINUTES = 36000L;

    private AuthRedisKeys() {
    }

    public static String code(String phone) {
        return "auth:user:" + phone + ":code";
    }

    public static String login(String token) {
        return "auth:token:" + token + ":login";
    }
}
