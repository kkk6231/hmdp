package com.hmdp.constant;

/**
 * 秒杀资格预扣 Lua 脚本返回码。
 */
public final class SeckillLuaResultCode {

    public static final int SUCCESS = 0;
    public static final int OUT_OF_STOCK = 1;
    public static final int DUPLICATE_ORDER = 2;
    public static final int ACTIVITY_NOT_STARTED = 3;
    public static final int ACTIVITY_ENDED = 4;
    public static final int ACTIVITY_NOT_READY = 5;

    private SeckillLuaResultCode() {
    }
}
