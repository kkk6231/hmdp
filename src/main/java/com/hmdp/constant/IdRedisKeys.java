package com.hmdp.constant;

/** 全局 ID 生成相关 Redis Key。 */
public final class IdRedisKeys {

    private IdRedisKeys() {
    }

    public static String sequence(String entity, String date) {
        return "id:" + entity + ":" + date + ":sequence";
    }
}
