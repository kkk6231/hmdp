package com.hmdp.constant;

/** 博客及关注流相关 Redis Key。 */
public final class BlogRedisKeys {

    private BlogRedisKeys() {
    }

    public static String likes(Long blogId) {
        return "blog:blog:" + blogId + ":likes";
    }

    public static String feed(Long userId) {
        return "blog:user:" + userId + ":feed";
    }
}
