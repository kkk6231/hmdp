package com.hmdp.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 秒杀接口限流注解。
 *
 * <p>用户和 IP 两个维度拥有独立的阈值与统计窗口。</p>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface SeckillRateLimit {

    long userMaxCount() default 5;

    long userWindowSeconds() default 10;

    long ipMaxCount() default 50;

    long ipWindowSeconds() default 10;
}
