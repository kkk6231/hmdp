package com.hmdp.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** 秒杀接口限流规则，分别配置用户和优惠券维度的阈值与窗口。 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface SeckillRateLimit {

    long userMaxCount() default 5;

    long userWindowSeconds() default 10;

    long voucherMaxCount() default 500;

    long voucherWindowSeconds() default 1;
}
