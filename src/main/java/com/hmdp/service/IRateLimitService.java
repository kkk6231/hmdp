package com.hmdp.service;

import com.hmdp.constant.SeckillRateLimitResult;

/**
 * 限流能力抽象，屏蔽底层计数算法与存储实现。
 */
public interface IRateLimitService {

    /**
     * 尝试获取一次请求资格。
     *
     * @param userKey 用户维度限流 Key
     * @param userMaxCount 用户窗口内允许的最大请求数
     * @param userWindowSeconds 用户窗口大小，单位：秒
     * @param voucherKey voucher 维度限流 Key
     * @param voucherMaxCount voucher 窗口内允许的最大请求数
     * @param voucherWindowSeconds voucher 窗口大小，单位：秒
     * @return 两级限流判定结果
     */
    SeckillRateLimitResult tryAcquire(
            String userKey,
            long userMaxCount,
            long userWindowSeconds,
            String voucherKey,
            long voucherMaxCount,
            long voucherWindowSeconds);
}
