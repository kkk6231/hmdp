package com.hmdp.service;

/**
 * 限流能力抽象，屏蔽底层计数算法与存储实现。
 */
public interface IRateLimitService {

    /**
     * 尝试获取一次请求资格。
     *
     * @param limitKey 限流 Key
     * @param maxCount 窗口内允许的最大请求数
     * @param windowSeconds 窗口大小，单位：秒
     * @return true 表示允许通过，false 表示已超过阈值
     */
    boolean tryAcquire(String limitKey, long maxCount, long windowSeconds);
}
