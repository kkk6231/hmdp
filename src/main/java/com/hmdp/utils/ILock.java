package com.hmdp.utils;

public interface ILock {

    /**
     * 尝试获取锁
     * @param expireTime 锁的过期时间，单位：秒
     * @return true表示获取锁成功，false表示获取锁失败
     */
    boolean tryLock(long expireTime);

    /**
     * 释放锁
     */
    void unlock();
}
