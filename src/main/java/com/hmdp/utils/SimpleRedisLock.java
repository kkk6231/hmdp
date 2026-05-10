package com.hmdp.utils;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import cn.hutool.core.lang.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

public class SimpleRedisLock implements ILock {

    //锁的名称，用于区分不同业务
    private String name;
    private StringRedisTemplate stringRedisTemplate;

    //构造函数
    //用于初始化锁的名称和Redis模板
    public SimpleRedisLock(String name, StringRedisTemplate stringRedisTemplate) {
        this.name = name;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    //锁的key前缀
    private static final String LOCK_KEY_PREFIX = "lock:";
    // 区分不同服务（JVM）的标识
    private static final String ID_PREFIX = UUID.randomUUID().toString(true) + "-";
    // 声明脚本并初始化
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;
    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    /**
     * 获取锁
     * @param expireTime 锁的过期时间，单位：秒
     * @return
     */
    @Override
    public boolean tryLock(long expireTime) {
        // 构建锁的key
        String lockKey = LOCK_KEY_PREFIX + name;
        //获取当前线程的ID
        String threadId = ID_PREFIX + Thread.currentThread().getId();
        // 尝试获取锁
        Boolean result = stringRedisTemplate.opsForValue()
                .setIfAbsent(lockKey, threadId, expireTime, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(result);
    }

    /**
     * 调用lua脚本改造分布式锁，将判断锁和删除变为原子操作
     */
    @Override
    public void unlock() {
        // 构建锁的key
        String lockKey = LOCK_KEY_PREFIX + name;
        //获取当前线程的ID（带前缀）
        String threadId = ID_PREFIX + Thread.currentThread().getId();
        //调用LUA脚本
        stringRedisTemplate.execute(
                UNLOCK_SCRIPT,
                Collections.singletonList(lockKey),
                threadId
        );
    }

/*    @Override
    public void unlock() {
        // 构建锁的key
        String lockKey = LOCK_KEY_PREFIX + name;
        //获取当前线程的ID（带前缀）
        String threadId = ID_PREFIX + Thread.currentThread().getId();
        // 检查当前线程是否持有锁
        String currentValue = stringRedisTemplate.opsForValue().get(lockKey);
        if (currentValue != null && currentValue.equals(threadId)) {
            // 释放锁
            stringRedisTemplate.delete(lockKey);
        }
    }*/

}
