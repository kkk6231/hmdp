package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.constant.ShopRedisKeys;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
@Component
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public <R,ID> R queryWithPassThrough(
            String key, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit){
        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(json)) {
            return JSONUtil.toBean(json, type);
        }
        if (json != null) {
            return null;
        }

        R r = dbFallback.apply(id);
        if (r == null) {
            // 缓存空值，防止不存在的数据被反复查询。
            stringRedisTemplate.opsForValue().set(
                    key, "", ShopRedisKeys.NULL_CACHE_TTL_MINUTES, TimeUnit.MINUTES);
            return null;
        }
        this.set(key, r, time, unit);
        return r;
    }

    public <R, ID> R queryWithLogicalExpire(
            String key, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isBlank(json)) {
            return null;
        }
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        if(expireTime.isAfter(LocalDateTime.now())) {
            return r;
        }
        String lockKey = ShopRedisKeys.cacheLock(id);
        boolean isLock = tryLock(lockKey);
        if (isLock){
            // 获取锁后异步刷新，当前请求继续返回旧值。
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    R newR = dbFallback.apply(id);
                    this.setWithLogicalExpire(key, newR, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    unlock(lockKey);
                }
            });
        }
        return r;
    }

    public <R, ID> R queryWithMutex(
            String key, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, type);
        }
        if (shopJson != null) {
            return null;
        }

        String lockKey = ShopRedisKeys.cacheLock(id);
        R r = null;
        try {
            boolean isLock = tryLock(lockKey);
            if (!isLock) {
                // 获取锁失败时短暂等待，避免并发回源。
                Thread.sleep(50);
                return queryWithMutex(key, id, type, dbFallback, time, unit);
            }
            r = dbFallback.apply(id);
            if (r == null) {
                // 缓存空值，防止缓存穿透。
                stringRedisTemplate.opsForValue().set(
                        key, "", ShopRedisKeys.NULL_CACHE_TTL_MINUTES, TimeUnit.MINUTES);
                return null;
            }
            this.set(key, r, time, unit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            unlock(lockKey);
        }
        return r;
    }

    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(
                key, "1", ShopRedisKeys.CACHE_LOCK_TTL_SECONDS, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }
}
