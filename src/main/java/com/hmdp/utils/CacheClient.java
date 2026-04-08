package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Component
public class CacheClient {

    private StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     * 普通缓存
     * @param key
     * @param value
     * @param timeOut
     * @param timeUnit
     */
    public void set(String key, Object value, Long timeOut, TimeUnit timeUnit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), timeOut, timeUnit);
    }

    /**
     * 带逻辑过期的缓存
     * @param key
     * @param value
     * @param timeOut
     * @param timeUnit
     */
    public void setWithExpire(String key, Object value, Long timeOut, TimeUnit timeUnit) {
        // 封装RedisData
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(timeUnit.toSeconds(timeOut))); //确保转为秒
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    /**
     * 根据指定的key查询缓存，并反序列化为指定类型，利用缓存空值的方式解决缓存穿透问题
     * @param keyPrefix
     * @param id
     * @param type
     * @param dbFallback
     * @param timeOut
     * @param timeUnit
     * @return
     * @param <T>
     * @param <ID>
     */
    public <T, ID> T queryWithPassThrough(String keyPrefix, ID id, Class<T> type,
                                          Function<ID, T> dbFallback, Long timeOut, TimeUnit timeUnit) {
        String key = keyPrefix + id;
        // 1.从redis查询缓存（JSON格式）
        String Json = stringRedisTemplate.opsForValue().get(key);

        T t = null;
        // 2.判断是否存在
        if (StrUtil.isNotBlank(Json)) {
            // 2.1如果存在，直接返回（先反序列化为Shop对象）
            return JSONUtil.toBean(Json, type);
        }
        // 2.2判断是null还是空字符串
        if (Json != null) {
            // 如果不是null，只能是空字符串，返回不存在
            return null;
        }
        // 3.如果Json为null，说明缓存中无记录，查询数据库
        t = dbFallback.apply(id);
        // 3.1当数据库中不存在此记录时，在redis中加入一个空对象（防止缓存穿透）
        if (t == null) {
            // 缓存空值
            stringRedisTemplate.opsForValue().set(key,"", RedisConstants.CACHE_NULL_TTL ,TimeUnit.MINUTES);
            return null;
        }
        // 3.2若存在，存入redis
        set(key, t, timeOut, timeUnit);
        // 4.返回查询结果
        return t;
    }

    /**
     * 根据指定的key查询缓存，并反序列化为指定类型，利用逻辑过期的方式解决缓存击穿问题
     * @param keyPrefix
     * @param id
     * @param type
     * @param dbFallback
     * @param timeOut
     * @param timeUnit
     * @return
     * @param <T>
     * @param <ID>
     */
    public <T, ID> T queryWithLogicExpire(String keyPrefix, ID id, Class<T> type,
                                        Function<ID, T> dbFallback, Long timeOut, TimeUnit timeUnit) {
        String key = keyPrefix + id;
        String JSON = stringRedisTemplate.opsForValue().get(key);
        // 如果为空，说明请求的数据不存在
        if (StrUtil.isBlank(JSON)) {
            return null;
        }
        // 如果非空，判断是否过期
        // 先反序列化
        RedisData redisData = JSONUtil.toBean(JSON, RedisData.class);
        JSONObject data = (JSONObject)redisData.getData();
        T t = JSONUtil.toBean(data, type);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())) {
            // 未过期，直接返回
            return t;
        }
        // 已过期，需要缓存重建
        // 获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        if(isLock) {
            //开启一个新的线程，进行缓存重建，当前线程直接返回老数据
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    // 查询数据库
                    T t1 = dbFallback.apply(id);
                    // 存入Redis
                    this.setWithExpire(key, t1, timeOut, timeUnit);
                } finally {
                    unlock(lockKey);
                }
            });
        }
        return t;
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    /**
     * 获取互斥锁
     * @param key
     * @return
     */
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "", 10, TimeUnit.SECONDS);//setnx命令
        return BooleanUtil.isTrue(flag); //包装类拆箱
    }

    /**
     * 释放锁
     * @param key
     */
    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }

}
