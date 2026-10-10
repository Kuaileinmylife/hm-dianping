package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
public class CacheClient {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        // 设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));

        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    // 缓存穿透
    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit){
        String key = keyPrefix + id;
        // 从redis查询店铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);

        // 判断是否存在
        if(StrUtil.isNotBlank(json)){
            // 存在，直接返回
            return JSONUtil.toBean(json, type);
        }

        // 判断查的缓存是不是空值
        if(json != null){
            return null;
        }

        // 不存在，根据id查数据库
        R r = dbFallback.apply(id);

        // 不存在，返回错误
        if(r == null){
            // 为了避免缓存穿透还要将null值写入缓存
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);

            return null;
        }

        // 存在写入redis
        this.set(key, r, time, unit);

        // 返回
        return r;
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    // 逻辑过期：已有数据过期时返回旧数据，后台重建；缓存缺失时加锁回源。
    public <R, ID> R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type,
            Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        String lockKey = "lock:" + key;
        while (true) {
            String json = stringRedisTemplate.opsForValue().get(key);
            if (json != null && StrUtil.isBlank(json)) {
                return null; // 命中缓存的空值，防止缓存穿透
            }

            RedisData redisData = StrUtil.isNotBlank(json)
                    ? JSONUtil.toBean(json, RedisData.class) : null;
            // 旧版普通 JSON 没有逻辑过期时间，也通过回源重新生成缓存。
            if (redisData == null || redisData.getExpireTime() == null) {
                String token = tryLock(lockKey);
                if (token == null) {
                    pauseBeforeRetry();
                    continue;
                }
                try {
                    // 拿锁后再检查一次，避免重复查询数据库。
                    String latest = stringRedisTemplate.opsForValue().get(key);
                    if (!java.util.Objects.equals(json, latest)) {
                        continue;
                    }
                    return rebuild(key, id, dbFallback, time, unit);
                } finally {
                    unlock(lockKey, token);
                }
            }

            R result = JSONUtil.toBean((JSONObject) redisData.getData(), type);
            if (redisData.getExpireTime().isAfter(LocalDateTime.now())) {
                return result;
            }

            String token = tryLock(lockKey);
            if (token != null) {
                try {
                    CACHE_REBUILD_EXECUTOR.submit(() -> {
                        try {
                            // 获取锁后再检查缓存，可能已有其他请求完成了重建。
                            String latest = stringRedisTemplate.opsForValue().get(key);
                            if (java.util.Objects.equals(json, latest)) {
                                rebuild(key, id, dbFallback, time, unit);
                            }
                        } catch (Exception e) {
                            log.error("缓存重建失败，key={}", key, e);
                        } finally {
                            // 后台任务完成后才释放锁。
                            unlock(lockKey, token);
                        }
                    });
                } catch (RuntimeException e) {
                    unlock(lockKey, token); // 提交任务失败时释放锁
                    throw e;
                }
            }
            return result;
        }
    }

    private <R, ID> R rebuild(String key, ID id, Function<ID, R> dbFallback,
            Long time, TimeUnit unit) {
        R data = dbFallback.apply(id);
        if (data == null) {
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
        } else {
            setWithLogicalExpire(key, data, time, unit);
        }
        return data;
    }

    private void pauseBeforeRetry() {
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("等待缓存重建时被中断", e);
        }
    }

    private String tryLock(String key) {
        String token = UUID.randomUUID().toString();
        Boolean acquired = stringRedisTemplate.opsForValue()
                .setIfAbsent(key, token, LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(acquired) ? token : null;
    }

    // 原子地检查锁归属再删除，防止锁过期后误删其他请求的新锁。
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT = new DefaultRedisScript<>(
            "if redis.call('get', KEYS[1]) == ARGV[1] then "
                    + "return redis.call('del', KEYS[1]) else return 0 end", Long.class);

    private void unlock(String key, String token) {
        stringRedisTemplate.execute(UNLOCK_SCRIPT, Collections.singletonList(key), token);
    }
}