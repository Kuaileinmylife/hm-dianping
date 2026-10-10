package com.hmdp.utils;

import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class CacheClientTest {
    private CacheClient client;
    private StringRedisTemplate redis;
    private ValueOperations<String, String> values;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        redis = mock(StringRedisTemplate.class);
        values = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        when(values.setIfAbsent(anyString(), anyString(), anyLong(), any(TimeUnit.class)))
                .thenReturn(true);
        client = new CacheClient();
        ReflectionTestUtils.setField(client, "stringRedisTemplate", redis);
    }

    private Shop shop(String name) {
        Shop shop = new Shop();
        shop.setId(1L);
        shop.setName(name);
        return shop;
    }

    private String cached(Shop shop, LocalDateTime expiry) {
        RedisData data = new RedisData();
        data.setData(shop);
        data.setExpireTime(expiry);
        return JSONUtil.toJsonStr(data);
    }

    @Test
    void missingCacheLoadsDatabaseUsingProvidedPrefix() {
        Shop expected = shop("loaded");
        Shop actual = client.queryWithLogicalExpire("custom:", 1L, Shop.class,
                id -> expected, 30L, TimeUnit.MINUTES);
        assertSame(expected, actual);
        verify(values, times(2)).get("custom:1");
        verify(values).set(eq("custom:1"), contains("expireTime"));
        verify(redis).execute(any(RedisScript.class), anyList(), anyString());
    }

    @Test
    void cachedNullDoesNotQueryDatabase() {
        when(values.get("custom:1")).thenReturn("");
        assertNull(client.queryWithLogicalExpire("custom:", 1L, Shop.class,
                id -> { throw new AssertionError("should not query database"); },
                30L, TimeUnit.MINUTES));
        verify(values, never()).setIfAbsent(anyString(), anyString(), anyLong(), any(TimeUnit.class));
    }

    @Test
    void oldPlainJsonIsConvertedToLogicalCache() {
        when(values.get("custom:1")).thenReturn(JSONUtil.toJsonStr(shop("old")));
        Shop expected = shop("current");
        assertSame(expected, client.queryWithLogicalExpire("custom:", 1L, Shop.class,
                id -> expected, 30L, TimeUnit.MINUTES));
        verify(values).set(eq("custom:1"), contains("expireTime"));
    }

    @Test
    void expiredCacheReturnsOldDataAndUnlocksOnlyAfterRebuild() throws Exception {
        when(values.get("custom:1")).thenReturn(cached(shop("old"), LocalDateTime.now().minusSeconds(1)));
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        CountDownLatch unlocked = new CountDownLatch(1);
        doAnswer(invocation -> { unlocked.countDown(); return 1L; })
                .when(redis).execute(any(RedisScript.class), anyList(), anyString());
        try {
            Shop result = client.queryWithLogicalExpire("custom:", 1L, Shop.class, id -> {
                started.countDown();
                try {
                    if (!finish.await(3, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("test timed out");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
                return shop("new");
            }, 30L, TimeUnit.MINUTES);
            assertEquals("old", result.getName());
            assertTrue(started.await(3, TimeUnit.SECONDS));
            assertEquals(1L, unlocked.getCount(), "lock must remain held during database query");
        } finally {
            finish.countDown();
        }
        assertTrue(unlocked.await(3, TimeUnit.SECONDS));
        verify(values).set(eq("custom:1"), contains("new"));
    }

    @Test
    void databaseExceptionStillReleasesLock() {
        assertThrows(IllegalStateException.class, () ->
                client.queryWithLogicalExpire("custom:", 1L, Shop.class,
                        id -> { throw new IllegalStateException("database failed"); },
                        30L, TimeUnit.MINUTES));
        verify(redis).execute(any(RedisScript.class), anyList(), anyString());
    }
}