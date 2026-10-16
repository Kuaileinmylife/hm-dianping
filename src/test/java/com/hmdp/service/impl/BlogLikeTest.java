package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.utils.UserHolder;
import org.junit.jupiter.api.*;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class BlogLikeTest {
    BlogServiceImpl service;
    RLock lock;
    ZSetOperations<String, String> zset;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        service = spy(new BlogServiceImpl());
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        RedissonClient redisson = mock(RedissonClient.class);
        lock = mock(RLock.class);
        zset = mock(ZSetOperations.class);
        when(redis.opsForZSet()).thenReturn(zset);
        when(zset.score(anyString(), anyString())).thenReturn(null);
        when(redisson.getLock("lock:blog:like:1:7")).thenReturn(lock);
        when(lock.tryLock()).thenReturn(true);
        when(lock.isHeldByCurrentThread()).thenReturn(true);
        doReturn(true).when(service).updateLikeCount(anyLong(), anyBoolean());
        ReflectionTestUtils.setField(service, "stringRedisTemplate", redis);
        ReflectionTestUtils.setField(service, "redissonClient", redisson);
        login();
    }

    void login() {
        UserDTO user = new UserDTO();
        user.setId(7L);
        UserHolder.saveUser(user);
    }

    @AfterEach
    void cleanup() { UserHolder.removeUser(); }

    @Test
    void overlappingRequestCannotReadOrIncrementAgain() throws Exception {
        ReentrantLock localLock = new ReentrantLock();
        when(lock.tryLock()).thenAnswer(i -> localLock.tryLock());
        when(lock.isHeldByCurrentThread()).thenAnswer(i -> localLock.isHeldByCurrentThread());
        doAnswer(i -> { localLock.unlock(); return null; }).when(lock).unlock();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        doAnswer(i -> {
            entered.countDown();
            if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("timeout");
            return true;
        }).when(service).updateLikeCount(1L, true);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            Future<Result> first = worker.submit(() -> {
                login();
                try { return service.likeBlog(1L); }
                finally { UserHolder.removeUser(); }
            });
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertFalse(service.likeBlog(1L).getSuccess());
            release.countDown();
            assertTrue(first.get(5, TimeUnit.SECONDS).getSuccess());
            verify(zset, times(1)).score("blog:liked:1", "7");
            verify(service, times(1)).updateLikeCount(1L, true);
            verify(zset, times(1)).add(eq("blog:liked:1"), eq("7"), anyDouble());
        } finally {
            release.countDown();
            worker.shutdownNow();
        }
    }

    @Test
    void failedDatabaseUpdateDoesNotModifyRedis() {
        doReturn(false).when(service).updateLikeCount(1L, true);
        assertFalse(service.likeBlog(1L).getSuccess());
        verify(zset, never()).add(anyString(), anyString(), anyDouble());
        verify(lock).unlock();
    }

    @Test
    void existingLikeIsRemoved() {
        when(zset.score("blog:liked:1", "7")).thenReturn(123D);
        assertTrue(service.likeBlog(1L).getSuccess());
        verify(service).updateLikeCount(1L, false);
        verify(zset).remove("blog:liked:1", "7");
        verify(lock).unlock();
    }

    @Test
    void redisFailureStillReleasesLock() {
        when(zset.add(anyString(), anyString(), anyDouble())).thenThrow(new IllegalStateException("Redis failure"));
        assertThrows(IllegalStateException.class, () -> service.likeBlog(1L));
        verify(lock).unlock();
    }

    @Test
    void anonymousRequestDoesNotAcquireLock() {
        UserHolder.removeUser();
        assertFalse(service.likeBlog(1L).getSuccess());
        verifyNoInteractions(lock, zset);
    }
}
