package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.conditions.query.QueryChainWrapper;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.BlogLike;
import com.hmdp.service.IBlogLikeService;
import com.hmdp.utils.UserHolder;
import org.junit.jupiter.api.*;
import org.mockito.InOrder;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

// Isolated regression tests: no running MySQL or Redis required.
class BlogLikeTest {
    BlogServiceImpl service;
    IBlogLikeService likes;
    QueryChainWrapper<BlogLike> query;
    StringRedisTemplate redis;
    ValueOperations<String, String> values;
    RedissonClient redisson;
    RLock lock;
    static final String KEY = "blog:like:status:1:7";

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        service = new BlogServiceImpl();
        likes = mock(IBlogLikeService.class);
        query = mock(QueryChainWrapper.class, RETURNS_SELF);
        redis = mock(StringRedisTemplate.class);
        values = mock(ValueOperations.class);
        redisson = mock(RedissonClient.class);
        lock = mock(RLock.class);
        when(likes.query()).thenReturn(query);
        when(query.count()).thenReturn(0L);
        when(redis.opsForValue()).thenReturn(values);
        when(redisson.getLock("lock:blog:like:1:7")).thenReturn(lock);
        when(lock.tryLock()).thenReturn(true);
        when(lock.isHeldByCurrentThread()).thenReturn(true);
        ReflectionTestUtils.setField(service, "blogLikeService", likes);
        ReflectionTestUtils.setField(service, "stringRedisTemplate", redis);
        ReflectionTestUtils.setField(service, "redissonClient", redisson);
        login();
    }

    void login() {
        UserDTO user = new UserDTO(); user.setId(7L); UserHolder.saveUser(user);
    }
    @AfterEach void cleanup() { UserHolder.removeUser(); }

    @Test void likeCommitsBeforeInvalidatingCacheAndUnlocking() {
        assertTrue(service.likeBlog(1L).getSuccess());
        InOrder order = inOrder(likes, redis, lock);
        order.verify(likes).likeBlog(1L, 7L);
        order.verify(redis).delete(KEY);
        order.verify(lock).unlock();
        verify(likes, never()).unlikeBlog(anyLong(), anyLong());
        verify(query).eq("blog_id", 1L);
        verify(query).eq("user_id", 7L);
    }
    @Test void existingRelationCancelsLike() {
        when(query.count()).thenReturn(1L);
        assertTrue(service.likeBlog(1L).getSuccess());
        verify(likes).unlikeBlog(1L, 7L);
        verify(likes, never()).likeBlog(anyLong(), anyLong());
        verify(redis).delete(KEY);
    }
    @Test void failedTransactionDoesNotInvalidateCacheAndReleasesLock() {
        doThrow(new IllegalStateException("database failure")).when(likes).likeBlog(1L, 7L);
        assertThrows(IllegalStateException.class, () -> service.likeBlog(1L));
        verify(redis, never()).delete(anyString());
        verify(lock).unlock();
    }
    @Test void cacheInvalidationFailureKeepsSuccessfulDatabaseResult() {
        when(redis.delete(KEY)).thenThrow(new DataAccessResourceFailureException("Redis unavailable"));
        assertTrue(service.likeBlog(1L).getSuccess());
        verify(likes).likeBlog(1L, 7L);
        verify(lock).unlock();
    }
    @Test void anonymousRequestDoesNotAccessStorage() {
        UserHolder.removeUser();
        assertFalse(service.likeBlog(1L).getSuccess());
        verifyNoInteractions(redisson, redis);
        verify(likes, never()).query();
    }
    @Test void invalidIdsDoNotAccessStorage() {
        for (Long id : new Long[]{null, 0L, -1L}) assertFalse(service.likeBlog(id).getSuccess());
        verifyNoInteractions(redisson, redis);
        verify(likes, never()).query();
    }
    @Test void overlappingRequestCannotToggleAgain() throws Exception {
        ReentrantLock local = new ReentrantLock();
        when(lock.tryLock()).thenAnswer(i -> local.tryLock());
        when(lock.isHeldByCurrentThread()).thenAnswer(i -> local.isHeldByCurrentThread());
        doAnswer(i -> { local.unlock(); return null; }).when(lock).unlock();
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        doAnswer(i -> {
            entered.countDown();
            if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("timeout");
            return null;
        }).when(likes).likeBlog(1L, 7L);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> first = worker.submit(() -> {
                login();
                try { return service.likeBlog(1L).getSuccess(); }
                finally { UserHolder.removeUser(); }
            });
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertFalse(service.likeBlog(1L).getSuccess());
            release.countDown();
            assertTrue(first.get(5, TimeUnit.SECONDS));
            verify(query, times(1)).count();
            verify(likes, times(1)).likeBlog(1L, 7L);
            verify(redis, times(1)).delete(KEY);
        } finally { release.countDown(); worker.shutdownNow(); }
    }
    Blog readStatus() {
        Blog blog = new Blog(); blog.setId(1L);
        ReflectionTestUtils.invokeMethod(service, "isBlogLiked", blog);
        return blog;
    }
    @Test void cachedTrueAndFalseAvoidDatabase() {
        when(values.get(KEY)).thenReturn("1", "0");
        assertTrue(readStatus().getIsLike());
        assertFalse(readStatus().getIsLike());
        verify(likes, never()).query();
    }
    @Test void cacheMissCachesPositiveDatabaseResultWithExpiry() {
        when(query.count()).thenReturn(1L);
        assertTrue(readStatus().getIsLike());
        verify(values).set(KEY, "1", 60, TimeUnit.SECONDS);
    }
    @Test void cacheMissCachesNegativeResultWithExpiry() {
        assertFalse(readStatus().getIsLike());
        verify(values).set(KEY, "0", 60, TimeUnit.SECONDS);
    }
    @Test void invalidCacheValueFallsBackToDatabase() {
        when(values.get(KEY)).thenReturn("bad-data");
        assertFalse(readStatus().getIsLike());
        verify(query).count();
        verify(values).set(KEY, "0", 60, TimeUnit.SECONDS);
    }
    @Test void redisReadAndWriteFailuresStillReturnDatabaseStatus() {
        when(values.get(KEY)).thenThrow(new DataAccessResourceFailureException("read failure"));
        doThrow(new DataAccessResourceFailureException("write failure"))
                .when(values).set(KEY, "1", 60, TimeUnit.SECONDS);
        when(query.count()).thenReturn(1L);
        assertTrue(readStatus().getIsLike());
        verify(query).count();
    }
    @Test void anonymousStatusIsFalseWithoutStorageAccess() {
        UserHolder.removeUser();
        assertFalse(readStatus().getIsLike());
        verifyNoInteractions(redis);
        verify(likes, never()).query();
    }
    @Test void emptyLikeListDoesNotReadRedis() {
        when(query.list()).thenReturn(java.util.Collections.emptyList());
        assertEquals(java.util.Collections.emptyList(), service.queryBlogLikes(1L).getData());
        verify(query).orderByAsc("create_time");
        verify(query).orderByAsc("id");
        verify(query).last("LIMIT 5");
        verifyNoInteractions(redis);
    }
    @Test @SuppressWarnings("unchecked")
    void likeListPreservesRelationOrderAndReturnsPublicUserDtos() {
        com.hmdp.service.IUserService users = mock(com.hmdp.service.IUserService.class);
        QueryChainWrapper<com.hmdp.entity.User> userQuery = mock(QueryChainWrapper.class, RETURNS_SELF);
        ReflectionTestUtils.setField(service, "userService", users);
        when(users.query()).thenReturn(userQuery);
        BlogLike first = new BlogLike(); first.setUserId(9L);
        BlogLike second = new BlogLike(); second.setUserId(3L);
        when(query.list()).thenReturn(java.util.Arrays.asList(first, second));
        com.hmdp.entity.User a = new com.hmdp.entity.User(); a.setId(9L); a.setNickName("first");
        com.hmdp.entity.User b = new com.hmdp.entity.User(); b.setId(3L); b.setNickName("second");
        when(userQuery.list()).thenReturn(java.util.Arrays.asList(a, b));
        java.util.List<UserDTO> result = (java.util.List<UserDTO>) service.queryBlogLikes(1L).getData();
        assertEquals(Long.valueOf(9), result.get(0).getId());
        assertEquals("first", result.get(0).getNickName());
        assertEquals(Long.valueOf(3), result.get(1).getId());
        verify(userQuery).in("id", java.util.Arrays.asList(9L, 3L));
        verify(userQuery).last("ORDER BY FIELD(id,9,3)");
        verifyNoInteractions(redis);
    }
}
