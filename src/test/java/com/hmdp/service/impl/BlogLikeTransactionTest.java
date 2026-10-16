package com.hmdp.service.impl;

import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.hmdp.entity.Blog;
import com.hmdp.entity.BlogLike;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogLikeService;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.interceptor.TransactionInterceptor;
import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

// Exercises real Spring transaction advice with a mocked transaction manager.
// This checks commit/rollback decisions, not actual database rollback or DDL constraints.
class BlogLikeTransactionTest {
    BlogLikeServiceImpl target;
    IBlogLikeService proxy;
    BlogMapper blogs;
    PlatformTransactionManager transactions;
    TransactionStatus status;

    @BeforeEach void setUp() {
        target = spy(new BlogLikeServiceImpl());
        blogs = mock(BlogMapper.class);
        ReflectionTestUtils.setField(target, "blogMapper", blogs);
        transactions = mock(PlatformTransactionManager.class);
        status = mock(TransactionStatus.class);
        when(transactions.getTransaction(any(TransactionDefinition.class))).thenReturn(status);
        doReturn(true).when(target).save(any(BlogLike.class));
        doReturn(true).when(target).remove(any(Wrapper.class));
        when(blogs.update(isNull(), any(Wrapper.class))).thenReturn(1);
        ProxyFactory factory = new ProxyFactory(target);
        factory.setInterfaces(IBlogLikeService.class);
        factory.addAdvice(new TransactionInterceptor(transactions, new AnnotationTransactionAttributeSource()));
        proxy = (IBlogLikeService) factory.getProxy();
    }
    @Test void successfulLikeSavesRelationAndCommits() {
        proxy.likeBlog(1L, 7L);
        ArgumentCaptor<BlogLike> relation = ArgumentCaptor.forClass(BlogLike.class);
        verify(target).save(relation.capture());
        assertEquals(Long.valueOf(1), relation.getValue().getBlogId());
        assertEquals(Long.valueOf(7), relation.getValue().getUserId());
        verify(transactions).commit(status);
        verify(transactions, never()).rollback(any());
    }
    @Test void missingBlogRollsBackInsertedRelation() {
        when(blogs.update(isNull(), any(Wrapper.class))).thenReturn(0);
        assertThrows(IllegalStateException.class, () -> proxy.likeBlog(1L, 7L));
        verify(target).save(any(BlogLike.class));
        verify(transactions).rollback(status);
        verify(transactions, never()).commit(any());
    }
    @Test void failedRelationSaveDoesNotIncrementCount() {
        doReturn(false).when(target).save(any(BlogLike.class));
        assertThrows(IllegalStateException.class, () -> proxy.likeBlog(1L, 7L));
        verifyNoInteractions(blogs);
        verify(transactions).rollback(status);
    }
    @Test void duplicateRelationDoesNotIncrementCount() {
        doThrow(new DuplicateKeyException("duplicate relation")).when(target).save(any(BlogLike.class));
        assertThrows(DuplicateKeyException.class, () -> proxy.likeBlog(1L, 7L));
        verifyNoInteractions(blogs);
        verify(transactions).rollback(status);
    }
    @Test void successfulUnlikeCommitsGuardedDecrement() {
        proxy.unlikeBlog(1L, 7L);
        ArgumentCaptor<Wrapper> update = ArgumentCaptor.forClass(Wrapper.class);
        verify(blogs).update(isNull(), update.capture());
        assertTrue(update.getValue().getSqlSegment().contains("liked >"));
        verify(transactions).commit(status);
    }
    @Test void absentRelationDoesNotDecrementCount() {
        doReturn(false).when(target).remove(any(Wrapper.class));
        proxy.unlikeBlog(1L, 7L);
        verifyNoInteractions(blogs);
        verify(transactions).commit(status);
    }
    @Test void failedDecrementRollsBackRelationDeletion() {
        when(blogs.update(isNull(), any(Wrapper.class))).thenReturn(0);
        assertThrows(IllegalStateException.class, () -> proxy.unlikeBlog(1L, 7L));
        verify(target).remove(any(Wrapper.class));
        verify(transactions).rollback(status);
        verify(transactions, never()).commit(any());
    }
}
