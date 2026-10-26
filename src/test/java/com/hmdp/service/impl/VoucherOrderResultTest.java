package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.conditions.query.QueryChainWrapper;
import com.hmdp.dto.OrderResultDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.utils.UserHolder;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

// 隔离测试：不启动应用，不连接 Redis/MySQL，也不发送 MQ 消息。
class VoucherOrderResultTest {
    VoucherOrderServiceImpl service;
    QueryChainWrapper<VoucherOrder> query;
    StringRedisTemplate redis;
    ValueOperations<String, String> values;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        service = spy(new VoucherOrderServiceImpl());
        query = mock(QueryChainWrapper.class, RETURNS_SELF);
        redis = mock(StringRedisTemplate.class);
        values = mock(ValueOperations.class);
        doReturn(query).when(service).query();
        // one() 返回实体而非链式对象，默认模拟尚未落库。
        when(query.one()).thenReturn(null);
        when(redis.opsForValue()).thenReturn(values);
        ReflectionTestUtils.setField(service, "stringRedisTemplate", redis);
        UserDTO user = new UserDTO();
        user.setId(7L);
        UserHolder.saveUser(user);
    }

    @AfterEach
    void cleanup() { UserHolder.removeUser(); }

    @Test
    void committedOrderSucceedsWithoutRedis() {
        when(query.one()).thenReturn(new VoucherOrder());
        Result result = service.queryOrderResult(100L);
        assertTrue(result.getSuccess());
        assertEquals("SUCCESS", ((OrderResultDTO) result.getData()).getState());
        assertEquals("100", ((OrderResultDTO) result.getData()).getOrderId());
        verify(query).eq("id", 100L);
        verify(query).eq("user_id", 7L);
        verifyNoInteractions(redis);
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"8"})
    void missingOrForeignOwnerDoesNotExposeTransaction(String owner) {
        when(values.get("seckill:owner:100")).thenReturn(owner);
        Result result = service.queryOrderResult(100L);
        assertFalse(result.getSuccess());
        assertNull(result.getData());
        verify(query).eq("user_id", 7L);
        verify(values, never()).get("seckill:tx:100");
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"0", "1", "2", "3", "unexpected"})
    void distinguishesReservationFromDatabaseSuccess(String state) {
        when(values.get("seckill:owner:100")).thenReturn("7");
        when(values.get("seckill:tx:100")).thenReturn(state);
        String expected = "0".equals(state) ? "PROCESSING"
                : ("1".equals(state) || "2".equals(state) || "3".equals(state))
                ? "REJECTED" : "UNKNOWN";
        Result result = service.queryOrderResult(100L);
        assertTrue(result.getSuccess());
        assertEquals(expected, ((OrderResultDTO) result.getData()).getState());
    }

    @Test
    void anonymousCallerCannotQuery() {
        UserHolder.removeUser();
        assertFalse(service.queryOrderResult(100L).getSuccess());
        verifyNoInteractions(query, redis);
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(longs = {0L, -1L})
    void invalidIdDoesNotAccessStorage(Long orderId) {
        assertFalse(service.queryOrderResult(orderId).getSuccess());
        verifyNoInteractions(query, redis);
    }
}
