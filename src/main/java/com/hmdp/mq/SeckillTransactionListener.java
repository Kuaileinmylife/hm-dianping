package com.hmdp.mq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.entity.VoucherOrder;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.messaging.Message;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.Collections;

@Slf4j
@RocketMQTransactionListener
public class SeckillTransactionListener implements RocketMQLocalTransactionListener {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private ObjectMapper objectMapper;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    private static final DefaultRedisScript<Long> CHECK_SCRIPT;
    // 静态加载lua脚本
    static{
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(
                new ClassPathResource("seckill-tx.lua")
        );
        SECKILL_SCRIPT.setResultType(Long.class);

        CHECK_SCRIPT = new DefaultRedisScript<>();
        CHECK_SCRIPT.setLocation(
                new ClassPathResource("check-seckill-tx.lua")
        );
        CHECK_SCRIPT.setResultType(Long.class);
    }

    // 半消息发送成功后，执行本地预扣
    @Override
    public RocketMQLocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        try {
            VoucherOrder order = readOrder(msg);

            Long result = stringRedisTemplate.execute(
                    SECKILL_SCRIPT,
                    Collections.emptyList(),
                    order.getVoucherId().toString(),
                    order.getUserId().toString(),
                    order.getId().toString()
            );

            return toState(result);
        } catch (Exception e) {
            // redis超时可能发送在脚本执行之后，不能直接认定失败
            log.error("执行秒杀本地事务异常，等待回查", e);
            return RocketMQLocalTransactionState.UNKNOWN;
        }
    }

    // broker回查本次订单的预扣结果
    @Override
    public RocketMQLocalTransactionState checkLocalTransaction(Message msg) {
        try {
            VoucherOrder order = readOrder(msg);
            String key = "seckill:tx:" + order.getId();

            Long result = stringRedisTemplate.execute(
                    CHECK_SCRIPT,
                    Collections.singletonList(key)
            );

            return toState(result);
        } catch (Exception e) {
            log.error("秒杀事务回查异常，暂时无法确定结果", e);
            return RocketMQLocalTransactionState.UNKNOWN;
        }
    }

    private VoucherOrder readOrder(Message msg) throws IOException {
        // 当前的事务回调中，消息体是byte[]
        return objectMapper.readValue(
                (byte[])msg.getPayload(),
                VoucherOrder.class
        );
    }

    private RocketMQLocalTransactionState toState(Long result){
        if(result == null){
            return RocketMQLocalTransactionState.UNKNOWN;
        }

        if(result == 0L){
            return RocketMQLocalTransactionState.COMMIT;
        }

        if(result == 1L || result == 2L || result == 3L){
            return RocketMQLocalTransactionState.ROLLBACK;
        }

        return RocketMQLocalTransactionState.UNKNOWN;
    }
}
