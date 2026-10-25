package com.hmdp.mq;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Slf4j
@Component
@RocketMQMessageListener(
        topic = "seckill-order-topic",
        consumerGroup = "hmdp-seckill-consumer"
) // 消费者
public class VoucherOrderConsumer implements RocketMQListener<VoucherOrder> {
    @Resource
    private IVoucherOrderService voucherOrderService;

    @Resource
    private RedissonClient redissonClient;

    @Override
    public void onMessage(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();

        // 沿用原来的用户锁
        RLock lock = redissonClient.getLock("lock:order:" + userId);

        boolean acquired = lock.tryLock();

        if (!acquired) {
            // 获取锁失败，本次不能确认消费成功
            throw new IllegalStateException(
                    "获取订单失败，orderId=" + voucherOrder.getId()
            );
        }

        try {
            // 通过Spring注入的代理对象，调用事务方法
            voucherOrderService.createVoucherOrder(voucherOrder);

            log.info("订单消息处理完成，orderId={}",  voucherOrder.getId());
        } finally {
            lock.unlock();
        }
    }
}
