package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import org.springframework.core.io.ClassPathResource;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import lombok.val;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

//    @Override
//    public Result seckilVoucher(Long voucherId) {
//        // 查询优惠卷信息
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//
//        // 判断秒杀是否开始和结束
//        if(voucher.getBeginTime().isAfter(LocalDateTime.now())) {
//            // 尚未开始
//            return Result.fail("秒杀活动尚未开始");
//        }
//
//        if(voucher.getEndTime().isBefore(LocalDateTime.now())) {
//            // 已经结束
//            return Result.fail("秒杀活动已经结束");
//        }
//
//        // 判断库存是否充足
//        if(voucher.getStock() < 1){
//            return Result.fail("库存不足");
//        }
//
//        // 获取当前用户id
//        Long userId = UserHolder.getUser().getId();
//
//        //synchronized(userId.toString().intern()) {
//
//        // 创建锁对象
//        //SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//
//        // 获取锁
//        boolean isLock = lock.tryLock();
//        // 判断是否获取锁成功
//        if(!isLock){
//            // 获取锁失败
//            return Result.fail("不允许重复下单");
//        }
//
//        // 获取代理对象（事务）
//        try {
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVouc(voucherId, userId);
//        } finally {
//            // 释放锁
//            lock.unlock();
//        }
//    }

    // 加载脚本
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static{
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    // 阻塞队列
//    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024*1024);

    // 线程池
    private static final ExecutorService SECKILL_ORDIR_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init(){
        SECKILL_ORDIR_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable {
       String queueName = "stream.orders";
        @Override
        public void run() {
            while(true){
                try {
                    // 获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS streams.order >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed()) // lastConsumed()对应'>'
                    );

                    // 判断消息是否获取成功
                    if(list == null || list.isEmpty()){
                        // 如果获取失败，没有消息，继续下一次循环
                        continue;
                    }

                    // 解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);

                    // 获取成功，可以下单
                    handleVoucherOrder(voucherOrder);

                    // 进行ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常",e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while(true){
                try {
                    // 获取pending-list中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 2000 STREAMS streams.order >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );

                    // 判断消息是否获取成功
                    if(list == null || list.isEmpty()){
                        // 如果获取失败，没有消息，结束循环
                        break;
                    }

                    // 解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);

                    // 获取成功，可以下单
                    handleVoucherOrder(voucherOrder);

                    // 进行ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理pending-list订单异常",e);
                }
            }
        }
    }

//    private class VoucherOrderHandler implements Runnable {
//        @Override
//        public void run() {
//            while(true){
//                try {
//                    // 获取队列中的订单信息
//                    VoucherOrder voucherOrder = orderTasks.take();
//                    // 创建订单
//                    handleVoucherOrder(voucherOrder);
//                } catch (Exception e) {
//                    log.error("处理订单异常",e);
//                }
//
//            }
//        }
//    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        // 获取用户id
        Long userId = voucherOrder.getUserId();

        // 创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);

        // 获取锁
        boolean isLock = lock.tryLock();
        // 判断是否获取锁成功
        if(!isLock){
            // 获取锁失败
            log.error("不允许重复下单");
            return;
        }

        // 获取代理对象（事务）
        try {
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            // 释放锁
            lock.unlock();
        }
    }

    private IVoucherOrderService proxy;

    @Override
    public Result seckilVoucher(Long voucherId) {
        // 获取用户
        Long userId = UserHolder.getUser().getId();

        // 获取订单id
        long orderId = redisIdWorker.nextId("order");

        // 1,执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );

        int r = result.intValue();

        // 判断结果是否为0
        if(r != 0){
            // 不为0就有问题, 没有购买资格
            return Result.fail(r == 1 ? "库存不足":"不能重复下单");
        }


        // 获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        // 返回订单id
        return Result.ok(orderId);
    }

//    @Override
//    public Result seckilVoucher(Long voucherId) {
//        // 获取用户
//        Long userId = UserHolder.getUser().getId();
//
//        // 1,执行lua脚本
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(), userId.toString()
//        );
//
//        int r = result.intValue();
//
//        // 判断结果是否为0
//        if(r != 0){
//            // 不为0就有问题, 没有购买资格
//            return Result.fail(r == 1 ? "库存不足":"不能重复下单");
//        }
//
//        long orderId = redisIdWorker.nextId("order");
//
//        // 为0就有购买资格，把下单信息保存到阻塞队列
//        // 创建订单对象
//        VoucherOrder voucherOrder = new VoucherOrder();
//        voucherOrder.setId(orderId);
//        voucherOrder.setUserId(userId);
//        voucherOrder.setVoucherId(voucherId);
//
//        // 获取代理对象
//        proxy = (IVoucherOrderService) AopContext.currentProxy();
//
//        // 放入阻塞队列
//        orderTasks.add(voucherOrder);
//
//        // 返回订单id
//        return Result.ok(orderId);
//    }

    @Override
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
            // 一人一单
            Long userId = voucherOrder.getUserId();
            Long voucherId = voucherOrder.getVoucherId();

            // 查询订单
            Long count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();

            // 判断是否存在
            if(count>0){
                // 用户已经购买过
                log.error("用户已经购买过一次");
                return;
            }

            // 扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1")
                    .eq("voucher_id", voucherId)
                    .gt("stock", 0) // 加乐观锁(cas比较法，用库存代替版本号)
                    .update();

            if(!success){
                // 扣减失败
                log.error("库存不足");
                return;
            }

            // 返回订单id
            save(voucherOrder);
    }
}
