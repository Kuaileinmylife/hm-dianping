package com.hmdp.service.impl;

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
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Transactional
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    @Override
    public Result seckilVoucher(Long voucherId) {
        // 查询优惠卷信息
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);

        // 判断秒杀是否开始和结束
        if(voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            // 尚未开始
            return Result.fail("秒杀活动尚未开始");
        }

        if(voucher.getEndTime().isBefore(LocalDateTime.now())) {
            // 已经结束
            return Result.fail("秒杀活动已经结束");
        }

        // 判断库存是否充足
        if(voucher.getStock() < 1){
            return Result.fail("库存不足");
        }

        // 获取当前用户id
        Long userId = UserHolder.getUser().getId();

        //synchronized(userId.toString().intern()) {

        // 创建锁对象
        //SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        RLock lock = redissonClient.getLock("lock:order:" + userId);

        // 获取锁
        boolean isLock = lock.tryLock();
        // 判断是否获取锁成功
        if(!isLock){
            // 获取锁失败
            return Result.fail("不允许重复下单");
        }

        // 获取代理对象（事务）
        try {
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVouc(voucherId, userId);
        } finally {
            // 释放锁
            lock.unlock();
        }
    }

    @Transactional
    public Result createVouc(Long voucherId,  Long userId) {
            // 一人一单
            // 查询订单
            Long count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();

            // 判断是否存在
            if(count>0){
                // 用户已经购买过
                return Result.fail("用户已经购买过");
            }

            // 扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1")
                    .eq("voucher_id", voucherId)
                    .gt("stock", 0) // 加乐观锁(cas比较法，用库存代替版本号)
                    .update();

            if(!success){
                // 扣减失败
                return Result.fail("库存不足");
            }

            // 返回订单id
            VoucherOrder voucherOrder = new VoucherOrder();

            // 创建订单id
            long orderId = redisIdWorker.nextId("order");

            voucherOrder.setId(orderId);

            voucherOrder.setUserId(userId);

            voucherOrder.setVoucherId(voucherId);

            save(voucherOrder);

            return Result.ok(orderId);
    }
}
