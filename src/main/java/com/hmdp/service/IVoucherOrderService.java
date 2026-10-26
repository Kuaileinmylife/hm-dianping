package com.hmdp.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IVoucherOrderService extends IService<VoucherOrder> {
    Result seckilVoucher(Long voucherId);

    // 查询当前登录用户的订单创建结果，尚未落库时结合 Redis 判断。
    Result queryOrderResult(Long orderId);

    void createVoucherOrder(VoucherOrder voucherOrder);
}