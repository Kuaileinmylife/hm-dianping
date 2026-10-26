package com.hmdp.dto;

import lombok.Data;

/**
 * 异步下单的查询结果，与订单的支付状态（未支付、已支付等）分开表示。
 * 外层 Result.success 表示查询是否正常完成，state 表示下单进度。
 */
@Data
public class OrderResultDTO {
    // 用字符串返回订单号，避免前端处理大整数时丢失精度。
    private String orderId;

    // SUCCESS：订单已创建，可在 MySQL 中查到。
    // PROCESSING：Redis 预扣成功，尚未查询到数据库订单。
    // REJECTED：库存不足、重复抢购或本次事务已关闭，请求未受理。
    // UNKNOWN：暂时无法确认结果，不能直接判定为下单失败。
    private String state;

    // 展示给用户的处理结果说明。
    private String message;
}
