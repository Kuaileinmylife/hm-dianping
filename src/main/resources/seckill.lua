--1.1优惠卷id
local voucherId = ARGV[1]
--1.2用户id
local userId = ARGV[2]
--1.3订单id
local orderId = ARGV[3]

-- 2.数据key
-- 2.1库存key
local stockKey = 'seckill:stock:' .. voucherId
-- 2.2订单key
local orderKey = 'seckill:order:' .. voucherId

-- 3.脚本业务
-- 3.1判断库存是否充足 get stockKey
local stock = tonumber(redis.call('get', stockKey))
if not stock or stock <= 0 then
    -- 内存不足返回1
    return 1
end

-- 3.2判断用户是否下单 SISMEMBER orderKey userId
if(redis.call('sismember', orderKey, userId) == 1) then
    -- 3.3存在，说明是重复下单，返回2
    return 2
end

-- 3.4.扣库存 incrby stockKey -1
redis.call('incrby', stockKey, -1)
-- 3.5.下单（保存用户）add orderKey userId
redis.call('sadd', orderKey, userId)

-- 发送消息到队列中 XADD stream.orders * k1 v1 k2 v2...
redis.call('xadd', 'stream.orders', '*', 'userId', userId, 'voucherId', voucherId, 'id', orderId)

-- 成功返回0
return 0