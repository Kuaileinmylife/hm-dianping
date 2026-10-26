-- 接收参数
local voucherId = ARGV[1]
local userId = ARGV[2]
local orderId = ARGV[3]

-- redis key
local stockKey = 'seckill:stock:' .. voucherId
local orderKey = 'seckill:order:' .. voucherId
local txKey = 'seckill:tx:' .. orderId -- 存放的是订单的执行结果

-- 同一订单已经有执行结果，直接返回
-- 防止重复执行脚本时再次扣减库存
local previousResult = redis.call('get', txKey)
if previousResult then
    return tonumber(previousResult)
end

-- 检查库存
local stock = tonumber(redis.call('get', stockKey))
if not stock or stock <= 0 then
    redis.call('set', txKey, '1')
    return 1
end

-- 检查用户是否重复购买
if redis.call('sismember', orderKey, userId) == 1 then
    redis.call('set', txKey, '2')
    return 2
end

-- 预扣库存，记录抢购用户
redis.call('incrby', stockKey, -1)
redis.call('sadd', orderKey, userId)

-- 记录本次订单的预扣结果
redis.call('set', txKey, '0')

return 0