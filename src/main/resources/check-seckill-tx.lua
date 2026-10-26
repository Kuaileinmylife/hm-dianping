local txKey = KEYS[1]

-- 读取状态
local result = redis.call('get', txKey)

if result then
    return tonumber(result)
end

-- 没有执行记录，关闭本次事务，禁止之后迟到的回调继续预扣
redis.call('set', txKey, '3')
return 3