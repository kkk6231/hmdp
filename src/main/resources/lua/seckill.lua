-- 1. 数据 Key，由调用方构造
local stockKey = KEYS[1]
local userOrderKey = KEYS[2]
local transactionKey = KEYS[3]
local orderResultKey = KEYS[4]
local pendingKey = KEYS[5]
local activityMetaKey = KEYS[6]

-- 2. 参数列表
local userId = ARGV[1]
local orderId = ARGV[2]
local voucherId = ARGV[3]
local transactionTtlSeconds = tonumber(ARGV[4])
local processingTimestamp = tonumber(ARGV[5])

-- 订单已经 SUCCESS/FAILED 时不能被重复执行的 Lua 降级回 PROCESSING
local function ensureProcessingResult()
    local currentStatus = redis.call('hget', orderResultKey, 'status')
    if currentStatus == 'SUCCESS' or currentStatus == 'FAILED' then
        return
    end

    redis.call('hset', orderResultKey,
            'status', 'PROCESSING',
            'userId', userId,
            'voucherId', voucherId)
    redis.call('hsetnx', orderResultKey, 'createdAt', processingTimestamp)
    -- PROCESSING 必须保留到进入终态，顺便清除旧版本可能设置的 TTL
    redis.call('persist', orderResultKey)
    -- NX 保留第一次进入 PROCESSING 的时间，不因事务回查而延后超时判断
    redis.call('zadd', pendingKey, 'NX', processingTimestamp, orderId)
end

-- 3. 已经得到过明确事务结果时，重复执行直接返回原结果
local transactionState = redis.call('get', transactionKey)
if transactionState == 'COMMIT' then
    ensureProcessingResult()
    return 0
end
if transactionState == 'ROLLBACK:OUT_OF_STOCK' then
    return 1
end
if transactionState == 'ROLLBACK:DUPLICATE' then
    return 2
end
if transactionState == 'ROLLBACK:NOT_STARTED' then
    return 3
end
if transactionState == 'ROLLBACK:ENDED' then
    return 4
end
if transactionState == 'ROLLBACK:NOT_READY' then
    return 5
end

-- 4. 使用 Redis 服务器时间校验活动时间。
-- 活动元数据与库存一起预热，但保留到活动结束后的恢复窗口，
-- 因此不能通过库存 Key 是否存在来判断活动是否仍在进行。
local beginTimeValue = redis.call('hget', activityMetaKey, 'beginTime')
local endTimeValue = redis.call('hget', activityMetaKey, 'endTime')
if not beginTimeValue or not endTimeValue then
    redis.call('set', transactionKey, 'ROLLBACK:NOT_READY', 'EX', transactionTtlSeconds)
    return 5
end

local beginTime = tonumber(beginTimeValue)
local endTime = tonumber(endTimeValue)
if not beginTime or not endTime or beginTime >= endTime then
    redis.call('set', transactionKey, 'ROLLBACK:NOT_READY', 'EX', transactionTtlSeconds)
    return 5
end

local redisTime = redis.call('TIME')
local nowMillis = tonumber(redisTime[1]) * 1000
        + math.floor(tonumber(redisTime[2]) / 1000)
if nowMillis < beginTime then
    redis.call('set', transactionKey, 'ROLLBACK:NOT_STARTED', 'EX', transactionTtlSeconds)
    return 3
end
if nowMillis >= endTime then
    redis.call('set', transactionKey, 'ROLLBACK:ENDED', 'EX', transactionTtlSeconds)
    return 4
end

-- 5. 判断用户是否已经获得该券资格
local existingOrderId = redis.call('hget', userOrderKey, userId)
if existingOrderId then
    if existingOrderId == orderId then
        -- 同一个 orderId 再次执行，说明此前预扣已经成功，恢复 COMMIT 状态
        redis.call('set', transactionKey, 'COMMIT', 'EX', transactionTtlSeconds)
        ensureProcessingResult()
        return 0
    end

    redis.call('set', transactionKey, 'ROLLBACK:DUPLICATE', 'EX', transactionTtlSeconds)
    return 2
end

-- 6. 判断库存。库存 Key 不存在说明预热数据不完整或已经过期
local stockValue = redis.call('get', stockKey)
if not stockValue then
    redis.call('set', transactionKey, 'ROLLBACK:NOT_READY', 'EX', transactionTtlSeconds)
    return 5
end

local stock = tonumber(stockValue)
if not stock or stock <= 0 then
    redis.call('set', transactionKey, 'ROLLBACK:OUT_OF_STOCK', 'EX', transactionTtlSeconds)
    return 1
end

-- 7. 原子预扣库存、保存用户订单映射并记录 MQ 本地事务结果
redis.call('incrby', stockKey, -1)
redis.call('hset', userOrderKey, userId, orderId)
local stockTtl = redis.call('ttl', stockKey)
if stockTtl > 0 then
    redis.call('expire', userOrderKey, stockTtl)
end
redis.call('set', transactionKey, 'COMMIT', 'EX', transactionTtlSeconds)
ensureProcessingResult()
return 0
