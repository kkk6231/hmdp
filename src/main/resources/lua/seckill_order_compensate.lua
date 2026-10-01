local stockKey = KEYS[1]
local userOrderKey = KEYS[2]
local orderStateKey = KEYS[3]
local pendingKey = KEYS[4]

local orderId = ARGV[1]
local failedTtlSeconds = tonumber(ARGV[2])
local failureReason = ARGV[3]
local failedTimestamp = ARGV[4]

local currentStatus = redis.call('hget', orderStateKey, 'status')
if currentStatus == 'FAILED' then
    return 1
end
if currentStatus == 'SUCCESS' then
    return 2
end
if currentStatus ~= 'PROCESSING' then
    return 4
end

local existingOrderId = redis.call('get', userOrderKey)
if not existingOrderId or existingOrderId ~= orderId then
    return 3
end
if redis.call('exists', stockKey) == 0 then
    return 5
end

redis.call('incrby', stockKey, 1)
redis.call('del', userOrderKey)
redis.call('hset', orderStateKey,
        'status', 'FAILED',
        'failureReason', failureReason,
        'failedAt', failedTimestamp,
        'updatedAt', failedTimestamp)
redis.call('expire', orderStateKey, failedTtlSeconds)
redis.call('zrem', pendingKey, orderId)
return 0
