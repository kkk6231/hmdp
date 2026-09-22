local stockKey = KEYS[1]
local userOrderKey = KEYS[2]
local resultKey = KEYS[3]
local pendingKey = KEYS[4]

local userId = ARGV[1]
local orderId = ARGV[2]
local failedTtlSeconds = tonumber(ARGV[3])
local failureReason = ARGV[4]
local failedTimestamp = ARGV[5]

local currentStatus = redis.call('hget', resultKey, 'status')
if currentStatus == 'FAILED' then
    return 1
end
if currentStatus == 'SUCCESS' then
    return 2
end
if currentStatus ~= 'PROCESSING' then
    return 4
end

local existingOrderId = redis.call('hget', userOrderKey, userId)
if not existingOrderId or existingOrderId ~= orderId then
    return 3
end
if redis.call('exists', stockKey) == 0 then
    return 5
end

redis.call('incrby', stockKey, 1)
redis.call('hdel', userOrderKey, userId)
redis.call('hset', resultKey,
        'status', 'FAILED',
        'failureReason', failureReason,
        'failedAt', failedTimestamp,
        'updatedAt', failedTimestamp)
redis.call('expire', resultKey, failedTtlSeconds)
redis.call('zrem', pendingKey, orderId)
return 0
