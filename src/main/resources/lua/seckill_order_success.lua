local orderStateKey = KEYS[1]
local pendingKey = KEYS[2]

local orderId = ARGV[1]
local userId = ARGV[2]
local voucherId = ARGV[3]
local successTtlSeconds = tonumber(ARGV[4])
local successTimestamp = ARGV[5]

local currentStatus = redis.call('hget', orderStateKey, 'status')
if currentStatus == 'FAILED' then
    return 1
end

redis.call('hset', orderStateKey,
        'status', 'SUCCESS',
        'userId', userId,
        'voucherId', voucherId,
        'updatedAt', successTimestamp)
redis.call('hdel', orderStateKey, 'failureReason', 'failedAt')
redis.call('expire', orderStateKey, successTtlSeconds)
redis.call('zrem', pendingKey, orderId)
return 0
