local resultKey = KEYS[1]
local pendingKey = KEYS[2]

local orderId = ARGV[1]
local userId = ARGV[2]
local voucherId = ARGV[3]
local successTtlSeconds = tonumber(ARGV[4])
local successTimestamp = ARGV[5]

local currentStatus = redis.call('hget', resultKey, 'status')
if currentStatus == 'FAILED' then
    return 1
end

redis.call('hset', resultKey,
        'status', 'SUCCESS',
        'userId', userId,
        'voucherId', voucherId,
        'updatedAt', successTimestamp)
redis.call('hdel', resultKey, 'failureReason', 'failedAt')
redis.call('expire', resultKey, successTtlSeconds)
redis.call('zrem', pendingKey, orderId)
return 0
