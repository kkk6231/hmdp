local orderStateKey = KEYS[1]

local userId = ARGV[1]
local voucherId = ARGV[2]
local successTtlSeconds = tonumber(ARGV[3])
local successTimestamp = ARGV[4]

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
return 0
