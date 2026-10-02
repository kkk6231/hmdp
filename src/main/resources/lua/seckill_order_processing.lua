local orderStateKey = KEYS[1]

local userId = ARGV[1]
local voucherId = ARGV[2]
local processingTimestamp = tonumber(ARGV[3])

local currentStatus = redis.call('hget', orderStateKey, 'status')
if currentStatus == 'SUCCESS' or currentStatus == 'FAILED' then
    return 0
end

redis.call('hset', orderStateKey,
        'status', 'PROCESSING',
        'userId', userId,
        'voucherId', voucherId)
redis.call('hsetnx', orderStateKey, 'createdAt', processingTimestamp)
redis.call('persist', orderStateKey)
return 1
