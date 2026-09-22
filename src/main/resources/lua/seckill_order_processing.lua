local resultKey = KEYS[1]
local pendingKey = KEYS[2]

local orderId = ARGV[1]
local userId = ARGV[2]
local voucherId = ARGV[3]
local processingTimestamp = tonumber(ARGV[4])

local currentStatus = redis.call('hget', resultKey, 'status')
if currentStatus == 'SUCCESS' or currentStatus == 'FAILED' then
    return 0
end

redis.call('hset', resultKey,
        'status', 'PROCESSING',
        'userId', userId,
        'voucherId', voucherId)
redis.call('hsetnx', resultKey, 'createdAt', processingTimestamp)
redis.call('persist', resultKey)
redis.call('zadd', pendingKey, 'NX', processingTimestamp, orderId)
return 1
