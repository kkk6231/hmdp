-- KEYS[1]: 用户维度限流 Key
-- KEYS[2]: voucher 维度限流 Key
-- ARGV[1]: 用户窗口允许的最大请求次数
-- ARGV[2]: 用户固定窗口大小，单位：秒
-- ARGV[3]: voucher 窗口允许的最大请求次数
-- ARGV[4]: voucher 固定窗口大小，单位：秒
-- 返回值：0-通过，1-用户限流，2-voucher 全局限流

local userKey = KEYS[1]
local voucherKey = KEYS[2]

local userMaxCount = tonumber(ARGV[1])
local userWindowSeconds = tonumber(ARGV[2])
local voucherMaxCount = tonumber(ARGV[3])
local voucherWindowSeconds = tonumber(ARGV[4])

-- 先检查两个维度，任一维度超限时都不消耗另一个维度的配额。
local userCurrent = tonumber(redis.call('GET', userKey) or '0')
if userCurrent >= userMaxCount then
    return 1
end

local voucherCurrent = tonumber(redis.call('GET', voucherKey) or '0')
if voucherCurrent >= voucherMaxCount then
    return 2
end

-- Lua 脚本原子执行，两项都允许后再统一增加计数。
userCurrent = redis.call('INCR', userKey)
if userCurrent == 1 then
    redis.call('EXPIRE', userKey, userWindowSeconds)
end

voucherCurrent = redis.call('INCR', voucherKey)
if voucherCurrent == 1 then
    redis.call('EXPIRE', voucherKey, voucherWindowSeconds)
end

return 0
