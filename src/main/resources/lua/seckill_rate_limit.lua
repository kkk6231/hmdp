-- KEYS[1]: 限流 Key
-- ARGV[1]: 当前窗口允许的最大请求次数
-- ARGV[2]: 固定窗口大小，单位：秒
-- 返回值：1 表示允许，0 表示拒绝

local current = redis.call('INCR', KEYS[1])

-- 只在固定窗口的第一次请求设置过期时间，防止后续请求不断延长窗口。
if current == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[2])
end

if current > tonumber(ARGV[1]) then
    return 0
end

return 1
