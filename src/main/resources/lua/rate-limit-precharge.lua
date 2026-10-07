-- 启动补记：新实例起来时，把当前窗口已经过去的那部分按比例先占住。
-- 与判定脚本同一套窗口口径：窗口号与已过时间都用 Redis 服务端时钟算，补记与判定落在同一个键上。
-- KEYS[1] 锚点键（形如 apigw:rl:{appNo}）  ARGV[1] windowMs  ARGV[2] ttlMs  ARGV[3] appLimit
local t = redis.call('TIME')
local nowMs = t[1] * 1000 + math.floor(t[2] / 1000)
local windowMs = tonumber(ARGV[1])
local ttlMs = tonumber(ARGV[2])
local appLimit = tonumber(ARGV[3])

local window = math.floor(nowMs / windowMs)
local winStart = window * windowMs
local elapsed = nowMs - winStart
if elapsed < 0 then elapsed = 0 end
if elapsed > windowMs then elapsed = windowMs end

local charge = math.floor(appLimit * elapsed / windowMs)
if charge <= 0 then
    return { 0 }
end

local tag = string.match(KEYS[1], "{.*}")
local appKey = "apigw:rl:" .. tag .. ":app:" .. window
redis.call('INCRBY', appKey, charge)
redis.call('PEXPIRE', appKey, ttlMs)
return { charge }
