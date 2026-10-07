-- ============================================================================
-- 限流固定窗口计数（应用总量 + 应用下来源地址，两层一次判完）
--
-- 为什么必须是「一个 Lua 脚本」而不是 GET/MULTI 几条命令来回跑：
--   Redis 单线程执行脚本，整个脚本对其他命令是原子的——多台网关在同一窗口同时
--   加数时，每笔的「看当前计数 → 判额度 → 自占一次」在服务端串行完成。
--   谁先执行谁占名额，后执行者看到的是已被前面所有实例累加过的值：
--     · 不会超发：不可能两台都读到「还没到额度」然后一起放行，计数全局唯一；
--     · 不会少放：检查通过的每笔都恰好 +1，第 N 笔放行、第 N+1 笔拒绝，确定无疑。
--
-- 窗口划分（固定窗口，不带上一窗余数）：
--   窗口号 = floor(Redis TIME / 窗口毫秒长)，计数 key 在本脚本内按窗口号拼出。
--   窗口号以「Redis 服务端时钟」为唯一口径（不信任任何一台网关机器的本地钟，
--   多机时钟有偏差时也不会各算各的 key）：
--   到点后窗口号 +1，新窗口的 key 第一次被访问时才创建、从 0 开始数；
--   上一个窗口的 key 原样留到 TTL 到期被 Redis 回收，它的值不可能混进新窗口——
--   新窗口读的是另一个 key，从 0 起，没有任何「余数结转」。
--   窗口键只由本脚本中「判定通过」的 INCR 创建并累加：不存在任何启动补记/预占——
--   新实例接入共享存储时本窗额度只反映真实用量，启动动作本身不消耗额度；
--   两层（应用总量/来源地址）的窗口键挂在同一个窗口号下，各自随本层第一笔真实
--   放行从 0 建起，窗口刚建立时两层口径天然一致。
--
-- 键（由锚点 KEYS[1] 在脚本内派生，两层天然同槽，集群下一个脚本一次操作）：
--   KEYS[1] 锚点键：  apigw:rl:{<appNo>}                  （不写值，仅提供 hash tag 定槽）
--   派生应用总量键：  apigw:rl:{<appNo>}:app:<window>
--   派生来源地址键：  apigw:rl:{<appNo>}:ip:<ipHash>:<window>
--
-- 入参：
--   ARGV[1] windowMs   窗口长度（毫秒，固定 60000）
--   ARGV[2] ttlMs      计数键存活时长（窗口长 + 宽限），过期由 Redis 自动删除，不膨胀
--   ARGV[3] appLimit   应用每分钟额度；-1 表示这一层不限
--   ARGV[4] ipLimit    来源每分钟额度；-1 表示这一层不限
--   ARGV[5] ipHash     来源地址的 SHA-256 十六进制（IPv6 含冒号不直接进 key；
--                      同一来源在网关侧已由 ClientIpResolver 归一成唯一字面量再哈希）
--
-- 返回 { allowed, scope, retryAfterSec }
--   allowed=1 放行；allowed=0 拒绝，scope=0 卡在应用总量 / scope=1 卡在来源地址，
--   retryAfterSec= 到「当前这个窗口结束」还要等多少秒（向上取整，最少 1 秒），
--   即「下一个窗口重新开始计数的最早时刻」距今的整秒数——不是空泛数字，
--   调用方等到该时刻后本窗口配额必然已重置。
-- ============================================================================
local t = redis.call('TIME')
local nowMs = t[1] * 1000 + math.floor(t[2] / 1000)
local windowMs = tonumber(ARGV[1])
local ttlMs = tonumber(ARGV[2])
local appLimit = tonumber(ARGV[3])
local ipLimit = tonumber(ARGV[4])
local ipHash = ARGV[5]

local window = math.floor(nowMs / windowMs)
local winStart = window * windowMs
local retryAfter = math.ceil((winStart + windowMs - nowMs) / 1000)
if retryAfter < 1 then
    retryAfter = 1
end

-- hash tag 从锚点键原样取（锚点形如 apigw:rl:{appNo}），派生出的键与它同槽
local tag = string.match(KEYS[1], "{.*}")
local appKey = "apigw:rl:" .. tag .. ":app:" .. window
local ipKey = "apigw:rl:" .. tag .. ":ip:" .. ipHash .. ":" .. window

-- 只「看」不「加」：是否拒绝在本次脚本内立刻判死，拒绝一律不做 INCR——
-- 被挡的请求不消耗任何一层的名额（刷凶的地址不会把应用总量也吃光，
-- 应用总量满了也不会在某个地址的计数器上空加）。
if appLimit >= 0 then
    local appCount = tonumber(redis.call('GET', appKey) or '0')
    if appCount >= appLimit then
        return { 0, 0, retryAfter }
    end
end

if ipLimit >= 0 then
    local ipCount = tonumber(redis.call('GET', ipKey) or '0')
    if ipCount >= ipLimit then
        return { 0, 1, retryAfter }
    end
end

-- 两层都没满：对通过的层各自 +1。INCR 在 key 不存在时从 0 创建并置 1，
-- 仅此时给本窗口键设 TTL（已存在的键不动 TTL，保证整窗存活、到点整窗回收）。
if appLimit >= 0 then
    if redis.call('INCR', appKey) == 1 then
        redis.call('PEXPIRE', appKey, ttlMs)
    end
end
if ipLimit >= 0 then
    if redis.call('INCR', ipKey) == 1 then
        redis.call('PEXPIRE', ipKey, ttlMs)
    end
end

return { 1, 0, 0 }
