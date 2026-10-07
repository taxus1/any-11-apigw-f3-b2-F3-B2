package com.apigw.proxy.ratelimit;

import reactor.core.publisher.Mono;

/**
 * 限流计数窗口存储端口。实现（Redis 见 {@link RedisRateLimitWindowStore}）必须做到：
 *
 * 1. 计数全局共享：多台网关实例访问同一份存储，同一应用在哪台机器上计数都累加在一起；
 * 2. 检查与占名额原子：{@link #checkAndConsume} 一次调用完成「看两层当前计数 → 判额度
 *    → 通过才各占一次」，并发下不超发、也不少放（靠单段 Lua 在 Redis 单线程内原子执行）；
 * 3. 拒绝不占名额：任一层超了立即拒绝，被挡请求不 INCR 任何计数器；
 * 4. 窗口自带 TTL：过期窗口键由存储自动回收，键空间不无限增长。
 *
 * 启动不占额度：存储只如实记录本窗真实放过的请求，没有、也不允许有任何「新实例启动预占」
 * 一类的入口——窗口口径（窗口号、起止、Retry-After）只存在于判定脚本那一份里。
 *
 * 故障语义：存储超时/连不上/执行出错时以 {@code onError} 信号外抛（且必须在很短的
 * redisTimeout 内发生，绝不死等），由上层的 {@link RateLimiter} 按既定策略 fail-open/
 * fail-closed——存储实现自己不做放行决策。
 */
public interface RateLimitWindowStore {

    /**
     * 原子判定并在通过时占一个名额。
     *
     * @param appNo       已通过鉴权的可信应用编号
     * @param canonicalIp 规范化来源地址（ClientIpResolver 口径）
     * @param appLimit    应用层每分钟额度；null=该层不限
     * @param ipLimit     来源层每分钟额度；null=该层不限
     */
    Mono<RateLimitVerdict> checkAndConsume(String appNo, String canonicalIp,
                                           Integer appLimit, Integer ipLimit);
}
