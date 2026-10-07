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

    /**
     * 可选能力：把「本窗口已经过去的那部分」按比例先占住（新实例启动后补一次）。
     *
     * <p>默认空实现——不支持的存储按「什么都不补」处理，判定口径不受影响。
     * 实现它的存储要用自己的时钟算窗口，保证补记与判定落在同一个窗口键上。
     *
     * @param appNo     应用编号
     * @param appLimit  应用层每分钟额度；null=该层不限，无需补记
     * @param windowMs  窗口长度（毫秒）
     */
    default Mono<Void> precharge(String appNo, Integer appLimit, long windowMs) {
        return Mono.empty();
    }
}
