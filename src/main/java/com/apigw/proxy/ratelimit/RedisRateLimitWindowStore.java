package com.apigw.proxy.ratelimit;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.List;

/**
 * 基于 Redis 的固定窗口计数实现（脚本见 classpath:lua/rate-limit.lua）。
 *
 * 计数 key 全部挂在同一个 hash tag {@code {<appNo>}} 下：Redis Cluster 按 tag 定槽，
 * 应用总量键与来源地址键由脚本从锚点键派生，保证落在同一槽，一个 Lua 同时操作两层键
 * （否则 CROSSSLOT）。单个应用的两层计数本就是同一热点域，按应用分槽也把负载摊开了。
 *
 * key 名（都在脚本内按 Redis 服务端时钟拼窗口号，网关机器时钟偏差不影响选 key）：
 *   apigw:rl:{appNo}                锚点键（不落值，只为提供 hash tag）
 *   apigw:rl:{appNo}:app:<window>   应用总量
 *   apigw:rl:{appNo}:ip:<hash>:<window>  来源地址计数；
 *     hash=SHA-256(规范化 IP) 十六进制——IPv6 规范字面量含冒号、长度不一，不直接拼进 key；
 *     同一来源的不同写法在 ClientIpResolver 已归一，哈希稳定唯一。
 *
 * 膨胀控制：计数键 TTL = 窗口长 + 30s 宽限（容忍边界/时钟小偏差），窗口一到就换 key，
 * 旧键 TTL 到期由 Redis 自动删除；存储里同一时刻每个「活跃应用」至多一个 app 键 +
 * 每个「活跃来源」一个 ip 键，且都活不过一分半，不需要任何定时扫描清理。
 */
@Slf4j
public class RedisRateLimitWindowStore implements RateLimitWindowStore {

    private static final String KEY_PREFIX = "apigw:rl:";
    /** 窗口结束后的宽限：保证整窗可计数 + 边界安全，之后由 Redis 回收。 */
    private static final long TTL_GRACE_MS = 30_000;
    /** 这一层不限时传给 Lua 的哨兵（额度都是非负数，-1 不会与真额度冲突）。 */
    private static final long UNLIMITED = -1L;

    @SuppressWarnings("rawtypes")
    private static final DefaultRedisScript<List> SCRIPT = createScript();
    @SuppressWarnings("rawtypes")
    private static final DefaultRedisScript<List> PRECHARGE_SCRIPT = createScript("lua/rate-limit-precharge.lua");

    private final ReactiveStringRedisTemplate redis;
    private final long windowMs;

    public RedisRateLimitWindowStore(ReactiveStringRedisTemplate redis, long windowSeconds) {
        this.redis = redis;
        this.windowMs = windowSeconds * 1000L;
    }

    @Override
    public Mono<RateLimitVerdict> checkAndConsume(String appNo, String canonicalIp,
                                                  Integer appLimit, Integer ipLimit) {
        // 只把锚点键作为 KEYS：真正的窗口键在脚本内用 redis TIME 算窗口号后派生，
        // 多台网关本地钟不一致也不会各算各的 key（那会把同一窗口拆成两个计数导致超发）
        String anchor = KEY_PREFIX + "{" + sanitize(appNo) + "}";
        String ipHash = sha256(canonicalIp == null ? "unknown" : canonicalIp);
        long ttlMs = windowMs + TTL_GRACE_MS;

        return redis.execute(
                        SCRIPT,
                        List.of(anchor),
                        List.of(
                                String.valueOf(windowMs),
                                String.valueOf(ttlMs),
                                appLimit == null ? String.valueOf(UNLIMITED) : String.valueOf(appLimit.longValue()),
                                ipLimit == null ? String.valueOf(UNLIMITED) : String.valueOf(ipLimit.longValue()),
                                ipHash))
                .next()
                .map(row -> {
                    @SuppressWarnings("unchecked")
                    List<Long> result = (List<Long>) row;
                    boolean allowed = result.get(0) == 1L;
                    String scope = result.get(1) == 1L ? "IP" : "APP";
                    long retryAfter = result.get(2);
                    return allowed
                            ? RateLimitVerdict.pass()
                            : RateLimitVerdict.reject(scope, retryAfter);
                });
    }

    @Override
    public Mono<Void> precharge(String appNo, Integer appLimit, long windowMs) {
        if (appLimit == null || appLimit <= 0) {
            return Mono.empty();
        }
        String anchor = KEY_PREFIX + "{" + sanitize(appNo) + "}";
        long ttlMs = windowMs + TTL_GRACE_MS;
        return redis.execute(PRECHARGE_SCRIPT, List.of(anchor),
                        List.of(String.valueOf(windowMs), String.valueOf(ttlMs),
                                String.valueOf(appLimit.longValue())))
                .then();
    }

    /** 应用编号只允许 [A-Za-z0-9._-]（鉴权头白名单 + 建号约束），这里再兜底剔掉花括号，防 tag 逃逸。 */
    private static String sanitize(String appNo) {
        StringBuilder sb = new StringBuilder(appNo.length());
        for (int i = 0; i < appNo.length(); i++) {
            char c = appNo.charAt(i);
            if (c == '{' || c == '}') {
                sb.append('_');
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private static String sha256(String value) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(md.digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 不可用", e);
        }
    }

    @SuppressWarnings("rawtypes")
    private static DefaultRedisScript<List> createScript() {
        return createScript("lua/rate-limit.lua");
    }

    @SuppressWarnings("rawtypes")
    private static DefaultRedisScript<List> createScript(String location) {
        DefaultRedisScript<List> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource(location));
        script.setResultType(List.class);
        return script;
    }
}
