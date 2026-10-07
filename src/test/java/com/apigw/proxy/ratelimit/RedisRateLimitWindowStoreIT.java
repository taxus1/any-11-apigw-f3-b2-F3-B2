package com.apigw.proxy.ratelimit;

import com.apigw.domain.ratelimit.RateLimitScope;
import com.apigw.domain.ratelimit.RateQuota;
import com.apigw.support.EnabledIfRedis;
import com.apigw.support.RedisAvailableCondition;
import io.lettuce.core.resource.ClientResources;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Redis 固定窗口计数的真机集成测试（连真实 Redis；不可达时整类跳过）。
 *
 * 用短窗口（2s）加速窗口轮转，验证：
 * - 精确放行到额度、第 N+1 笔拒绝且 Retry-After 在窗口剩余范围内；
 * - 拒绝不占名额（被 IP 层挡住的请求不消耗应用总量）；
 * - 窗口到点从零重新计数，不带余数；
 * - 新实例在窗口中途接入：本窗额度只反映真实用量，启动本身一分不占（无启动补记）；
 * - 两层额度共用同一份窗口口径：同一个窗口号、同随第一笔真实放行从 0 建起；
 * - Retry-After 与窗口边界同源：按它等够时长，下一笔必进新窗放行；
 * - 多线程并发在同一窗口抢名额：全局计数下「放行数恰等于额度」，不超发也不少放；
 * - 计数键带 TTL，窗口结束后自动过期被回收（存储不无限膨胀）。
 */
@EnabledIfRedis
class RedisRateLimitWindowStoreIT {

    private ReactiveRedisConnectionFactory factory;
    private ReactiveStringRedisTemplate redis;
    /** 测试用 2 秒窗口。 */
    private RedisRateLimitWindowStore store;
    private String app;

    @BeforeEach
    void setUp() {
        var clientConfig = LettuceClientConfiguration.builder()
                .clientResources(ClientResources.create()).build();
        var connConfig = new RedisStandaloneConfiguration(
                RedisAvailableCondition.host(), RedisAvailableCondition.port());
        var lettuceFactory = new LettuceConnectionFactory(connConfig, clientConfig);
        lettuceFactory.afterPropertiesSet();
        this.factory = lettuceFactory;
        this.redis = new ReactiveStringRedisTemplate(lettuceFactory);
        this.store = new RedisRateLimitWindowStore(redis, 2);
        this.app = "it-" + UUID.randomUUID().toString().substring(0, 8);
    }

    @AfterEach
    void tearDown() {
        // 清掉本应用 hash tag 下的所有键
        var keys = redis.keys("apigw:rl:{" + app + "}*").collectList().block();
        if (keys != null && !keys.isEmpty()) {
            redis.delete(Flux.fromIterable(keys)).block();
        }
        ((LettuceConnectionFactory) factory).destroy();
    }

    private RateLimitVerdict check(String ip, Integer appL, Integer ipL) {
        return store.checkAndConsume(app, ip, appL, ipL).block(Duration.ofSeconds(3));
    }

    /** Redis 服务端时钟（毫秒）——窗口口径的唯一来源，测试对齐边界也按它，不另用本机钟。 */
    private long redisNowMs() {
        DefaultRedisScript<List> timeScript = new DefaultRedisScript<>("return redis.call('TIME')", List.class);
        List<?> t = redis.execute(timeScript).next().block(Duration.ofSeconds(3));
        return Long.parseLong(String.valueOf(t.get(0))) * 1000L
                + Long.parseLong(String.valueOf(t.get(1))) / 1000L;
    }

    /** 等到「当前窗剩余时长 >= needMs」的起点，避免用例断言跨窗（边界以 Redis TIME 为准）。 */
    private void waitForFreshWindow(long windowMs, long needMs) throws InterruptedException {
        long into = redisNowMs() % windowMs;
        if (into > windowMs - needMs) {
            Thread.sleep(windowMs - into + 100);
        }
    }

    private long counterValue(String key) {
        return redis.opsForValue().get(key).map(Long::parseLong).block(Duration.ofSeconds(3));
    }

    @Test
    void allowsExactlyAppLimit_thenRejectsWithRetryAfter() {
        RateLimitVerdict v1 = check("1.1.1.1", 3, null);
        RateLimitVerdict v2 = check("2.2.2.2", 3, null);
        RateLimitVerdict v3 = check("3.3.3.3", 3, null);
        assertThat(List.of(v1.allowed(), v2.allowed(), v3.allowed())).containsOnly(true);

        RateLimitVerdict rejected = check("4.4.4.4", 3, null);
        assertThat(rejected.allowed()).isFalse();
        assertThat(rejected.blockedScope()).isEqualTo("APP");
        // 2s 窗口内等待秒数只能是 1 或 2（向上取整）
        assertThat(rejected.retryAfterSec()).isBetween(1L, 2L);
    }

    @Test
    void ipLayer_isIndependent_andRejectedDoesNotConsumeAppQuota() {
        // 应用总量 100，坏地址额度 2
        assertThat(check("9.9.9.9", 100, 2).allowed()).isTrue();
        assertThat(check("9.9.9.9", 100, 2).allowed()).isTrue();
        RateLimitVerdict blocked = check("9.9.9.9", 100, 2);
        assertThat(blocked.allowed()).isFalse();
        assertThat(blocked.blockedScope()).isEqualTo("IP");

        // 坏地址再刷多次全被挡；同应用其他地址照常，证明被挡请求没吃应用总量
        for (int i = 0; i < 10; i++) {
            assertThat(check("9.9.9.9", 100, 2).allowed()).isFalse();
        }
        for (int i = 0; i < 5; i++) {
            assertThat(check("8.8.8." + i, 100, null).allowed()).isTrue();
        }
    }

    @Test
    void windowResetsToZero_afterBoundary() throws InterruptedException {
        RateLimitVerdict first = check("1.1.1.1", 1, null);
        assertThat(first.allowed()).isTrue();
        assertThat(check("1.1.1.1", 1, null).allowed()).isFalse();

        // 等过窗口边界（2s 窗口 + 宽限余量）
        Thread.sleep(2_300);

        // 新窗口从 0 开始：上一窗余数不带过来
        assertThat(check("1.1.1.1", 1, null).allowed()).isTrue();
        assertThat(check("1.1.1.1", 1, null).allowed()).isFalse();
    }

    @Test
    void newInstanceMidWindow_startupConsumesNothing_quotaIsFullyReal() throws InterruptedException {
        // 回归（真限流器 + 真 Redis）：实例在窗口中途「起来」，本窗额度全归真实请求。
        // 旧「启动补记」按窗口已过比例预占额度（100 次/分的应用起步只剩几十次、头一窗
        // 提前 429），已移除——启动动作本身不消耗额度。
        waitForFreshWindow(2_000, 1_200);
        RateLimiterTest.FakeRepository repo = new RateLimiterTest.FakeRepository();
        repo.rows.add(RateQuota.reconstitute(RateLimitScope.APP, app, null, 100, null, null, null));
        RateLimitCatalog catalog = new RateLimitCatalog(repo);
        catalog.refreshBlock(Duration.ofSeconds(5));
        RateLimiter limiter = new RateLimiter(catalog, store,
                new RateLimitProperties(true, Duration.ofSeconds(10), 10_000,
                        Duration.ofMillis(500), true, 5, 5_000, 2));

        int realRequests = 30;
        for (int i = 0; i < realRequests; i++) {
            assertThat(limiter.check(app, "1.1.1.1").block(Duration.ofSeconds(3)).allowed()).isTrue();
        }

        // Redis 里的窗口计数恰好等于真实请求数：没有一笔名额被启动动作凭白占掉
        var keys = redis.keys("apigw:rl:{" + app + "}:app:*").collectList().block();
        assertThat(keys).hasSize(1);
        assertThat(counterValue(keys.get(0))).isEqualTo(realRequests);
    }

    @Test
    void bothLayers_oneWindowSource_bornAtZeroTogether() throws InterruptedException {
        // 两层额度共用同一份窗口口径：窗口刚建立时，应用层与来源层的计数键挂在同一个
        // 窗口号下，都随本层第一笔真实放行从 0 建起（不存在一层被预占、一层从 0 起的分裂）
        waitForFreshWindow(2_000, 1_200);
        for (int i = 0; i < 3; i++) {
            assertThat(check("5.5.5.5", 100, 100).allowed()).isTrue();
        }
        var appKeys = redis.keys("apigw:rl:{" + app + "}:app:*").collectList().block();
        var ipKeys = redis.keys("apigw:rl:{" + app + "}:ip:*").collectList().block();
        assertThat(appKeys).hasSize(1);
        assertThat(ipKeys).hasSize(1);
        String window = appKeys.get(0).substring(appKeys.get(0).lastIndexOf(':') + 1);
        assertThat(ipKeys.get(0)).endsWith(":" + window);
        assertThat(counterValue(appKeys.get(0))).isEqualTo(3);
        assertThat(counterValue(ipKeys.get(0))).isEqualTo(3);

        // 跨窗后两层仍同口径：新窗第一笔放行，两层新键挂在同一个新窗口号下、同从 1 起
        Thread.sleep(2_300);
        assertThat(check("5.5.5.5", 100, 100).allowed()).isTrue();
        String nextWindow = String.valueOf(Long.parseLong(window) + 1);
        var nextAppKeys = redis.keys("apigw:rl:{" + app + "}:app:" + nextWindow).collectList().block();
        var nextIpKeys = redis.keys("apigw:rl:{" + app + "}:ip:*:" + nextWindow).collectList().block();
        assertThat(nextAppKeys).hasSize(1);
        assertThat(nextIpKeys).hasSize(1);
        assertThat(counterValue(nextAppKeys.get(0))).isEqualTo(1);
        assertThat(counterValue(nextIpKeys.get(0))).isEqualTo(1);
    }

    @Test
    void retryAfter_isTheRealWindowBoundary_waitItOutAndPass() throws InterruptedException {
        // 窗口边界与 Retry-After 同一口径：按拒绝时给的秒数等够，下一笔必已进入新窗
        assertThat(check("1.1.1.1", 1, null).allowed()).isTrue();
        RateLimitVerdict rejected = check("1.1.1.1", 1, null);
        assertThat(rejected.allowed()).isFalse();
        assertThat(rejected.retryAfterSec()).isBetween(1L, 2L);

        Thread.sleep(rejected.retryAfterSec() * 1000L + 200L);

        assertThat(check("1.1.1.1", 1, null).allowed()).isTrue();
    }

    @Test
    void twoInstances_oneWindowSource_oneKeyExactTotal() throws InterruptedException {
        // 窗口口径只有 Redis TIME 一份：两个存储实例（=两台网关）划出的必然是同一个窗口，
        // 全窗只有一个计数键、合计放行恰为额度——不会各按各的钟拆出两个键各放一份
        RedisRateLimitWindowStore gw1 = new RedisRateLimitWindowStore(redis, 60);
        RedisRateLimitWindowStore gw2 = new RedisRateLimitWindowStore(redis, 60);
        waitForFreshWindow(60_000, 10_000);

        int allowed = 0;
        for (int i = 0; i < 12; i++) {
            RedisRateLimitWindowStore gw = (i % 2 == 0) ? gw1 : gw2;
            if (gw.checkAndConsume(app, "1.1.1." + i, 5, null).block(Duration.ofSeconds(3)).allowed()) {
                allowed++;
            }
        }
        assertThat(allowed).isEqualTo(5);

        var keys = redis.keys("apigw:rl:{" + app + "}:app:*").collectList().block();
        assertThat(keys).hasSize(1);
        assertThat(counterValue(keys.get(0))).isEqualTo(5);
    }

    @Test
    void concurrentBurst_neverOverAdmits() throws InterruptedException {
        int limit = 200;
        int threads = 16;
        int perThread = 40; // 合计 640 抢 200
        // 本用例的不变量「放行恰等于额度」只在同一个窗口内成立（固定窗口每窗各放一份额度）。
        // 用独立的 60s 长窗存储，并避开窗口边界再齐射，防止突发流量跨窗把两窗的额度算进一次断言
        RedisRateLimitWindowStore longWindowStore = new RedisRateLimitWindowStore(redis, 60);
        long now = System.currentTimeMillis();
        long intoWindow = now % 60_000L;
        if (intoWindow > 55_000L) {
            Thread.sleep(60_000L - intoWindow + 1_000L);
        }
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger allowed = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();

        for (int t = 0; t < threads; t++) {
            final String ip = "10.0.0." + t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        RateLimitVerdict v = longWindowStore
                                .checkAndConsume(app, ip, limit, null)
                                .block(Duration.ofSeconds(3));
                        if (v.allowed()) {
                            allowed.incrementAndGet();
                        } else {
                            rejected.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
        pool.shutdownNow();

        // 硬性不变量：全局共享计数下，16 线程合计放行恰好 = 额度，不多也不少
        assertThat(allowed.get()).isEqualTo(limit);
        assertThat(rejected.get()).isEqualTo(threads * perThread - limit);

        // Redis 里该窗口键的终值也必须正好停在额度上
        var keyNames = redis.keys("apigw:rl:{" + app + "}:app:*").collectList().block();
        assertThat(keyNames).hasSize(1);
        Long finalCount = redis.opsForValue().get(keyNames.get(0)).map(Long::parseLong).block();
        assertThat(finalCount).isEqualTo(limit);
    }

    @Test
    void windowKeys_expireByTtl() throws InterruptedException {
        assertThat(check("1.1.1.1", 1, null).allowed()).isTrue();
        var before = redis.keys("apigw:rl:{" + app + "}:app:*").collectList().block();
        assertThat(before).hasSize(1);
        Duration ttl = redis.getExpire(before.get(0)).block();
        // TTL = 窗口 2s + 30s 宽限；放宽断言在 2s..35s 之间
        assertThat(ttl).isNotNull();
        assertThat(ttl.getSeconds()).isBetween(2L, 35L);

        // 等过 TTL（2s 窗口 + 30s 宽限，再留余量）
        Thread.sleep(33_000);
        var after = redis.keys("apigw:rl:{" + app + "}:app:*").collectList().block();
        assertThat(after == null || after.isEmpty()).isTrue();
    }

    @Test
    void bothLayersUnlimited_doesNotCreateKeys() {
        for (int i = 0; i < 5; i++) {
            RateLimitVerdict v = check("1.1.1.1", null, null);
            assertThat(v.allowed()).isTrue();
        }
        var keys = redis.keys("apigw:rl:{" + app + "}*").collectList().block();
        // 两层都不限时脚本不 INCR；锚点键也不应被创建（从未写入）
        List<String> concrete = new ArrayList<>();
        if (keys != null) {
            concrete.addAll(keys);
        }
        assertThat(concrete).isEmpty();
    }
}
