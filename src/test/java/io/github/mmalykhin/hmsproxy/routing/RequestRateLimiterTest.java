package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics;
import io.github.mmalykhin.hmsproxy.security.ClientRequestContext;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;
import org.junit.Test;

public class RequestRateLimiterTest {
  @Test
  public void classifiesRepresentativeMethodFamiliesAndRpcClasses() {
    RequestRateLimiter limiter =
        new RequestRateLimiter(ProxyConfig.RateLimitConfig.disabled(), new PrometheusMetrics(), () -> 0L);

    RequestRateLimiter.RequestClassification read = limiter.classify("get_table");
    RequestRateLimiter.RequestClassification ddl = limiter.classify("create_table");
    RequestRateLimiter.RequestClassification txn = limiter.classify("open_txns");
    RequestRateLimiter.RequestClassification lock = limiter.classify("show_locks");

    Assert.assertEquals("metadata_read", read.methodFamily());
    Assert.assertEquals(List.of(), read.rpcClasses());
    Assert.assertTrue(ddl.rpcClasses().contains("write"));
    Assert.assertTrue(ddl.rpcClasses().contains("ddl"));
    Assert.assertTrue(txn.rpcClasses().contains("write"));
    Assert.assertTrue(txn.rpcClasses().contains("txn"));
    Assert.assertTrue(lock.rpcClasses().contains("lock"));
    Assert.assertFalse(lock.rpcClasses().contains("write"));
  }

  @Test
  public void enforcesPerPrincipalIndependently() throws Exception {
    AtomicLong nowNanos = new AtomicLong(1L);
    RequestRateLimiter limiter = new RequestRateLimiter(
        new ProxyConfig.RateLimitConfig(
            new ProxyConfig.RateLimitPolicyConfig(1, 1),
            ProxyConfig.RateLimitPolicyConfig.disabled(),
            Map.of(),
            Map.of(),
            Map.of(),
            Map.of()),
        new PrometheusMetrics(),
        nowNanos::get);

    withClientContext(null, "alice@EXAMPLE.COM", () -> limiter.enforceRequest("getName"));
    try {
      withClientContext(null, "alice@EXAMPLE.COM", () -> limiter.enforceRequest("getName"));
      Assert.fail("Expected rate limit for repeated principal");
    } catch (RateLimitExceededException e) {
      Assert.assertTrue(e.getMessage().contains("principal"));
    }
    withClientContext(null, "bob@EXAMPLE.COM", () -> limiter.enforceRequest("getName"));
  }

  @Test
  public void enforcesAggregateSourceCidrLimitsAcrossDifferentIps() throws Exception {
    AtomicLong nowNanos = new AtomicLong(1L);
    RequestRateLimiter limiter = new RequestRateLimiter(
        new ProxyConfig.RateLimitConfig(
            ProxyConfig.RateLimitPolicyConfig.disabled(),
            ProxyConfig.RateLimitPolicyConfig.disabled(),
            Map.of(
                "corp",
                new ProxyConfig.SourceCidrRateLimitConfig(
                    List.of("10.10.0.0/16"),
                    new ProxyConfig.RateLimitPolicyConfig(1, 1))),
            Map.of(),
            Map.of(),
            Map.of()),
        new PrometheusMetrics(),
        nowNanos::get);

    withClientContext("10.10.1.10", null, () -> limiter.enforceRequest("get_table"));
    try {
      withClientContext("10.10.2.20", null, () -> limiter.enforceRequest("get_table"));
      Assert.fail("Expected CIDR-scoped rate limit to be shared across IPs");
    } catch (RateLimitExceededException e) {
      Assert.assertTrue(e.getMessage().contains("source CIDR rule"));
    }
  }

  @Test
  public void enforcesConfiguredCatalogLimits() throws Exception {
    AtomicLong nowNanos = new AtomicLong(1L);
    RequestRateLimiter limiter = new RequestRateLimiter(
        new ProxyConfig.RateLimitConfig(
            ProxyConfig.RateLimitPolicyConfig.disabled(),
            ProxyConfig.RateLimitPolicyConfig.disabled(),
            Map.of(),
            Map.of(),
            Map.of("catalog1", new ProxyConfig.RateLimitPolicyConfig(1, 1)),
            Map.of()),
        new PrometheusMetrics(),
        nowNanos::get);

    limiter.enforceCatalog("get_table", "catalog1");
    try {
      limiter.enforceCatalog("get_table", "catalog1");
      Assert.fail("Expected catalog-scoped rate limit");
    } catch (RateLimitExceededException e) {
      Assert.assertTrue(e.getMessage().contains("catalog 'catalog1'"));
    }
    limiter.enforceCatalog("get_table", "catalog2");
  }

  private static void withClientContext(String remoteAddress, String remoteUser, CheckedRunnable runnable) throws Exception {
    String previousRemoteAddress = ClientRequestContext.setRemoteAddress(remoteAddress);
    String previousRemoteUser = ClientRequestContext.setRemoteUser(remoteUser);
    try {
      runnable.run();
    } finally {
      ClientRequestContext.restoreRemoteAddress(previousRemoteAddress);
      ClientRequestContext.restoreRemoteUser(previousRemoteUser);
    }
  }

  @FunctionalInterface
  private interface CheckedRunnable {
    void run() throws Exception;
  }
}
