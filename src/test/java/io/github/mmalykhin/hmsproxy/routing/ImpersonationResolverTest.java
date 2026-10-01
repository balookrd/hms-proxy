package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogExposureMode;
import io.github.mmalykhin.hmsproxy.config.catalog.ViewTextRewriteMode;
import io.github.mmalykhin.hmsproxy.config.compatibility.CompatibilityConfig;
import io.github.mmalykhin.hmsproxy.config.ddlguard.TransactionalDdlGuardConfig;
import io.github.mmalykhin.hmsproxy.config.ddlguard.TransactionalDdlGuardMode;
import io.github.mmalykhin.hmsproxy.config.federation.FederationConfig;
import io.github.mmalykhin.hmsproxy.config.management.ManagementConfig;
import io.github.mmalykhin.hmsproxy.config.ratelimit.RateLimitConfig;
import io.github.mmalykhin.hmsproxy.config.routing.AdaptiveTimeoutConfig;
import io.github.mmalykhin.hmsproxy.config.routing.BackendConfig;
import io.github.mmalykhin.hmsproxy.config.routing.BackendStatePollingConfig;
import io.github.mmalykhin.hmsproxy.config.routing.CircuitBreakerConfig;
import io.github.mmalykhin.hmsproxy.config.routing.DatabaseListCacheConfig;
import io.github.mmalykhin.hmsproxy.config.routing.DatabaseMetadataCacheConfig;
import io.github.mmalykhin.hmsproxy.config.routing.DegradedRoutingPolicy;
import io.github.mmalykhin.hmsproxy.config.routing.HedgedReadConfig;
import io.github.mmalykhin.hmsproxy.config.routing.IcebergPointerGuardConfig;
import io.github.mmalykhin.hmsproxy.config.routing.LatencyRoutingConfig;
import io.github.mmalykhin.hmsproxy.config.security.CatalogRangerConfig;
import io.github.mmalykhin.hmsproxy.config.security.RangerConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityMode;
import io.github.mmalykhin.hmsproxy.config.server.FrontendProfile;
import io.github.mmalykhin.hmsproxy.config.server.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;
import io.github.mmalykhin.hmsproxy.security.ClientRequestContext;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TTransport;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ImpersonationResolverTest {

  private ImpersonationResolver resolver;
  private TTransport transport;

  @Before
  public void setUp() {
    CatalogRangerConfig rangerConfig = new CatalogRangerConfig(
        true, "http://localhost:6080", "hive", "hive", "hms-proxy", null, 30000L, 5000, 5000, null, null, null, false);
    RangerConfig ranger = new RangerConfig(true, rangerConfig, Map.of());

    ProxyConfig config = new ProxyConfig(
        new ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new SecurityConfig(SecurityMode.KERBEROS, "hive/proxy@REALM", "/tmp/k.keytab", null, null, true, Map.of()),
        ".",
        "default",
        Map.of(),
        new BackendConfig(Map.of()),
        new CompatibilityConfig(FrontendProfile.APACHE_3_1_3, null, null, false),
        new FederationConfig(false, ViewTextRewriteMode.DISABLED, false),
        new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.DISABLED, List.of()),
        new ManagementConfig(false, "127.0.0.1", 10083),
        null,
        SyntheticReadLockStoreConfig.inMemory(),
        RateLimitConfig.disabled(),
        new LatencyRoutingConfig(
            new BackendStatePollingConfig(false, 10_000, 5_000L),
            new AdaptiveTimeoutConfig(false, 2_000L, 1_000L, 10_000L, 4.0d, 0.2d),
            new CircuitBreakerConfig(false, 1, 200L),
            new HedgedReadConfig(false, 1, 30_000L),
            DegradedRoutingPolicy.STRICT,
            new DatabaseListCacheConfig(60_000L, 100, true),
            new DatabaseMetadataCacheConfig(60_000L, 100, true)),
        IcebergPointerGuardConfig.defaults(),
        List.of(),
        ranger
    );

    resolver = new ImpersonationResolver(config);
    transport = new TMemoryBuffer(1024);
    ClientRequestContext.setCurrentTransport(transport);
  }

  @After
  public void tearDown() {
    ClientRequestContext.setConnectionUgi(transport, null);
    ClientRequestContext.restoreCurrentTransport(null);
    ClientRequestContext.restoreRemoteUser(null);
  }

  @Test
  public void testResolveFromRemoteUserResolvesGroups() throws Exception {
    UserGroupInformation aliceUgi =
        UserGroupInformation.createUserForTesting("alice", new String[]{"sales_grp", "analysts"});

    aliceUgi.doAs((PrivilegedExceptionAction<Void>) () -> {
      ClientRequestContext.setRemoteUser("alice@REALM");
      Optional<ImpersonationContext> ctx = resolver.resolve();
      Assert.assertTrue(ctx.isPresent());
      Assert.assertEquals("alice", ctx.get().userName());
      Assert.assertEquals(List.of("sales_grp", "analysts"), ctx.get().groupNames());
      return null;
    });
  }

  @Test
  public void testResolveFromEmptyConnectionUgiResolvesGroups() throws Exception {
    UserGroupInformation bobUgi =
        UserGroupInformation.createUserForTesting("bob", new String[]{"finance_grp"});

    bobUgi.doAs((PrivilegedExceptionAction<Void>) () -> {
      // Simulate set_ugi called with empty groups
      ClientRequestContext.setConnectionUgi(transport, new ImpersonationContext("bob", List.of()));
      Optional<ImpersonationContext> ctx = resolver.resolve();
      Assert.assertTrue(ctx.isPresent());
      Assert.assertEquals("bob", ctx.get().userName());
      Assert.assertEquals(List.of("finance_grp"), ctx.get().groupNames());
      return null;
    });
  }

  @Test
  public void testResolveFromNonEmptyConnectionUgiPreservesProvidedGroups() throws Exception {
    ClientRequestContext.setConnectionUgi(transport, new ImpersonationContext("charlie", List.of("custom_group")));
    Optional<ImpersonationContext> ctx = resolver.resolve();
    Assert.assertTrue(ctx.isPresent());
    Assert.assertEquals("charlie", ctx.get().userName());
    Assert.assertEquals(List.of("custom_group"), ctx.get().groupNames());
  }

  @Test
  public void testResolveServicePrincipalReturnsEmpty() throws Exception {
    ClientRequestContext.setRemoteUser("hive/proxy@REALM");
    Optional<ImpersonationContext> ctx = resolver.resolve();
    Assert.assertFalse(ctx.isPresent());
  }

  @Test
  public void testDisabledImpersonationReturnsEmpty() throws Exception {
    ProxyConfig disabledConfig = new ProxyConfig(
        new ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new SecurityConfig(SecurityMode.NONE, null, null, null, null, true, Map.of()),
        ".",
        "default",
        Map.of(),
        new BackendConfig(Map.of()),
        new CompatibilityConfig(FrontendProfile.APACHE_3_1_3, null, null, false),
        new FederationConfig(false, ViewTextRewriteMode.DISABLED, false),
        new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.DISABLED, List.of()),
        new ManagementConfig(false, "127.0.0.1", 10083),
        null,
        SyntheticReadLockStoreConfig.inMemory(),
        RateLimitConfig.disabled(),
        new LatencyRoutingConfig(
            new BackendStatePollingConfig(false, 10_000, 5_000L),
            new AdaptiveTimeoutConfig(false, 2_000L, 1_000L, 10_000L, 4.0d, 0.2d),
            new CircuitBreakerConfig(false, 1, 200L),
            new HedgedReadConfig(false, 1, 30_000L),
            DegradedRoutingPolicy.STRICT,
            new DatabaseListCacheConfig(60_000L, 100, true),
            new DatabaseMetadataCacheConfig(60_000L, 100, true)),
        IcebergPointerGuardConfig.defaults(),
        List.of(),
        RangerConfig.disabled()
    );

    ImpersonationResolver disabledResolver = new ImpersonationResolver(disabledConfig);
    ClientRequestContext.setRemoteUser("alice@REALM");
    Assert.assertFalse(disabledResolver.resolve().isPresent());
  }
}
