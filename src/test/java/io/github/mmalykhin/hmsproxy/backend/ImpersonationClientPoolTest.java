package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogExposureMode;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityMode;
import io.github.mmalykhin.hmsproxy.config.server.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;
import io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics;
import java.lang.reflect.Constructor;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.junit.Assert;
import org.junit.Test;

public class ImpersonationClientPoolTest {
  private static final String SOCKET_TIMEOUT_KEY = "hive.metastore.client.socket.timeout";

  @Test
  public void poolServesConcurrentCallsForSameUserOnDistinctSessions() throws Exception {
    int parallelism = 4;
    CyclicBarrier barrier = new CyclicBarrier(parallelism);
    AtomicInteger sessionsOpened = new AtomicInteger();

    BackendRuntime.SessionFactory factory = new BackendRuntime.SessionFactory() {
      @Override
      public BackendInvocationSession open(
          ProxyConfig proxyConfig,
          CatalogConfig catalogConfig,
          HiveConf hiveConf,
          boolean backendKerberosEnabled,
          MetastoreRuntimeProfile runtimeProfile
      ) throws MetaException {
        sessionsOpened.incrementAndGet();
        return makeSession(barrier);
      }

      @Override
      public BackendInvocationSession openImpersonating(
          ProxyConfig proxyConfig,
          CatalogConfig catalogConfig,
          HiveConf hiveConf,
          boolean backendKerberosEnabled,
          MetastoreRuntimeProfile runtimeProfile,
          String userName,
          List<String> groupNames
      ) throws MetaException {
        return open(proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, runtimeProfile);
      }
    };

    PrometheusMetrics metrics = new PrometheusMetrics();
    CatalogConfig catalogConfig = catalogConfig(parallelism);
    BackendRuntime runtime = BackendRuntime.open(
        config(catalogConfig),
        catalogConfig,
        new HiveConf(),
        false,
        MetastoreRuntimeProfile.APACHE_3_1_3,
        factory,
        metrics);
    CatalogBackend backend = newBackend(catalogConfig, runtime, metrics);
    int baseline = sessionsOpened.get();

    ExecutorService executor = Executors.newFixedThreadPool(parallelism);
    try {
      ImpersonationContext ctx = new ImpersonationContext("alice", List.of("g1"));
      CountDownLatch start = new CountDownLatch(1);
      Future<?>[] futures = new Future<?>[parallelism];
      for (int i = 0; i < parallelism; i++) {
        futures[i] = executor.submit(() -> {
          start.await();
          try {
            backend.invokeRawByName("getStatus", new Class<?>[0], new Object[0], ctx);
          } catch (Throwable t) {
            throw new RuntimeException(t);
          }
          return null;
        });
      }
      start.countDown();
      for (Future<?> f : futures) {
        f.get(10, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
      backend.close();
    }

    int impersonationSessionsOpened = sessionsOpened.get() - baseline;
    Assert.assertEquals(
        "expected pool to open one session per concurrent caller (incl. pre-warmed)",
        parallelism,
        impersonationSessionsOpened);
    String rendered = metrics.render();
    Assert.assertTrue(
        "expected impersonation pool gauges in metrics output:\n" + rendered,
        rendered.contains("hms_proxy_impersonation_pool_users")
            && rendered.contains("hms_proxy_impersonation_pool_sessions"));
  }

  @Test
  public void evictionDiscardsAllPooledSessionsForUser() throws Exception {
    AtomicInteger sessionsOpened = new AtomicInteger();
    BackendRuntime.SessionFactory factory = new BackendRuntime.SessionFactory() {
      @Override
      public BackendInvocationSession open(
          ProxyConfig proxyConfig,
          CatalogConfig catalogConfig,
          HiveConf hiveConf,
          boolean backendKerberosEnabled,
          MetastoreRuntimeProfile runtimeProfile
      ) throws MetaException {
        sessionsOpened.incrementAndGet();
        return makeSession(null);
      }

      @Override
      public BackendInvocationSession openImpersonating(
          ProxyConfig proxyConfig,
          CatalogConfig catalogConfig,
          HiveConf hiveConf,
          boolean backendKerberosEnabled,
          MetastoreRuntimeProfile runtimeProfile,
          String userName,
          List<String> groupNames
      ) throws MetaException {
        return open(proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, runtimeProfile);
      }
    };

    PrometheusMetrics metrics = new PrometheusMetrics();
    CatalogConfig catalogConfig = catalogConfig(4);
    BackendRuntime runtime = BackendRuntime.open(
        config(catalogConfig),
        catalogConfig,
        new HiveConf(),
        false,
        MetastoreRuntimeProfile.APACHE_3_1_3,
        factory,
        metrics);
    CatalogBackend backend = newBackend(catalogConfig, runtime, metrics);

    try {
      ImpersonationContext ctx = new ImpersonationContext("alice", List.of("g1"));
      try {
        backend.invokeRawByName("getStatus", new Class<?>[0], new Object[0], ctx);
      } catch (Throwable t) {
        throw new RuntimeException(t);
      }
    } finally {
      backend.close();
    }

    String rendered = metrics.render();
    Assert.assertTrue(
        "expected eviction counter to appear after close:\n" + rendered,
        rendered.contains("hms_proxy_impersonation_session_evictions_total"));
  }

  @Test
  public void failedAdaptiveTimeoutReconnectRollsBackConfAndArmsCooldown() throws Exception {
    AtomicInteger sessionsOpened = new AtomicInteger();
    BackendRuntime.SessionFactory factory = new BackendRuntime.SessionFactory() {
      @Override
      public BackendInvocationSession open(
          ProxyConfig proxyConfig,
          CatalogConfig catalogConfig,
          HiveConf hiveConf,
          boolean backendKerberosEnabled,
          MetastoreRuntimeProfile runtimeProfile
      ) throws MetaException {
        if (sessionsOpened.incrementAndGet() > 1) {
          throw new MetaException("backend reconnect failed");
        }
        return makeSession(null);
      }

      @Override
      public BackendInvocationSession openImpersonating(
          ProxyConfig proxyConfig,
          CatalogConfig catalogConfig,
          HiveConf hiveConf,
          boolean backendKerberosEnabled,
          MetastoreRuntimeProfile runtimeProfile,
          String userName,
          List<String> groupNames
      ) throws MetaException {
        return open(proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, runtimeProfile);
      }
    };

    PrometheusMetrics metrics = new PrometheusMetrics();
    CatalogConfig catalogConfig = catalogConfig(4);
    BackendRuntime runtime = BackendRuntime.open(
        config(catalogConfig),
        catalogConfig,
        new HiveConf(),
        false,
        MetastoreRuntimeProfile.APACHE_3_1_3,
        factory,
        metrics);
    HiveConf conf = new HiveConf();
    conf.set(SOCKET_TIMEOUT_KEY, "10s");
    CatalogBackend backend = newBackend(catalogConfig, runtime, metrics, conf);

    try {
      try {
        backend.ensureClientSocketTimeout(60_000L, 60_000L);
        Assert.fail("expected the failed reconnect to surface to the caller");
      } catch (MetaException expected) {
        // reconnect could not open a fresh shared session
      }

      Assert.assertEquals(
          "hiveConf must not keep the timeout of a reconnect that never happened",
          "10s",
          conf.get(SOCKET_TIMEOUT_KEY));
      Assert.assertEquals(
          CatalogBackend.AdaptiveTimeoutResult.SKIPPED_COOLDOWN,
          backend.ensureClientSocketTimeout(60_000L, 60_000L));
      Assert.assertEquals(
          "failed reconnect must not be retried while cooling down", 2, sessionsOpened.get());
    } finally {
      backend.close();
    }
  }

  private static ProxyConfig config(CatalogConfig catalogConfig) {
    return ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog(catalogConfig.name())
        .catalogs(Map.of(catalogConfig.name(), catalogConfig))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();
  }

  private static CatalogConfig catalogConfig(int impersonationPoolMaxSize) {
    return new CatalogConfig(
        "catalog1",
        "c1",
        "file:///c1",
        true,
        CatalogAccessMode.READ_WRITE,
        List.of(),
        CatalogExposureMode.ALLOW_ALL,
        List.of(),
        Map.of(),
        MetastoreRuntimeProfile.APACHE_3_1_3,
        null,
        Map.of("hive.metastore.uris", "thrift://one"),
        0L,
        128,
        0L,
        1,
        impersonationPoolMaxSize,
        0L);
  }

  private static CatalogBackend newBackend(
      CatalogConfig catalogConfig,
      BackendRuntime runtime,
      PrometheusMetrics metrics
  ) throws Exception {
    return newBackend(catalogConfig, runtime, metrics, new HiveConf());
  }

  private static CatalogBackend newBackend(
      CatalogConfig catalogConfig,
      BackendRuntime runtime,
      PrometheusMetrics metrics,
      HiveConf hiveConf
  ) throws Exception {
    Catalog catalog = new Catalog();
    catalog.setName(catalogConfig.name());
    catalog.setDescription(catalogConfig.description());
    catalog.setLocationUri(catalogConfig.locationUri());
    BackendAdapter adapter = new ApacheBackendAdapter();
    Constructor<CatalogBackend> ctor = CatalogBackend.class.getDeclaredConstructor(
        ProxyConfig.class,
        CatalogConfig.class,
        HiveConf.class,
        BackendAdapter.class,
        BackendRuntime.class,
        Catalog.class,
        PrometheusMetrics.class);
    ctor.setAccessible(true);
    return ctor.newInstance(config(catalogConfig), catalogConfig, hiveConf, adapter, runtime, catalog, metrics);
  }

  private static BackendInvocationSession makeSession(CyclicBarrier barrier) throws MetaException {
    ThriftHiveMetastore.Iface thriftClient = (ThriftHiveMetastore.Iface) Proxy.newProxyInstance(
        ThriftHiveMetastore.Iface.class.getClassLoader(),
        new Class<?>[] {ThriftHiveMetastore.Iface.class},
        (proxy, method, args) -> {
          if ("getStatus".equals(method.getName())) {
            if (barrier != null) {
              try {
                barrier.await(5, TimeUnit.SECONDS);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            }
            return null;
          }
          throw new UnsupportedOperationException(method.getName());
        });
    try {
      Constructor<BackendInvocationSession> ctor = BackendInvocationSession.class.getDeclaredConstructor(
          org.apache.hadoop.hive.metastore.HiveMetaStoreClient.class,
          ThriftHiveMetastore.Iface.class,
          IsolatedMetastoreClient.class);
      ctor.setAccessible(true);
      return ctor.newInstance(null, thriftClient, null);
    } catch (Exception e) {
      MetaException me = new MetaException("session ctor failed");
      me.initCause(e);
      throw me;
    }
  }
}
