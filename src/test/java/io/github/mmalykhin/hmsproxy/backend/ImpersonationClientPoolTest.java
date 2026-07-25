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
import java.util.function.Supplier;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Test;

public class ImpersonationClientPoolTest {

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
  public void applicationErrorsKeepTheImpersonationSessionAndDoNotRetry() throws Exception {
    FailureRun run = runFailingCall(
        () -> new TApplicationException(TApplicationException.INTERNAL_ERROR, "backend blew up"));

    Assert.assertTrue("expected the application error to propagate", run.error() instanceof TApplicationException);
    Assert.assertEquals(TApplicationException.INTERNAL_ERROR, ((TApplicationException) run.error()).getType());
    Assert.assertEquals("call must not be replayed on a second connection", 1, run.calls());
    Assert.assertEquals("live connection must not be dropped and reopened", 1, run.impersonationSessionsOpened());
  }

  @Test
  public void transportFailuresStillDiscardTheSessionAndRetryOnce() throws Exception {
    FailureRun run = runFailingCall(() -> new TTransportException("connection reset"));

    Assert.assertTrue("expected the transport failure to propagate", run.error() instanceof TTransportException);
    Assert.assertEquals("transport failure retries once", 2, run.calls());
    Assert.assertEquals("discarded session is replaced by a fresh one", 2, run.impersonationSessionsOpened());
  }

  @Test
  public void protocolDesyncDiscardsTheSessionWithoutReplayingTheCall() throws Exception {
    FailureRun run = runFailingCall(
        () -> new TApplicationException(TApplicationException.BAD_SEQUENCE_ID, "reply for another call"));

    Assert.assertTrue("expected the desync error to propagate", run.error() instanceof TApplicationException);
    Assert.assertEquals("call must not be replayed", 1, run.calls());
    Assert.assertEquals("poisoned connection is dropped", 1, run.impersonationSessionsOpened());
    Assert.assertTrue(
        "expected a protocol_desync eviction:\n" + run.metrics(),
        run.metrics().contains("reason=\"protocol_desync\""));
  }

  private static FailureRun runFailingCall(Supplier<TException> failure) throws Exception {
    AtomicInteger sessionsOpened = new AtomicInteger();
    AtomicInteger calls = new AtomicInteger();
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
        return makeFailingSession(calls, failure);
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
    int baseline = sessionsOpened.get();

    Throwable error = null;
    try {
      backend.invokeRawByName(
          "getStatus", new Class<?>[0], new Object[0], new ImpersonationContext("alice", List.of("g1")));
      Assert.fail("Expected the backend failure to propagate");
    } catch (Throwable t) {
      error = t;
    } finally {
      backend.close();
    }
    return new FailureRun(error, calls.get(), sessionsOpened.get() - baseline, metrics.render());
  }

  private static BackendInvocationSession makeFailingSession(AtomicInteger calls, Supplier<TException> failure)
      throws MetaException {
    ThriftHiveMetastore.Iface thriftClient = (ThriftHiveMetastore.Iface) Proxy.newProxyInstance(
        ThriftHiveMetastore.Iface.class.getClassLoader(),
        new Class<?>[] {ThriftHiveMetastore.Iface.class},
        (proxy, method, args) -> {
          if ("getStatus".equals(method.getName())) {
            calls.incrementAndGet();
            throw failure.get();
          }
          if ("set_ugi".equals(method.getName())) {
            return List.of("g1");
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

  private record FailureRun(Throwable error, int calls, int impersonationSessionsOpened, String metrics) {
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
    return ctor.newInstance(config(catalogConfig), catalogConfig, new HiveConf(), adapter, runtime, catalog, metrics);
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
