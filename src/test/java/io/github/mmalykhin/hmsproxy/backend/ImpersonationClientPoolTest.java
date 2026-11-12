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
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
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
  public void aWriteWhoseReplyWasLostIsNotReplayed() throws Exception {
    // A read timeout or reset after the request went out: the backend may already have created it.
    FailureRun run = runFailingCall(
        () -> new TTransportException(TTransportException.UNKNOWN, "java.net.SocketTimeoutException: Read timed out"),
        FailingCall.WRITE,
        new ImpersonationContext("alice", List.of("g1")));

    Assert.assertTrue("expected the transport failure to propagate", run.error() instanceof TTransportException);
    Assert.assertEquals("a write must reach the backend once", 1, run.calls());
    Assert.assertEquals("the dead session is still dropped", 1, run.impersonationSessionsOpened());
    Assert.assertTrue(
        "expected a transport_failure eviction:\n" + run.metrics(),
        run.metrics().contains("reason=\"transport_failure\""));
  }

  @Test
  public void aWriteThatNeverLeftIsRetriedOnce() throws Exception {
    FailureRun run = runFailingCall(
        () -> new TTransportException(TTransportException.NOT_OPEN, "Cannot write to null outputStream"),
        FailingCall.WRITE,
        new ImpersonationContext("alice", List.of("g1")));

    Assert.assertEquals("NOT_OPEN is raised before sending, so the write is retried", 2, run.calls());
    Assert.assertEquals(2, run.impersonationSessionsOpened());
  }

  @Test
  public void theSharedPoolReplaysReadsButNotWritesAfterALostReply() throws Exception {
    FailureRun read = runFailingCall(
        () -> new TTransportException(TTransportException.END_OF_FILE, "reset"), FailingCall.READ, null);
    Assert.assertEquals("a read is retried once on the shared pool", 2, read.calls());
    Assert.assertEquals("the shared pool opens one replacement session", 1, read.impersonationSessionsOpened());

    FailureRun write = runFailingCall(
        () -> new TTransportException(TTransportException.END_OF_FILE, "reset"), FailingCall.WRITE, null);
    Assert.assertTrue(write.error() instanceof TTransportException);
    Assert.assertEquals("a write is sent to the backend once on the shared pool", 1, write.calls());
    Assert.assertEquals("no replacement session is opened for a replay", 0, write.impersonationSessionsOpened());
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

  @Test
  public void impersonationSessionsForDistinctUsersOpenConcurrently() throws Exception {
    int parallelism = 2;
    CyclicBarrier openBarrier = new CyclicBarrier(parallelism);
    BackendRuntime.SessionFactory factory = new BackendRuntime.SessionFactory() {
      @Override
      public BackendInvocationSession open(
          ProxyConfig proxyConfig,
          CatalogConfig catalogConfig,
          HiveConf hiveConf,
          boolean backendKerberosEnabled,
          MetastoreRuntimeProfile runtimeProfile
      ) throws MetaException {
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
        // Fails unless both users can be inside session establishment at the same time, i.e. unless
        // the blocking connect/set_ugi runs outside the catalog monitor.
        try {
          openBarrier.await(5, TimeUnit.SECONDS);
        } catch (Exception e) {
          MetaException me = new MetaException(
              "impersonation session open for '" + userName + "' was serialized: " + e);
          me.initCause(e);
          throw me;
        }
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

    ExecutorService executor = Executors.newFixedThreadPool(parallelism);
    try {
      String[] users = {"alice", "bob"};
      CountDownLatch start = new CountDownLatch(1);
      Future<?>[] futures = new Future<?>[parallelism];
      for (int i = 0; i < parallelism; i++) {
        ImpersonationContext ctx = new ImpersonationContext(users[i], List.of("g1"));
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
  }

  @Test
  public void concurrentCapacityEvictionDoesNotSurfaceEvictedClientsToCallers() throws Exception {
    BackendRuntime.SessionFactory factory = new BackendRuntime.SessionFactory() {
      @Override
      public BackendInvocationSession open(
          ProxyConfig proxyConfig,
          CatalogConfig catalogConfig,
          HiveConf hiveConf,
          boolean backendKerberosEnabled,
          MetastoreRuntimeProfile runtimeProfile
      ) throws MetaException {
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
    // One cached client for two users: every alternating call evicts the other user's client while
    // concurrent callers already hold a reference to it.
    CatalogConfig catalogConfig = catalogConfig(2, 1);
    BackendRuntime runtime = BackendRuntime.open(
        config(catalogConfig),
        catalogConfig,
        new HiveConf(),
        false,
        MetastoreRuntimeProfile.APACHE_3_1_3,
        factory,
        metrics);
    CatalogBackend backend = newBackend(catalogConfig, runtime, metrics);

    int parallelism = 4;
    ExecutorService executor = Executors.newFixedThreadPool(parallelism);
    try {
      CountDownLatch start = new CountDownLatch(1);
      Future<?>[] futures = new Future<?>[parallelism];
      for (int i = 0; i < parallelism; i++) {
        ImpersonationContext ctx = new ImpersonationContext(i % 2 == 0 ? "alice" : "bob", List.of("g1"));
        futures[i] = executor.submit(() -> {
          start.await();
          try {
            for (int call = 0; call < 50; call++) {
              backend.invokeRawByName("getStatus", new Class<?>[0], new Object[0], ctx);
            }
          } catch (Throwable t) {
            throw new RuntimeException(t);
          }
          return null;
        });
      }
      start.countDown();
      for (Future<?> f : futures) {
        f.get(30, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
      backend.close();
    }
  }

  @Test
  public void aReadSurvivesSeveralStaleIdleSessionsOfTheImpersonationPool() throws Exception {
    try (StaleBackend stand = StaleBackend.open(3, CatalogConfig.DEFAULT_SESSION_VALIDATE_AFTER_IDLE_MS)) {
      stand.warm(3, StaleBackend.ALICE);
      stand.killOpenSessions();

      stand.read(StaleBackend.ALICE);

      Assert.assertEquals("the retry must run on a fresh session, not on the next dead one", 1, stand.liveCalls("get_all_databases") - 3);
      Assert.assertEquals("every dead idle session is closed, none is left for the next caller", 3, stand.closedSessions());
      Assert.assertTrue(
          "expected stale_idle evictions:\n" + stand.metrics(),
          stand.metrics().contains("reason=\"stale_idle\""));
    }
  }

  @Test
  public void aReadSurvivesSeveralStaleIdleSessionsOfTheSharedPool() throws Exception {
    try (StaleBackend stand = StaleBackend.open(3, CatalogConfig.DEFAULT_SESSION_VALIDATE_AFTER_IDLE_MS)) {
      stand.warm(3, null);
      stand.killOpenSessions();

      stand.read(null);

      Assert.assertEquals(3, stand.closedSessions());
    }
  }

  @Test
  public void aReadTimeoutLeavesTheOtherIdleSessionsOpen() throws Exception {
    // The backend was slow, not gone: closing healthy idle sessions would only add reconnects.
    try (StaleBackend stand = StaleBackend.open(3, CatalogConfig.DEFAULT_SESSION_VALIDATE_AFTER_IDLE_MS)) {
      stand.warm(3, StaleBackend.ALICE);
      stand.failNextCallWith(new TTransportException(
          TTransportException.UNKNOWN, new java.net.SocketTimeoutException("Read timed out")));

      stand.read(StaleBackend.ALICE);

      Assert.assertEquals("only the session that timed out is dropped", 1, stand.closedSessions());
    }
  }

  @Test
  public void aWriteOnASessionIdlePastTheThresholdIsProbedAndMovedToAFreshSession() throws Exception {
    try (StaleBackend stand = StaleBackend.open(3, 1L)) {
      stand.warm(3, StaleBackend.ALICE);
      Thread.sleep(30L);
      stand.killOpenSessions();

      stand.write(StaleBackend.ALICE);

      Assert.assertEquals("the write must reach the backend exactly once", 1, stand.liveCalls("create_database"));
      Assert.assertEquals("the write must never be sent on a dead session", 0, stand.deadCalls("create_database"));
      Assert.assertEquals("one probe finds the dead session", 1, stand.deadCalls("getStatus"));
      Assert.assertEquals("the remaining dead idle sessions are closed too", 3, stand.closedSessions());
      Assert.assertTrue(
          "expected a stale probe outcome:\n" + stand.metrics(),
          stand.metrics().contains("hms_proxy_backend_session_validations_total")
              && stand.metrics().contains("outcome=\"stale\""));
    }
  }

  @Test
  public void aWriteOnTheSharedPoolIsProbedToo() throws Exception {
    try (StaleBackend stand = StaleBackend.open(1, 1L)) {
      stand.warm(1, null);
      Thread.sleep(30L);
      stand.killOpenSessions();

      stand.write(null);

      Assert.assertEquals(1, stand.liveCalls("create_database"));
      Assert.assertEquals(0, stand.deadCalls("create_database"));
    }
  }

  @Test
  public void aLiveIdleSessionIsProbedOnceAndReused() throws Exception {
    try (StaleBackend stand = StaleBackend.open(1, 1L)) {
      stand.warm(1, StaleBackend.ALICE);
      Thread.sleep(30L);
      int openedBefore = stand.openedSessions();

      stand.write(StaleBackend.ALICE);

      Assert.assertEquals(1, stand.liveCalls("getStatus"));
      Assert.assertEquals(1, stand.liveCalls("create_database"));
      Assert.assertEquals("a live session is kept", openedBefore, stand.openedSessions());
      Assert.assertEquals(0, stand.closedSessions());
    }
  }

  @Test
  public void aRecentlyUsedSessionIsNotProbedAndALostWriteIsStillNotReplayed() throws Exception {
    try (StaleBackend stand = StaleBackend.open(1, 60_000L)) {
      stand.warm(1, StaleBackend.ALICE);
      stand.killOpenSessions();

      try {
        stand.write(StaleBackend.ALICE);
        Assert.fail("expected the transport failure to propagate");
      } catch (TTransportException expected) {
        // The reply was lost on a session inside the threshold: nothing proves the write did not land.
      }

      Assert.assertEquals("no probe inside the threshold", 0, stand.deadCalls("getStatus"));
      Assert.assertEquals("the write is sent once", 1, stand.deadCalls("create_database"));
      Assert.assertEquals(0, stand.liveCalls("create_database"));
      Assert.assertTrue(
          "expected the lost write to be counted:\n" + stand.metrics(),
          stand.metrics().contains("hms_proxy_backend_calls_not_replayed_total")
              && stand.metrics().contains("method=\"create_database\""));
    }
  }

  @Test
  public void zeroThresholdTurnsTheProbeOff() throws Exception {
    try (StaleBackend stand = StaleBackend.open(1, 0L)) {
      stand.warm(1, StaleBackend.ALICE);
      Thread.sleep(30L);
      stand.killOpenSessions();

      try {
        stand.write(StaleBackend.ALICE);
        Assert.fail("expected the transport failure to propagate");
      } catch (TTransportException expected) {
        // Probe is off, so this is the pre-existing behaviour.
      }

      Assert.assertEquals(0, stand.deadCalls("getStatus"));
    }
  }

  @Test
  public void aReadIsNeverProbed() throws Exception {
    try (StaleBackend stand = StaleBackend.open(1, 1L)) {
      stand.warm(1, StaleBackend.ALICE);
      Thread.sleep(30L);

      stand.read(StaleBackend.ALICE);

      Assert.assertEquals("a read can be replayed, so it pays no probe", 0, stand.liveCalls("getStatus"));
    }
  }

  /** A fake backend whose open connections can be killed, the way a metastore drops them. */
  private static final class StaleBackend implements AutoCloseable {
    static final ImpersonationContext ALICE = new ImpersonationContext("alice", List.of("g1"));

    private final List<java.util.concurrent.atomic.AtomicBoolean> sessions =
        new java.util.concurrent.CopyOnWriteArrayList<>();
    private final Map<String, AtomicInteger> liveCalls = new java.util.concurrent.ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> deadCalls = new java.util.concurrent.ConcurrentHashMap<>();
    private final AtomicInteger closed = new AtomicInteger();
    private final java.util.concurrent.atomic.AtomicReference<TException> nextFailure =
        new java.util.concurrent.atomic.AtomicReference<>();
    private final PrometheusMetrics metrics = new PrometheusMetrics();
    private volatile CyclicBarrier warmBarrier;
    private CatalogBackend backend;

    static StaleBackend open(int poolSize, long validateAfterIdleMs) throws Exception {
      StaleBackend stand = new StaleBackend();
      BackendRuntime.SessionFactory factory = new BackendRuntime.SessionFactory() {
        @Override
        public BackendInvocationSession open(
            ProxyConfig proxyConfig,
            CatalogConfig catalogConfig,
            HiveConf hiveConf,
            boolean backendKerberosEnabled,
            MetastoreRuntimeProfile runtimeProfile
        ) throws MetaException {
          return stand.newSession();
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
          return stand.newSession();
        }
      };
      CatalogConfig catalogConfig = new CatalogConfig(
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
          poolSize,
          poolSize,
          0L).withSessionValidateAfterIdleMs(validateAfterIdleMs);
      BackendRuntime runtime = BackendRuntime.open(
          config(catalogConfig),
          catalogConfig,
          new HiveConf(),
          false,
          MetastoreRuntimeProfile.APACHE_3_1_3,
          factory,
          stand.metrics);
      stand.backend = newBackend(catalogConfig, runtime, stand.metrics);
      return stand;
    }

    private BackendInvocationSession newSession() throws MetaException {
      java.util.concurrent.atomic.AtomicBoolean dead = new java.util.concurrent.atomic.AtomicBoolean();
      sessions.add(dead);
      ThriftHiveMetastore.Iface thriftClient = (ThriftHiveMetastore.Iface) Proxy.newProxyInstance(
          ThriftHiveMetastore.Iface.class.getClassLoader(),
          new Class<?>[] {ThriftHiveMetastore.Iface.class},
          (proxy, method, args) -> {
            String name = method.getName();
            if ("set_ugi".equals(name)) {
              return List.of("g1");
            }
            if (dead.get()) {
              deadCalls.computeIfAbsent(name, k -> new AtomicInteger()).incrementAndGet();
              throw new TTransportException(TTransportException.END_OF_FILE);
            }
            TException failure = nextFailure.getAndSet(null);
            if (failure != null) {
              throw failure;
            }
            liveCalls.computeIfAbsent(name, k -> new AtomicInteger()).incrementAndGet();
            CyclicBarrier barrier = warmBarrier;
            if (barrier != null && "get_all_databases".equals(name)) {
              barrier.await(5, TimeUnit.SECONDS);
            }
            return "get_all_databases".equals(name) ? List.of() : null;
          });
      try {
        Constructor<BackendInvocationSession> ctor = BackendInvocationSession.class.getDeclaredConstructor(
            org.apache.hadoop.hive.metastore.HiveMetaStoreClient.class,
            ThriftHiveMetastore.Iface.class,
            IsolatedMetastoreClient.class);
        ctor.setAccessible(true);
        BackendInvocationSession session = ctor.newInstance(null, thriftClient, null);
        session.runAfterClose(closed::incrementAndGet);
        return session;
      } catch (Exception e) {
        MetaException me = new MetaException("session ctor failed");
        me.initCause(e);
        throw me;
      }
    }

    /** Runs {@code parallelism} overlapping reads, so the pool ends up holding that many idle sessions. */
    void warm(int parallelism, ImpersonationContext who) throws Exception {
      warmBarrier = parallelism > 1 ? new CyclicBarrier(parallelism) : null;
      ExecutorService executor = Executors.newFixedThreadPool(parallelism);
      try {
        List<Future<?>> futures = new java.util.ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
          futures.add(executor.submit(() -> {
            read(who);
            return null;
          }));
        }
        for (Future<?> f : futures) {
          f.get(10, TimeUnit.SECONDS);
        }
      } finally {
        warmBarrier = null;
        executor.shutdownNow();
      }
    }

    void killOpenSessions() {
      sessions.forEach(dead -> dead.set(true));
    }

    void failNextCallWith(TException failure) {
      nextFailure.set(failure);
    }

    void read(ImpersonationContext who) throws Exception {
      call("get_all_databases", new Class<?>[0], new Object[0], who);
    }

    void write(ImpersonationContext who) throws Exception {
      call("create_database", new Class<?>[] {Database.class}, new Object[] {new Database()}, who);
    }

    private void call(String method, Class<?>[] types, Object[] args, ImpersonationContext who) throws Exception {
      try {
        backend.invokeRawByName(method, types, args, who);
      } catch (Exception | Error e) {
        throw e;
      } catch (Throwable t) {
        throw new IllegalStateException(t);
      }
    }

    int liveCalls(String method) {
      AtomicInteger n = liveCalls.get(method);
      return n == null ? 0 : n.get();
    }

    int deadCalls(String method) {
      AtomicInteger n = deadCalls.get(method);
      return n == null ? 0 : n.get();
    }

    int closedSessions() {
      return closed.get();
    }

    int openedSessions() {
      return sessions.size();
    }

    String metrics() {
      return metrics.render();
    }

    @Override
    public void close() {
      backend.close();
    }
  }

  private static FailureRun runFailingCall(Supplier<TException> failure) throws Exception {
    return runFailingCall(failure, FailingCall.READ, new ImpersonationContext("alice", List.of("g1")));
  }

  /** A read and a write the fake backend fails, so the replay decision can be told apart. */
  private enum FailingCall {
    READ("getStatus", new Class<?>[0], new Object[0]),
    WRITE("create_database", new Class<?>[] {Database.class}, new Object[] {new Database()});

    private final String method;
    private final Class<?>[] parameterTypes;
    private final Object[] args;

    FailingCall(String method, Class<?>[] parameterTypes, Object[] args) {
      this.method = method;
      this.parameterTypes = parameterTypes;
      this.args = args;
    }
  }

  private static FailureRun runFailingCall(
      Supplier<TException> failure,
      FailingCall call,
      ImpersonationContext impersonation
  ) throws Exception {
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
      backend.invokeRawByName(call.method, call.parameterTypes, call.args, impersonation);
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
          if ("getStatus".equals(method.getName()) || "create_database".equals(method.getName())) {
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

  private static CatalogConfig catalogConfig(int impersonationPoolMaxSize, int maxImpersonationClients) {
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
        maxImpersonationClients,
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

  @Test
  public void theSweepClosesIdleSessionsOfAUserWhoNeverComesBack() throws Exception {
    IdleRun run = idleRun(0L, 50L);
    try {
      run.call("alice");
      Thread.sleep(150L);
      run.backend.sweepIdleImpersonation();

      Assert.assertEquals("the idle session is closed without another call from alice", 1, run.closes.get());
      Assert.assertTrue(
          "expected an idle eviction:\n" + run.metrics.render(),
          run.metrics.render().contains("reason=\"idle\""));
    } finally {
      run.backend.close();
    }
  }

  @Test
  public void theSweepEvictsAUserIdlePastTheClientTtl() throws Exception {
    IdleRun run = idleRun(50L, 0L);
    try {
      run.call("alice");
      int opened = run.sessionsOpened.get();
      Thread.sleep(150L);
      run.backend.sweepIdleImpersonation();

      Assert.assertEquals("the evicted user's pooled session is closed", 1, run.closes.get());
      run.call("alice");
      Assert.assertEquals("a returning user gets a fresh client", opened + 1, run.sessionsOpened.get());
    } finally {
      run.backend.close();
    }
  }

  @Test
  public void anActiveUserKeepsItsPooledSession() throws Exception {
    IdleRun run = idleRun(60_000L, 60_000L);
    try {
      run.call("alice");
      run.backend.sweepIdleImpersonation();
      Assert.assertEquals(0, run.closes.get());
      run.call("alice");
      Assert.assertEquals("the pooled session is reused", 1, run.sessionsOpened.get());
    } finally {
      run.backend.close();
    }
  }

  @Test
  public void theBackgroundSweeperRunsWithoutAnyTraffic() throws Exception {
    IdleRun run = idleRun(0L, 150L);
    try {
      run.call("alice");
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (run.closes.get() == 0 && System.nanoTime() < deadline) {
        Thread.sleep(25L);
      }
      Assert.assertEquals("the scheduled sweep must close the idle session on its own", 1, run.closes.get());
    } finally {
      run.backend.close();
    }
  }

  @Test
  public void theSweepPeriodIsHalfTheShortestTtlAndOffWithoutOne() {
    Assert.assertEquals(300_000L, CatalogBackend.idleSweepPeriodMs(ttlCatalogConfig(1_800_000L, 600_000L)));
    Assert.assertEquals(10_000L, CatalogBackend.idleSweepPeriodMs(ttlCatalogConfig(20_000L, 0L)));
    Assert.assertEquals("never tighter than the floor", 100L, CatalogBackend.idleSweepPeriodMs(ttlCatalogConfig(0L, 10L)));
    Assert.assertEquals("both TTLs off: no sweeper", 0L, CatalogBackend.idleSweepPeriodMs(ttlCatalogConfig(0L, 0L)));
  }

  private record IdleRun(
      CatalogBackend backend,
      PrometheusMetrics metrics,
      AtomicInteger sessionsOpened,
      AtomicInteger closes
  ) {
    void call(String user) throws Exception {
      try {
        backend.invokeRawByName("getStatus", new Class<?>[0], new Object[0], new ImpersonationContext(user, List.of("g1")));
      } catch (Throwable t) {
        throw new AssertionError(t);
      }
    }
  }

  /** A backend whose impersonation sessions report their close through {@code closes}. */
  private static IdleRun idleRun(long clientIdleTtlMs, long sessionIdleTtlMs) throws Exception {
    AtomicInteger sessionsOpened = new AtomicInteger();
    AtomicInteger closes = new AtomicInteger();
    BackendRuntime.SessionFactory factory = new BackendRuntime.SessionFactory() {
      @Override
      public BackendInvocationSession open(
          ProxyConfig proxyConfig,
          CatalogConfig catalogConfig,
          HiveConf hiveConf,
          boolean backendKerberosEnabled,
          MetastoreRuntimeProfile runtimeProfile
      ) throws MetaException {
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
        sessionsOpened.incrementAndGet();
        BackendInvocationSession session = makeSession(null);
        session.runAfterClose(closes::incrementAndGet);
        return session;
      }
    };
    PrometheusMetrics metrics = new PrometheusMetrics();
    CatalogConfig catalogConfig = ttlCatalogConfig(clientIdleTtlMs, sessionIdleTtlMs);
    BackendRuntime runtime = BackendRuntime.open(
        config(catalogConfig), catalogConfig, new HiveConf(), false, MetastoreRuntimeProfile.APACHE_3_1_3, factory, metrics);
    return new IdleRun(newBackend(catalogConfig, runtime, metrics), metrics, sessionsOpened, closes);
  }

  private static CatalogConfig ttlCatalogConfig(long clientIdleTtlMs, long sessionIdleTtlMs) {
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
        clientIdleTtlMs,
        1,
        4,
        sessionIdleTtlMs);
  }
}
