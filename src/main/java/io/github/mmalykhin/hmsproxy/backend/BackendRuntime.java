package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.config.server.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;

public final class BackendRuntime implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(BackendRuntime.class);
  private static final SessionFactory DEFAULT_SESSION_FACTORY = new DefaultSessionFactory();
  private static final long DEFAULT_BORROW_TIMEOUT_MS = 30_000L;

  private final ProxyConfig proxyConfig;
  private final CatalogConfig catalogConfig;
  private final HiveConf hiveConf;
  private final boolean backendKerberosEnabled;
  private final SessionFactory sessionFactory;
  private final int poolSize;
  private final Semaphore permits;
  private final LinkedBlockingQueue<BackendInvocationSession> pool;
  private final PrometheusMetrics metrics;
  private volatile MetastoreRuntimeProfile activeProfile;

  private BackendRuntime(
      ProxyConfig proxyConfig,
      CatalogConfig catalogConfig,
      HiveConf hiveConf,
      boolean backendKerberosEnabled,
      SessionFactory sessionFactory,
      MetastoreRuntimeProfile activeProfile,
      BackendInvocationSession initialSession,
      PrometheusMetrics metrics
  ) {
    this.proxyConfig = proxyConfig;
    this.catalogConfig = catalogConfig;
    this.hiveConf = hiveConf;
    this.backendKerberosEnabled = backendKerberosEnabled;
    this.sessionFactory = sessionFactory;
    this.poolSize = catalogConfig.sharedSessionPoolSize();
    this.permits = new Semaphore(poolSize);
    this.pool = new LinkedBlockingQueue<>(poolSize);
    this.metrics = metrics;
    this.activeProfile = activeProfile;
    this.pool.offer(initialSession);
  }

  public static BackendRuntime open(
      ProxyConfig proxyConfig,
      CatalogConfig catalogConfig,
      HiveConf hiveConf,
      boolean backendKerberosEnabled,
      MetastoreRuntimeProfile runtimeProfile
  ) throws MetaException {
    return open(proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, runtimeProfile,
        DEFAULT_SESSION_FACTORY, null);
  }

  public static BackendRuntime open(
      ProxyConfig proxyConfig,
      CatalogConfig catalogConfig,
      HiveConf hiveConf,
      boolean backendKerberosEnabled,
      MetastoreRuntimeProfile runtimeProfile,
      PrometheusMetrics metrics
  ) throws MetaException {
    return open(proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, runtimeProfile,
        DEFAULT_SESSION_FACTORY, metrics);
  }

  public static BackendRuntime open(
      ProxyConfig proxyConfig,
      CatalogConfig catalogConfig,
      HiveConf hiveConf,
      boolean backendKerberosEnabled,
      MetastoreRuntimeProfile runtimeProfile,
      SessionFactory sessionFactory
  ) throws MetaException {
    return open(proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, runtimeProfile,
        sessionFactory, null);
  }

  public static BackendRuntime open(
      ProxyConfig proxyConfig,
      CatalogConfig catalogConfig,
      HiveConf hiveConf,
      boolean backendKerberosEnabled,
      MetastoreRuntimeProfile runtimeProfile,
      SessionFactory sessionFactory,
      PrometheusMetrics metrics
  ) throws MetaException {
    BackendInvocationSession initial = sessionFactory.open(
        proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, runtimeProfile);
    return new BackendRuntime(
        proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, sessionFactory, runtimeProfile, initial, metrics);
  }

  public Object invokeShared(Method method, Object[] args) throws Throwable {
    return invokeOnPool(new SessionCall() {
      @Override
      public Object apply(BackendInvocationSession session) throws Throwable {
        return session.invoke(method, args);
      }
    }, method.getName());
  }

  public Object invokeSharedByName(String methodName, Class<?>[] parameterTypes, Object[] args) throws Throwable {
    return invokeOnPool(new SessionCall() {
      @Override
      public Object apply(BackendInvocationSession session) throws Throwable {
        return session.invokeByName(methodName, parameterTypes, args);
      }
    }, methodName);
  }

  public String reconnectShared(BackendAdapter adapter) throws MetaException {
    long timeoutMs = catalogConfig.latencyBudgetMs() > 0L
        ? catalogConfig.latencyBudgetMs()
        : DEFAULT_BORROW_TIMEOUT_MS;
    boolean acquired;
    try {
      acquired = permits.tryAcquire(poolSize, timeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      MetaException me = new MetaException(
          "Interrupted while quiescing backend metastore pool for catalog " + catalogConfig.name());
      me.initCause(e);
      throw me;
    }
    if (!acquired) {
      LOG.warn("Backend catalog '{}' reconnect timed out waiting to drain {} permits within {} ms",
          catalogConfig.name(), poolSize, timeoutMs);
      if (metrics != null) {
        metrics.recordBackendSessionAcquireTimeout(catalogConfig.name(), "reconnect");
      }
      throw new MetaException(
          "Timed out waiting to quiesce backend metastore pool for catalog " + catalogConfig.name()
              + " after " + timeoutMs + " ms");
    }
    try {
      activeProfile = adapter.runtimeProfile();
      drainAndCloseLocked();
      BackendInvocationSession fresh = sessionFactory.open(
          proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, activeProfile);
      pool.offer(fresh);
    } finally {
      permits.release(poolSize);
    }
    return adapter.backendVersion();
  }

  public BackendInvocationSession openImpersonationSession(
      MetastoreRuntimeProfile runtimeProfile,
      String userName,
      List<String> groupNames
  ) throws MetaException {
    return sessionFactory.openImpersonating(
        proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, runtimeProfile, userName, groupNames);
  }

  public BackendInvocationSession openEphemeralSession(HiveConf conf, MetastoreRuntimeProfile runtimeProfile)
      throws MetaException {
    return sessionFactory.open(proxyConfig, catalogConfig, conf, backendKerberosEnabled, runtimeProfile);
  }

  @Override
  public void close() {
    long timeoutMs = catalogConfig.latencyBudgetMs() > 0L
        ? catalogConfig.latencyBudgetMs()
        : DEFAULT_BORROW_TIMEOUT_MS;
    boolean acquired;
    try {
      acquired = permits.tryAcquire(poolSize, timeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("Backend catalog '{}' close interrupted while quiescing pool; draining without full permit acquisition",
          catalogConfig.name());
      drainAndCloseLocked();
      return;
    }
    try {
      if (!acquired) {
        LOG.warn("Backend catalog '{}' close timed out waiting to drain {} permits within {} ms; draining anyway",
            catalogConfig.name(), poolSize, timeoutMs);
      }
      drainAndCloseLocked();
    } finally {
      if (acquired) {
        permits.release(poolSize);
      }
    }
  }

  private Object invokeOnPool(SessionCall call, String label) throws Throwable {
    BackendInvocationSession session = borrow();
    try {
      Object result = call.apply(session);
      release(session);
      session = null;
      return result;
    } catch (Throwable cause) {
      if (!isTransportFailure(cause)) {
        release(session);
        session = null;
        throw cause;
      }
      LOG.warn("Backend catalog '{}' transport failed in method {}, discarding session and retrying once",
          catalogConfig.name(), label, cause);
      discard(session);
      session = null;
      BackendInvocationSession retry = borrow();
      try {
        Object result = call.apply(retry);
        release(retry);
        retry = null;
        return result;
      } catch (Throwable retryCause) {
        if (isTransportFailure(retryCause)) {
          discard(retry);
        } else {
          release(retry);
        }
        throw retryCause;
      }
    } finally {
      if (session != null) {
        release(session);
      }
    }
  }

  private BackendInvocationSession borrow() throws MetaException {
    long timeoutMs = catalogConfig.latencyBudgetMs() > 0L
        ? catalogConfig.latencyBudgetMs()
        : DEFAULT_BORROW_TIMEOUT_MS;
    boolean acquired;
    try {
      acquired = permits.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      MetaException me = new MetaException(
          "Interrupted while waiting for backend metastore session for catalog " + catalogConfig.name());
      me.initCause(e);
      throw me;
    }
    if (!acquired) {
      LOG.warn("Backend catalog '{}' session pool exhausted: no permit available within {} ms (poolSize={})",
          catalogConfig.name(), timeoutMs, poolSize);
      if (metrics != null) {
        metrics.recordBackendSessionAcquireTimeout(catalogConfig.name(), "borrow");
      }
      throw new MetaException(
          "Timed out waiting for backend metastore session for catalog " + catalogConfig.name()
              + " after " + timeoutMs + " ms");
    }
    BackendInvocationSession s = pool.poll();
    if (s != null) {
      return s;
    }
    try {
      return sessionFactory.open(proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, activeProfile);
    } catch (Throwable t) {
      permits.release();
      if (t instanceof MetaException me) {
        throw me;
      }
      MetaException me = new MetaException(
          "Unable to open backend metastore session for catalog " + catalogConfig.name());
      me.initCause(t);
      throw me;
    }
  }

  private void release(BackendInvocationSession session) {
    pool.offer(session);
    permits.release();
  }

  private void discard(BackendInvocationSession session) {
    CatalogBackend.closeQuietly(session, "shared backend metastore session (transport failure)");
    permits.release();
  }

  private void drainAndCloseLocked() {
    List<BackendInvocationSession> drained = new ArrayList<>(poolSize);
    pool.drainTo(drained);
    for (BackendInvocationSession s : drained) {
      CatalogBackend.closeQuietly(s, "shared backend metastore session");
    }
  }

  private static boolean isTransportFailure(Throwable cause) {
    return cause instanceof TTransportException;
  }

  @FunctionalInterface
  private interface SessionCall {
    Object apply(BackendInvocationSession session) throws Throwable;
  }

  public interface SessionFactory {
    BackendInvocationSession open(
        ProxyConfig proxyConfig,
        CatalogConfig catalogConfig,
        HiveConf hiveConf,
        boolean backendKerberosEnabled,
        MetastoreRuntimeProfile runtimeProfile
    ) throws MetaException;

    BackendInvocationSession openImpersonating(
        ProxyConfig proxyConfig,
        CatalogConfig catalogConfig,
        HiveConf hiveConf,
        boolean backendKerberosEnabled,
        MetastoreRuntimeProfile runtimeProfile,
        String userName,
        List<String> groupNames
    ) throws MetaException;
  }

  private static final class DefaultSessionFactory implements SessionFactory {
    @Override
    public BackendInvocationSession open(
        ProxyConfig proxyConfig,
        CatalogConfig catalogConfig,
        HiveConf hiveConf,
        boolean backendKerberosEnabled,
        MetastoreRuntimeProfile runtimeProfile
    ) throws MetaException {
      return BackendInvocationSession.open(
          proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, runtimeProfile);
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
      return BackendInvocationSession.openImpersonating(
          proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, runtimeProfile, userName, groupNames);
    }
  }
}
