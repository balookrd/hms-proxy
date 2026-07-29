package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.config.server.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftFailureClassifier;
import java.io.IOException;
import java.net.MalformedURLException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
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
  private final MetastoreApiClassLoader isolatedClassLoader;
  private final int poolSize;
  private final Semaphore permits;
  private final LinkedBlockingQueue<BackendInvocationSession> pool;
  private final PrometheusMetrics metrics;
  private final Object lifecycleLock = new Object();
  private volatile MetastoreRuntimeProfile activeProfile;
  private volatile boolean closed;
  private int leasedSessions;
  private boolean classLoaderClosed;

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
    this(
        proxyConfig,
        catalogConfig,
        hiveConf,
        backendKerberosEnabled,
        sessionFactory,
        null,
        activeProfile,
        initialSession,
        metrics);
  }

  private BackendRuntime(
      ProxyConfig proxyConfig,
      CatalogConfig catalogConfig,
      HiveConf hiveConf,
      boolean backendKerberosEnabled,
      SessionFactory sessionFactory,
      MetastoreApiClassLoader isolatedClassLoader,
      MetastoreRuntimeProfile activeProfile,
      BackendInvocationSession initialSession,
      PrometheusMetrics metrics
  ) {
    this.proxyConfig = proxyConfig;
    this.catalogConfig = catalogConfig;
    this.hiveConf = hiveConf;
    this.backendKerberosEnabled = backendKerberosEnabled;
    this.sessionFactory = sessionFactory;
    this.isolatedClassLoader = isolatedClassLoader;
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
    MetastoreApiClassLoader isolatedClassLoader = sessionFactory.requiresIsolatedClassLoader(runtimeProfile)
        ? openIsolatedClassLoader(proxyConfig, catalogConfig, runtimeProfile)
        : null;
    BackendInvocationSession initial = null;
    try {
      initial = sessionFactory.open(
          proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, runtimeProfile, isolatedClassLoader);
    } catch (Throwable t) {
      closeClassLoaderQuietly(isolatedClassLoader, catalogConfig.name());
      if (t instanceof MetaException me) {
        throw me;
      }
      MetaException me = new MetaException(
          "Unable to open backend metastore session for catalog " + catalogConfig.name());
      me.initCause(t);
      throw me;
    }
    return new BackendRuntime(
        proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, sessionFactory, isolatedClassLoader,
        runtimeProfile, initial, metrics);
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
    ensureOpen();
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
      ensureOpen();
      activeProfile = adapter.runtimeProfile();
      drainAndCloseLocked();
      BackendInvocationSession fresh = sessionFactory.open(
          proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, activeProfile, isolatedClassLoader);
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
    ensureOpen();
    return sessionFactory.openImpersonating(
        proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, runtimeProfile, userName, groupNames,
        isolatedClassLoader);
  }

  public BackendInvocationSession openEphemeralSession(HiveConf conf, MetastoreRuntimeProfile runtimeProfile)
      throws MetaException {
    ensureOpen();
    return sessionFactory.open(
        proxyConfig, catalogConfig, conf, backendKerberosEnabled, runtimeProfile, isolatedClassLoader);
  }

  @Override
  public void close() {
    synchronized (lifecycleLock) {
      if (closed) {
        return;
      }
      closed = true;
    }
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
      closeClassLoaderIfQuiesced();
      return;
    }
    try {
      if (!acquired) {
        LOG.warn("Backend catalog '{}' close timed out waiting to drain {} permits within {} ms; draining pooled"
                + " sessions now, in-flight sessions are closed on return",
            catalogConfig.name(), poolSize, timeoutMs);
      }
      drainAndCloseLocked();
      // Deferred while requests are still in flight: they may still need classes from this classloader.
      closeClassLoaderIfQuiesced();
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
      if (!ThriftFailureClassifier.isTransportFailure(cause)) {
        // A protocol desync poisons the connection, but the call must not be replayed.
        if (ThriftFailureClassifier.isProtocolDesync(cause)) {
          discard(session);
        } else {
          release(session);
        }
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
        if (ThriftFailureClassifier.isTransportFailure(retryCause)
            || ThriftFailureClassifier.isProtocolDesync(retryCause)) {
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
    ensureOpen();
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
    BackendInvocationSession pooled = pool.poll();
    if (pooled != null) {
      return leaseOrReject(pooled);
    }
    if (closed) {
      permits.release();
      throw closedException();
    }
    BackendInvocationSession fresh;
    try {
      fresh = sessionFactory.open(
          proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, activeProfile, isolatedClassLoader);
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
    return leaseOrReject(fresh);
  }

  /** Registers an in-flight session, or closes it when the runtime was closed concurrently. */
  private BackendInvocationSession leaseOrReject(BackendInvocationSession session) throws MetaException {
    synchronized (lifecycleLock) {
      if (!closed) {
        leasedSessions++;
        return session;
      }
    }
    CatalogBackend.closeQuietly(session, "shared backend metastore session (runtime closed)");
    permits.release();
    closeClassLoaderIfQuiesced();
    throw closedException();
  }

  private void release(BackendInvocationSession session) {
    boolean drop;
    synchronized (lifecycleLock) {
      leasedSessions--;
      drop = closed;
    }
    if (drop) {
      CatalogBackend.closeQuietly(session, "shared backend metastore session (runtime closed)");
    } else {
      pool.offer(session);
    }
    permits.release();
    if (drop) {
      closeClassLoaderIfQuiesced();
    }
  }

  private void discard(BackendInvocationSession session) {
    synchronized (lifecycleLock) {
      leasedSessions--;
    }
    CatalogBackend.closeQuietly(session, "shared backend metastore session (transport failure)");
    permits.release();
    closeClassLoaderIfQuiesced();
  }

  private void ensureOpen() throws MetaException {
    if (closed) {
      throw closedException();
    }
  }

  private MetaException closedException() {
    return new MetaException(
        "Backend metastore runtime for catalog " + catalogConfig.name() + " is closed");
  }

  /** Closes the isolated classloader once the runtime is closed and no session is in flight. */
  private void closeClassLoaderIfQuiesced() {
    boolean close;
    synchronized (lifecycleLock) {
      close = closed && leasedSessions == 0 && !classLoaderClosed;
      if (close) {
        classLoaderClosed = true;
      }
    }
    if (close) {
      closeClassLoaderQuietly(isolatedClassLoader, catalogConfig.name());
    }
  }

  private void drainAndCloseLocked() {
    List<BackendInvocationSession> drained = new ArrayList<>(poolSize);
    pool.drainTo(drained);
    for (BackendInvocationSession s : drained) {
      CatalogBackend.closeQuietly(s, "shared backend metastore session");
    }
  }

  private static MetastoreApiClassLoader openIsolatedClassLoader(
      ProxyConfig proxyConfig,
      CatalogConfig catalogConfig,
      MetastoreRuntimeProfile runtimeProfile
  ) throws MetaException {
    if (runtimeProfile == null || !runtimeProfile.requiresIsolation()) {
      return null;
    }
    try {
      return MetastoreApiClassLoader.forBackendRuntime(
          MetastoreRuntimeJarResolver.resolveBackendJar(proxyConfig, catalogConfig, runtimeProfile),
          runtimeProfile,
          BackendRuntime.class.getClassLoader());
    } catch (IllegalArgumentException | IllegalStateException | MalformedURLException e) {
      MetaException me = new MetaException(
          "Unable to initialize isolated backend runtime for catalog " + catalogConfig.name());
      me.initCause(e);
      throw me;
    }
  }

  private static void closeClassLoaderQuietly(MetastoreApiClassLoader classLoader, String catalogName) {
    if (classLoader == null) {
      return;
    }
    try {
      classLoader.close();
    } catch (IOException e) {
      LOG.warn("Ignoring failure while closing isolated backend classloader for catalog '{}'", catalogName, e);
    }
  }

  @FunctionalInterface
  private interface SessionCall {
    Object apply(BackendInvocationSession session) throws Throwable;
  }

  public interface SessionFactory {
    default boolean requiresIsolatedClassLoader(MetastoreRuntimeProfile runtimeProfile) {
      return false;
    }

    BackendInvocationSession open(
        ProxyConfig proxyConfig,
        CatalogConfig catalogConfig,
        HiveConf hiveConf,
        boolean backendKerberosEnabled,
        MetastoreRuntimeProfile runtimeProfile
    ) throws MetaException;

    default BackendInvocationSession open(
        ProxyConfig proxyConfig,
        CatalogConfig catalogConfig,
        HiveConf hiveConf,
        boolean backendKerberosEnabled,
        MetastoreRuntimeProfile runtimeProfile,
        ClassLoader isolatedClassLoader
    ) throws MetaException {
      return open(proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, runtimeProfile);
    }

    BackendInvocationSession openImpersonating(
        ProxyConfig proxyConfig,
        CatalogConfig catalogConfig,
        HiveConf hiveConf,
        boolean backendKerberosEnabled,
        MetastoreRuntimeProfile runtimeProfile,
        String userName,
        List<String> groupNames
    ) throws MetaException;

    default BackendInvocationSession openImpersonating(
        ProxyConfig proxyConfig,
        CatalogConfig catalogConfig,
        HiveConf hiveConf,
        boolean backendKerberosEnabled,
        MetastoreRuntimeProfile runtimeProfile,
        String userName,
        List<String> groupNames,
        ClassLoader isolatedClassLoader
    ) throws MetaException {
      return openImpersonating(
          proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, runtimeProfile, userName, groupNames);
    }
  }

  private static final class DefaultSessionFactory implements SessionFactory {
    @Override
    public boolean requiresIsolatedClassLoader(MetastoreRuntimeProfile runtimeProfile) {
      return runtimeProfile != null && runtimeProfile.requiresIsolation();
    }

    @Override
    public BackendInvocationSession open(
        ProxyConfig proxyConfig,
        CatalogConfig catalogConfig,
        HiveConf hiveConf,
        boolean backendKerberosEnabled,
        MetastoreRuntimeProfile runtimeProfile
    ) throws MetaException {
      return open(proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, runtimeProfile, null);
    }

    @Override
    public BackendInvocationSession open(
        ProxyConfig proxyConfig,
        CatalogConfig catalogConfig,
        HiveConf hiveConf,
        boolean backendKerberosEnabled,
        MetastoreRuntimeProfile runtimeProfile,
        ClassLoader isolatedClassLoader
    ) throws MetaException {
      return BackendInvocationSession.open(
          proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, runtimeProfile, isolatedClassLoader);
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
      return openImpersonating(
          proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, runtimeProfile, userName, groupNames, null);
    }

    @Override
    public BackendInvocationSession openImpersonating(
        ProxyConfig proxyConfig,
        CatalogConfig catalogConfig,
        HiveConf hiveConf,
        boolean backendKerberosEnabled,
        MetastoreRuntimeProfile runtimeProfile,
        String userName,
        List<String> groupNames,
        ClassLoader isolatedClassLoader
    ) throws MetaException {
      return BackendInvocationSession.openImpersonating(
          proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, runtimeProfile, userName, groupNames,
          isolatedClassLoader);
    }
  }
}
