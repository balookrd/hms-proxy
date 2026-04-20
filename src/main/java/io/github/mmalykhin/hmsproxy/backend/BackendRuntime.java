package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.config.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BackendRuntime implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(BackendRuntime.class);
  private static final SessionFactory DEFAULT_SESSION_FACTORY = new DefaultSessionFactory();

  private final ProxyConfig proxyConfig;
  private final ProxyConfig.CatalogConfig catalogConfig;
  private final HiveConf hiveConf;
  private final boolean backendKerberosEnabled;
  private final SessionFactory sessionFactory;
  private final int poolSize;
  private final Semaphore permits;
  private final LinkedBlockingQueue<BackendInvocationSession> pool;
  private volatile MetastoreRuntimeProfile activeProfile;

  private BackendRuntime(
      ProxyConfig proxyConfig,
      ProxyConfig.CatalogConfig catalogConfig,
      HiveConf hiveConf,
      boolean backendKerberosEnabled,
      SessionFactory sessionFactory,
      MetastoreRuntimeProfile activeProfile,
      BackendInvocationSession initialSession
  ) {
    this.proxyConfig = proxyConfig;
    this.catalogConfig = catalogConfig;
    this.hiveConf = hiveConf;
    this.backendKerberosEnabled = backendKerberosEnabled;
    this.sessionFactory = sessionFactory;
    this.poolSize = catalogConfig.sharedSessionPoolSize();
    this.permits = new Semaphore(poolSize);
    this.pool = new LinkedBlockingQueue<>(poolSize);
    this.activeProfile = activeProfile;
    this.pool.offer(initialSession);
  }

  public static BackendRuntime open(
      ProxyConfig proxyConfig,
      ProxyConfig.CatalogConfig catalogConfig,
      HiveConf hiveConf,
      boolean backendKerberosEnabled,
      MetastoreRuntimeProfile runtimeProfile
  ) throws MetaException {
    return open(proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, runtimeProfile, DEFAULT_SESSION_FACTORY);
  }

  public static BackendRuntime open(
      ProxyConfig proxyConfig,
      ProxyConfig.CatalogConfig catalogConfig,
      HiveConf hiveConf,
      boolean backendKerberosEnabled,
      MetastoreRuntimeProfile runtimeProfile,
      SessionFactory sessionFactory
  ) throws MetaException {
    BackendInvocationSession initial = sessionFactory.open(
        proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, runtimeProfile);
    return new BackendRuntime(
        proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, sessionFactory, runtimeProfile, initial);
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
    permits.acquireUninterruptibly(poolSize);
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
    permits.acquireUninterruptibly(poolSize);
    try {
      drainAndCloseLocked();
    } finally {
      permits.release(poolSize);
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
    permits.acquireUninterruptibly();
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
        ProxyConfig.CatalogConfig catalogConfig,
        HiveConf hiveConf,
        boolean backendKerberosEnabled,
        MetastoreRuntimeProfile runtimeProfile
    ) throws MetaException;

    BackendInvocationSession openImpersonating(
        ProxyConfig proxyConfig,
        ProxyConfig.CatalogConfig catalogConfig,
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
        ProxyConfig.CatalogConfig catalogConfig,
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
        ProxyConfig.CatalogConfig catalogConfig,
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
