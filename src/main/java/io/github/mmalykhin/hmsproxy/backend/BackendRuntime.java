package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.compatibility.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
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
  private BackendInvocationSession sharedSession;

  private BackendRuntime(
      ProxyConfig proxyConfig,
      ProxyConfig.CatalogConfig catalogConfig,
      HiveConf hiveConf,
      boolean backendKerberosEnabled,
      SessionFactory sessionFactory,
      BackendInvocationSession sharedSession
  ) {
    this.proxyConfig = proxyConfig;
    this.catalogConfig = catalogConfig;
    this.hiveConf = hiveConf;
    this.backendKerberosEnabled = backendKerberosEnabled;
    this.sessionFactory = sessionFactory;
    this.sharedSession = sharedSession;
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
    BackendInvocationSession sharedSession = sessionFactory.open(
        proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, runtimeProfile);
    return new BackendRuntime(
        proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, sessionFactory, sharedSession);
  }

  public synchronized Object invokeShared(Method method, Object[] args) throws Throwable {
    return sharedSession.invoke(method, args);
  }

  public synchronized Object invokeSharedByName(String methodName, Class<?>[] parameterTypes, Object[] args) throws Throwable {
    return sharedSession.invokeByName(methodName, parameterTypes, args);
  }

  public synchronized String reconnectShared(BackendAdapter adapter) throws MetaException {
    CatalogBackend.closeQuietly(sharedSession, "stale shared backend metastore session before reconnect");
    sharedSession = sessionFactory.open(
        proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, adapter.runtimeProfile());
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

  @Override
  public synchronized void close() {
    CatalogBackend.closeQuietly(sharedSession, "shared backend metastore session");
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
