package io.github.mmalykhin.hmsproxy;

import java.lang.reflect.Method;
import java.util.List;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class BackendRuntime implements AutoCloseable {
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

  static BootstrapState bootstrap(
      ProxyConfig proxyConfig,
      ProxyConfig.CatalogConfig catalogConfig,
      HiveConf hiveConf,
      boolean backendKerberosEnabled
  ) throws MetaException {
    return bootstrap(proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, DEFAULT_SESSION_FACTORY);
  }

  static BootstrapState bootstrap(
      ProxyConfig proxyConfig,
      ProxyConfig.CatalogConfig catalogConfig,
      HiveConf hiveConf,
      boolean backendKerberosEnabled,
      SessionFactory sessionFactory
  ) throws MetaException {
    BackendInvocationSession bootstrapSession = sessionFactory.open(
        proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, MetastoreRuntimeProfile.APACHE_3_1_3);
    String backendVersion = detectBackendVersion(catalogConfig.name(), bootstrapSession);
    return new BootstrapState(bootstrapSession, backendVersion);
  }

  static BackendRuntime open(
      ProxyConfig proxyConfig,
      ProxyConfig.CatalogConfig catalogConfig,
      HiveConf hiveConf,
      boolean backendKerberosEnabled,
      MetastoreRuntimeProfile runtimeProfile,
      BackendInvocationSession bootstrapSession
  ) throws MetaException {
    return open(
        proxyConfig,
        catalogConfig,
        hiveConf,
        backendKerberosEnabled,
        runtimeProfile,
        bootstrapSession,
        DEFAULT_SESSION_FACTORY);
  }

  static BackendRuntime open(
      ProxyConfig proxyConfig,
      ProxyConfig.CatalogConfig catalogConfig,
      HiveConf hiveConf,
      boolean backendKerberosEnabled,
      MetastoreRuntimeProfile runtimeProfile,
      BackendInvocationSession bootstrapSession,
      SessionFactory sessionFactory
  ) throws MetaException {
    BackendInvocationSession sharedSession = bootstrapSession;
    if (runtimeProfile != MetastoreRuntimeProfile.APACHE_3_1_3) {
      CatalogBackend.closeQuietly(bootstrapSession, "bootstrap Apache backend metastore session");
      sharedSession = sessionFactory.open(
          proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, runtimeProfile);
    }
    return new BackendRuntime(
        proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, sessionFactory, sharedSession);
  }

  synchronized Object invokeShared(Method method, Object[] args) throws Throwable {
    return sharedSession.invoke(method, args);
  }

  synchronized Object invokeSharedByName(String methodName, Class<?>[] parameterTypes, Object[] args) throws Throwable {
    return sharedSession.invokeByName(methodName, parameterTypes, args);
  }

  synchronized String reconnectShared(BackendAdapter adapter) throws MetaException {
    CatalogBackend.closeQuietly(sharedSession, "stale shared backend metastore session before reconnect");
    BootstrapState bootstrapState = bootstrap(proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, sessionFactory);
    adapter.updateBackendVersion(bootstrapState.backendVersion());
    if (adapter.runtimeProfile() == MetastoreRuntimeProfile.APACHE_3_1_3) {
      sharedSession = bootstrapState.session();
    } else {
      CatalogBackend.closeQuietly(bootstrapState.session(), "bootstrap Apache backend metastore session after reconnect");
      sharedSession = sessionFactory.open(
          proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, adapter.runtimeProfile());
    }
    return adapter.backendVersion();
  }

  BackendInvocationSession openImpersonationSession(
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

  private static String detectBackendVersion(String catalogName, BackendInvocationSession session) {
    try {
      String version = session.getVersion();
      if (version != null && !version.isBlank()) {
        LOG.info("Detected backend catalog '{}' metastore version {}", catalogName, version);
      }
      return version;
    } catch (Throwable e) {
      LOG.debug("Unable to detect backend catalog '{}' metastore version via getVersion()", catalogName, e);
      return null;
    }
  }

  record BootstrapState(BackendInvocationSession session, String backendVersion) {
  }

  interface SessionFactory {
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
