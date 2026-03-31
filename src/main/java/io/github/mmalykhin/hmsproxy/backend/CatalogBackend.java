package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.compatibility.MetastoreCompatibility;
import io.github.mmalykhin.hmsproxy.compatibility.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreHandler;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CatalogBackend implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogBackend.class);
  private static final int MAX_IMPERSONATION_CLIENTS = 128;

  private final ProxyConfig proxyConfig;
  private final ProxyConfig.CatalogConfig config;
  private final HiveConf hiveConf;
  private final Map<String, ImpersonationClient> impersonationClients = new LinkedHashMap<>(16, 0.75f, true);
  private final BackendAdapter adapter;
  private final BackendRuntime runtime;
  private final Catalog catalog;

  private CatalogBackend(
      ProxyConfig proxyConfig,
      ProxyConfig.CatalogConfig config,
      HiveConf hiveConf,
      BackendAdapter adapter,
      BackendRuntime runtime,
      Catalog catalog
  ) {
    this.proxyConfig = proxyConfig;
    this.config = config;
    this.hiveConf = hiveConf;
    this.adapter = adapter;
    this.runtime = runtime;
    this.catalog = catalog;
  }

  public static CatalogBackend open(ProxyConfig proxyConfig, ProxyConfig.CatalogConfig catalogConfig)
      throws MetaException {
    HiveConf conf = new HiveConf();
    boolean backendKerberosEnabled = backendKerberosEnabled(catalogConfig);
    conf.set("hadoop.security.authentication", backendKerberosEnabled ? "kerberos" : "simple");
    for (Map.Entry<String, String> entry : catalogConfig.hiveConf().entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }

    MetastoreRuntimeProfile runtimeProfile = catalogConfig.runtimeProfile() != null
        ? catalogConfig.runtimeProfile()
        : MetastoreRuntimeProfile.APACHE_3_1_3;
    BackendAdapter adapter = BackendAdapterFactory.create(runtimeProfile);
    BackendRuntime runtime = BackendRuntime.open(proxyConfig, catalogConfig, conf, backendKerberosEnabled, runtimeProfile);
    LOG.info("Backend catalog '{}' selected runtimeProfile={} compatibilityProfile={}",
        catalogConfig.name(), adapter.runtimeProfile(), adapter.backendProfile());
    Catalog catalog = new Catalog();
    catalog.setName(catalogConfig.name());
    catalog.setDescription(catalogConfig.description());
    catalog.setLocationUri(catalogConfig.locationUri());
    return new CatalogBackend(proxyConfig, catalogConfig, conf, adapter, runtime, catalog);
  }

  public String name() {
    return config.name();
  }

  public Catalog catalog() {
    return new Catalog(catalog);
  }

  public boolean impersonationEnabled() {
    return config.impersonationEnabled();
  }

  public String backendVersion() {
    return adapter.backendVersion();
  }

  public MetastoreCompatibility.BackendProfile backendProfile() {
    return adapter.backendProfile();
  }

  public MetastoreRuntimeProfile runtimeProfile() {
    return adapter.runtimeProfile();
  }

  public boolean usesLegacyRequestApi() {
    return MetastoreCompatibility.usesLegacyRequestApi(adapter.backendProfile());
  }

  public void rememberLegacyRequestApi() {
    logLegacyRequestApiCompatibilitySwitch();
  }

  public Object invoke(Method method, Object[] args, RoutingMetaStoreHandler.ImpersonationContext impersonation)
      throws Throwable {
    return adapter.invoke(this, method, args, impersonation);
  }

  public Object invokeGetTableReq(GetTableRequest request, RoutingMetaStoreHandler.ImpersonationContext impersonation)
      throws Throwable {
    return adapter.invokeGetTableReq(this, request, impersonation);
  }

  public Object invokeGetTablesReq(GetTablesRequest request, RoutingMetaStoreHandler.ImpersonationContext impersonation)
      throws Throwable {
    return adapter.invokeGetTablesReq(this, request, impersonation);
  }

  public Object invokeRaw(Method method, Object[] args, RoutingMetaStoreHandler.ImpersonationContext impersonation)
      throws Throwable {
    if (impersonation != null && config.impersonationEnabled()) {
      return invokeWithImpersonation(method, args, impersonation);
    }
    if (impersonation != null && LOG.isDebugEnabled()) {
      LOG.debug("Backend catalog '{}' has impersonation disabled, using shared client for user '{}'",
          config.name(), impersonation.userName());
    }
    return invokeSharedClient(method, args);
  }

  public Object invokeRawByName(
      String methodName,
      Class<?>[] parameterTypes,
      Object[] args,
      RoutingMetaStoreHandler.ImpersonationContext impersonation
  ) throws Throwable {
    if (impersonation != null && config.impersonationEnabled()) {
      return impersonationClient(impersonation).invokeByName(methodName, parameterTypes, args);
    }
    if (impersonation != null && LOG.isDebugEnabled()) {
      LOG.debug("Backend catalog '{}' has impersonation disabled, using shared client for user '{}'",
          config.name(), impersonation.userName());
    }
    try {
      return runtime.invokeSharedByName(methodName, parameterTypes, args);
    } catch (Throwable cause) {
      if (!(cause instanceof org.apache.thrift.TApplicationException)
          && !(cause instanceof org.apache.thrift.transport.TTransportException)) {
        throw cause;
      }
      LOG.warn("Backend catalog '{}' transport failed in method {}, reconnecting once",
          config.name(), methodName, cause);
      runtime.reconnectShared(adapter);
      return runtime.invokeSharedByName(methodName, parameterTypes, args);
    }
  }

  public void logLegacyRequestApiCompatibilitySwitch() {
    LOG.info("Backend catalog '{}' switched to legacy request API compatibility mode", config.name());
  }

  private Object invokeSharedClient(Method method, Object[] args) throws Throwable {
    try {
      return runtime.invokeShared(method, args);
    } catch (Throwable cause) {
      if (!(cause instanceof org.apache.thrift.TApplicationException)
          && !(cause instanceof org.apache.thrift.transport.TTransportException)) {
        throw cause;
      }
      LOG.warn("Backend catalog '{}' transport failed in method {}, reconnecting once",
          config.name(), method.getName(), cause);
      runtime.reconnectShared(adapter);
      return runtime.invokeShared(method, args);
    }
  }

  private Object invokeWithImpersonation(
      Method method,
      Object[] args,
      RoutingMetaStoreHandler.ImpersonationContext impersonation
  ) throws Throwable {
    return impersonationClient(impersonation).invoke(method, args);
  }

  @Override
  public synchronized void close() {
    closeQuietly(runtime, "backend runtime");
    for (ImpersonationClient impersonationClient : impersonationClients.values()) {
      impersonationClient.closeQuietly();
    }
    impersonationClients.clear();
  }

  private static boolean backendKerberosEnabled(ProxyConfig.CatalogConfig catalogConfig) {
    return Boolean.parseBoolean(catalogConfig.hiveConf().getOrDefault("hive.metastore.sasl.enabled", "false"));
  }

  public static void closeQuietly(AutoCloseable closeable, String description) {
    if (closeable == null) {
      return;
    }
    try {
      closeable.close();
    } catch (Exception e) {
      LOG.warn("Ignoring failure while closing {}", description, e);
    }
  }

  private synchronized ImpersonationClient impersonationClient(
      RoutingMetaStoreHandler.ImpersonationContext impersonation
  ) throws MetaException {
    ImpersonationClient client = impersonationClients.get(impersonation.userName());
    if (client != null) {
      return client;
    }

    client = new ImpersonationClient(impersonation.userName(), impersonation.groupNames());
    impersonationClients.put(impersonation.userName(), client);
    evictOldImpersonationClientsIfNeeded();
    return client;
  }

  private void evictOldImpersonationClientsIfNeeded() {
    while (impersonationClients.size() > MAX_IMPERSONATION_CLIENTS) {
      String eldestUser = impersonationClients.keySet().iterator().next();
      ImpersonationClient evicted = impersonationClients.remove(eldestUser);
      if (evicted != null) {
        LOG.info("Evicting cached impersonation client for user '{}' in catalog '{}'",
            eldestUser, config.name());
        evicted.closeQuietly();
      }
    }
  }

  private final class ImpersonationClient implements AutoCloseable {
    private final String userName;
    private final List<String> groupNames;
    private BackendInvocationSession session;

    private ImpersonationClient(String userName, List<String> groupNames) throws MetaException {
      this.userName = userName;
      this.groupNames = List.copyOf(groupNames);
      open();
    }

    synchronized Object invoke(Method method, Object[] args) throws Throwable {
      try {
        if ("set_ugi".equals(method.getName())) {
          return List.copyOf(groupNames);
        }
        return session.invoke(method, args);
      } catch (Throwable cause) {
        if (!(cause instanceof org.apache.thrift.TApplicationException)
            && !(cause instanceof org.apache.thrift.transport.TTransportException)) {
          throw cause;
        }
        LOG.warn("Backend catalog '{}' transport failed for impersonated user '{}' in method {}, reconnecting once",
            config.name(), userName, method.getName(), cause);
        reconnect();
        try {
          if ("set_ugi".equals(method.getName())) {
            return List.copyOf(groupNames);
          }
          return session.invoke(method, args);
        } catch (Throwable retryError) {
          throw retryError;
        }
      }
    }

    synchronized Object invokeByName(String methodName, Class<?>[] parameterTypes, Object[] args) throws Throwable {
      try {
        if ("set_ugi".equals(methodName)) {
          return List.copyOf(groupNames);
        }
        return session.invokeByName(methodName, parameterTypes, args);
      } catch (Throwable cause) {
        if (!(cause instanceof org.apache.thrift.TApplicationException)
            && !(cause instanceof org.apache.thrift.transport.TTransportException)) {
          throw cause;
        }
        LOG.warn("Backend catalog '{}' transport failed for impersonated user '{}' in method {}, reconnecting once",
            config.name(), userName, methodName, cause);
        reconnect();
        if ("set_ugi".equals(methodName)) {
          return List.copyOf(groupNames);
        }
        return session.invokeByName(methodName, parameterTypes, args);
      }
    }

    @Override
    public synchronized void close() {
      CatalogBackend.closeQuietly(session, "impersonation backend metastore session for user '" + userName + "'");
    }

    private void closeQuietly() {
      try {
        close();
      } catch (RuntimeException e) {
        LOG.warn("Failed to close cached impersonation client for user '{}' in catalog '{}'",
            userName, config.name(), e);
      }
    }

    private void open() throws MetaException {
      session = runtime.openImpersonationSession(adapter.runtimeProfile(), userName, groupNames);
      LOG.debug("Opened cached impersonation client for user '{}' in catalog '{}'", userName, config.name());
    }

    private synchronized void reconnect() throws MetaException {
      close();
      open();
    }
  }
}
