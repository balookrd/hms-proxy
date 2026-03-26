package io.github.mmalykhin.hmsproxy;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class CatalogBackend implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogBackend.class);
  private static final int MAX_IMPERSONATION_CLIENTS = 128;

  private final ProxyConfig proxyConfig;
  private final ProxyConfig.CatalogConfig config;
  private final HiveConf hiveConf;
  private final boolean backendKerberosEnabled;
  private final Map<String, ImpersonationClient> impersonationClients = new LinkedHashMap<>(16, 0.75f, true);
  private HiveMetaStoreClient client;
  private ThriftHiveMetastore.Iface thriftClient;
  private final Catalog catalog;

  private CatalogBackend(
      ProxyConfig proxyConfig,
      ProxyConfig.CatalogConfig config,
      HiveConf hiveConf,
      boolean backendKerberosEnabled,
      HiveMetaStoreClient client,
      ThriftHiveMetastore.Iface thriftClient,
      Catalog catalog
  ) {
    this.proxyConfig = proxyConfig;
    this.config = config;
    this.hiveConf = hiveConf;
    this.backendKerberosEnabled = backendKerberosEnabled;
    this.client = client;
    this.thriftClient = thriftClient;
    this.catalog = catalog;
  }

  static CatalogBackend open(ProxyConfig proxyConfig, ProxyConfig.CatalogConfig catalogConfig)
      throws MetaException {
    HiveConf conf = new HiveConf();
    boolean backendKerberosEnabled = backendKerberosEnabled(catalogConfig);
    conf.set("hadoop.security.authentication", backendKerberosEnabled ? "kerberos" : "simple");
    for (Map.Entry<String, String> entry : catalogConfig.hiveConf().entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }

    HiveMetaStoreClient client = openClient(proxyConfig, catalogConfig, conf, backendKerberosEnabled);
    ThriftHiveMetastore.Iface thriftClient = extractThriftClient(client);
    Catalog catalog = new Catalog();
    catalog.setName(catalogConfig.name());
    catalog.setDescription(catalogConfig.description());
    catalog.setLocationUri(catalogConfig.locationUri());
    return new CatalogBackend(proxyConfig, catalogConfig, conf, backendKerberosEnabled, client, thriftClient, catalog);
  }

  String name() {
    return config.name();
  }

  Catalog catalog() {
    return new Catalog(catalog);
  }

  boolean impersonationEnabled() {
    return config.impersonationEnabled();
  }

  ThriftHiveMetastore.Iface thriftClient() {
    return thriftClient;
  }

  Object invoke(Method method, Object[] args, RoutingMetaStoreHandler.ImpersonationContext impersonation)
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

  private synchronized Object invokeSharedClient(Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(thriftClient, args);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (!isTransportFailure(cause)) {
        throw cause;
      }
      LOG.warn("Backend catalog '{}' transport failed in method {}, reconnecting once",
          config.name(), method.getName(), cause);
      reconnect();
      try {
        return method.invoke(thriftClient, args);
      } catch (InvocationTargetException retryError) {
        throw retryError.getCause();
      }
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
    client.close();
    for (ImpersonationClient impersonationClient : impersonationClients.values()) {
      impersonationClient.closeQuietly();
    }
    impersonationClients.clear();
  }

  private static HiveMetaStoreClient openClient(
      ProxyConfig proxyConfig,
      ProxyConfig.CatalogConfig catalogConfig,
      HiveConf conf,
      boolean backendKerberosEnabled
  ) throws MetaException {
    if (!backendKerberosEnabled) {
      return new HiveMetaStoreClient(conf);
    }

    ProxyConfig.SecurityConfig security = proxyConfig.security();
    String principal = security.outboundPrincipal();
    String keytab = security.outboundKeytab();
    LOG.info("Connecting to backend catalog '{}' with Kerberos principal {} using keytab {}",
        catalogConfig.name(), principal, keytab);

    Configuration securityConf = new Configuration(false);
    securityConf.set("hadoop.security.authentication", "kerberos");
    UserGroupInformation.setConfiguration(securityConf);

    try {
      UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
      return ugi.doAs((PrivilegedExceptionAction<HiveMetaStoreClient>) () -> new HiveMetaStoreClient(conf));
    } catch (Exception e) {
      MetaException metaException = new MetaException(
          "Unable to open backend metastore client for catalog "
              + catalogConfig.name()
              + " with Kerberos principal "
              + principal);
      metaException.initCause(e);
      throw metaException;
    }
  }

  // HiveMetaStoreClient implements IMetaStoreClient (high-level API), not ThriftHiveMetastore.Iface
  // (raw Thrift API). The proxy intercepts ThriftHiveMetastore.Iface calls and must forward them to
  // the same interface on each backend. There is no public API to obtain the raw Thrift client from
  // HiveMetaStoreClient, so we use reflection to access the private "client" field.
  // If this breaks after a Hive upgrade, the alternative is to build the Thrift transport directly.
  private static ThriftHiveMetastore.Iface extractThriftClient(HiveMetaStoreClient client)
      throws MetaException {
    try {
      Field field = HiveMetaStoreClient.class.getDeclaredField("client");
      field.setAccessible(true);
      Object value = field.get(client);
      if (!(value instanceof ThriftHiveMetastore.Iface)) {
        throw new MetaException(
            "Unexpected type for HiveMetaStoreClient.client field: "
                + (value == null ? "null" : value.getClass().getName()));
      }
      return (ThriftHiveMetastore.Iface) value;
    } catch (ReflectiveOperationException e) {
      MetaException metaException = new MetaException(
          "Unable to access underlying thrift client via reflection. "
              + "This may happen after a Hive library upgrade that changes HiveMetaStoreClient internals.");
      metaException.initCause(e);
      throw metaException;
    }
  }

  private static boolean backendKerberosEnabled(ProxyConfig.CatalogConfig catalogConfig) {
    return Boolean.parseBoolean(catalogConfig.hiveConf().getOrDefault("hive.metastore.sasl.enabled", "false"));
  }

  private synchronized void reconnect() throws MetaException {
    client.close();
    client = openClient(proxyConfig, config, hiveConf, backendKerberosEnabled);
    thriftClient = extractThriftClient(client);
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

  private static boolean isTransportFailure(Throwable throwable) {
    return throwable instanceof TTransportException
        || throwable instanceof org.apache.thrift.TApplicationException;
  }

  private final class ImpersonationClient implements AutoCloseable {
    private final String userName;
    private final List<String> groupNames;
    private HiveMetaStoreClient client;
    private ThriftHiveMetastore.Iface thriftClient;

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
        return method.invoke(thriftClient, args);
      } catch (InvocationTargetException e) {
        Throwable cause = e.getCause();
        if (!isTransportFailure(cause)) {
          throw cause;
        }
        LOG.warn("Backend catalog '{}' transport failed for impersonated user '{}' in method {}, reconnecting once",
            config.name(), userName, method.getName(), cause);
        reconnect();
        try {
          if ("set_ugi".equals(method.getName())) {
            return List.copyOf(groupNames);
          }
          return method.invoke(thriftClient, args);
        } catch (InvocationTargetException retryError) {
          throw retryError.getCause();
        }
      }
    }

    @Override
    public synchronized void close() {
      client.close();
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
      client = openClient(proxyConfig, config, hiveConf, backendKerberosEnabled);
      try {
        thriftClient = extractThriftClient(client);
        thriftClient.set_ugi(userName, groupNames);
        LOG.debug("Opened cached impersonation client for user '{}' in catalog '{}'", userName, config.name());
      } catch (Exception e) {
        client.close();
        MetaException metaException = new MetaException(
            "Unable to open impersonating backend metastore client for catalog "
                + config.name()
                + " and user "
                + userName);
        metaException.initCause(e);
        throw metaException;
      }
    }

    private synchronized void reconnect() throws MetaException {
      close();
      open();
    }
  }
}
