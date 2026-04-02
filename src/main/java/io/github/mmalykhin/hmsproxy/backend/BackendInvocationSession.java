package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.compatibility.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreHandler;
import io.github.mmalykhin.hmsproxy.security.KerberosPrincipalUtil;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BackendInvocationSession implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(BackendInvocationSession.class);

  private final HiveMetaStoreClient client;
  private final ThriftHiveMetastore.Iface thriftClient;
  private final IsolatedMetastoreClient isolatedClient;

  private BackendInvocationSession(
      HiveMetaStoreClient client,
      ThriftHiveMetastore.Iface thriftClient,
      IsolatedMetastoreClient isolatedClient
  ) {
    this.client = client;
    this.thriftClient = thriftClient;
    this.isolatedClient = isolatedClient;
  }

  public static BackendInvocationSession open(
      ProxyConfig proxyConfig,
      ProxyConfig.CatalogConfig catalogConfig,
      HiveConf conf,
      boolean backendKerberosEnabled,
      MetastoreRuntimeProfile runtimeProfile
  ) throws MetaException {
    return runtimeProfile.isHortonworks()
        ? openIsolated(proxyConfig, catalogConfig, conf, backendKerberosEnabled, runtimeProfile)
        : openApache(proxyConfig, catalogConfig, conf, backendKerberosEnabled);
  }

  public static BackendInvocationSession openImpersonating(
      ProxyConfig proxyConfig,
      ProxyConfig.CatalogConfig catalogConfig,
      HiveConf conf,
      boolean backendKerberosEnabled,
      MetastoreRuntimeProfile runtimeProfile,
      String userName,
      List<String> groupNames
  ) throws MetaException {
    BackendInvocationSession session = open(
        proxyConfig, catalogConfig, conf, backendKerberosEnabled, runtimeProfile);
    try {
      session.setUgi(userName, groupNames);
      return session;
    } catch (Throwable e) {
      CatalogBackend.closeQuietly(session, "failed impersonation backend metastore session for user '" + userName + "'");
      MetaException metaException = new MetaException(
          "Unable to open impersonating backend metastore client for catalog "
              + catalogConfig.name()
              + " and user "
              + userName
              + ". Backend HMS must allow proxy-user impersonation for outbound principal "
              + proxyConfig.security().outboundPrincipal()
              + " (for example via hadoop.proxyuser."
              + RoutingMetaStoreHandler.shortUserName(proxyConfig.security().outboundPrincipal())
              + ".*), or impersonation must be disabled for this backend via catalog."
              + catalogConfig.name()
              + ".impersonation-enabled=false");
      metaException.initCause(e);
      throw metaException;
    }
  }

  void setUgi(String userName, List<String> groupNames) throws Throwable {
    if (isolatedClient != null) {
      isolatedClient.setUgi(userName, groupNames);
      return;
    }
    thriftClient.set_ugi(userName, groupNames);
  }

  Object invoke(Method method, Object[] args) throws Throwable {
    if (isolatedClient != null) {
      return isolatedClient.invoke(method, args);
    }
    try {
      return method.invoke(thriftClient, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  Object invokeByName(String methodName, Class<?>[] parameterTypes, Object[] args) throws Throwable {
    if (isolatedClient != null) {
      return isolatedClient.invokeByName(methodName, parameterTypes, args);
    }
    Method method = ThriftHiveMetastore.Iface.class.getMethod(methodName, parameterTypes);
    return invoke(method, args);
  }

  @Override
  public void close() {
    CatalogBackend.closeQuietly(client, "backend metastore client session");
    CatalogBackend.closeQuietly(isolatedClient, "isolated backend metastore client session");
  }

  private static BackendInvocationSession openApache(
      ProxyConfig proxyConfig,
      ProxyConfig.CatalogConfig catalogConfig,
      HiveConf conf,
      boolean backendKerberosEnabled
  ) throws MetaException {
    HiveMetaStoreClient client = openApacheClient(proxyConfig, catalogConfig, conf, backendKerberosEnabled);
    ThriftHiveMetastore.Iface thriftClient = extractThriftClient(client);
    return new BackendInvocationSession(client, thriftClient, null);
  }

  private static BackendInvocationSession openIsolated(
      ProxyConfig proxyConfig,
      ProxyConfig.CatalogConfig catalogConfig,
      HiveConf conf,
      boolean backendKerberosEnabled,
      MetastoreRuntimeProfile runtimeProfile
  ) throws MetaException {
    if (!backendKerberosEnabled) {
      try {
        return new BackendInvocationSession(
            null,
            null,
            IsolatedMetastoreClient.open(proxyConfig, catalogConfig, runtimeProfile, conf));
      } catch (Exception e) {
        MetaException metaException = new MetaException(
            "Unable to open isolated backend metastore client for catalog " + catalogConfig.name());
        metaException.initCause(e);
        throw metaException;
      }
    }

    ProxyConfig.SecurityConfig security = proxyConfig.security();
    String principal = KerberosPrincipalUtil.resolveForLocalHost(security.outboundPrincipal());
    String keytab = security.outboundKeytab();
    LOG.info("Connecting to backend catalog '{}' with isolated runtime {} using Kerberos principal {} and keytab {}",
        catalogConfig.name(), runtimeProfile, principal, keytab);

    try {
      IsolatedMetastoreClient isolatedClient = IsolatedMetastoreClient.open(
          proxyConfig,
          catalogConfig,
          runtimeProfile,
          principal,
          keytab,
          conf);
      return new BackendInvocationSession(null, null, isolatedClient);
    } catch (Exception e) {
      MetaException metaException = new MetaException(
          "Unable to open isolated backend metastore client for catalog "
              + catalogConfig.name()
              + " with Kerberos principal "
              + principal);
      metaException.initCause(e);
      throw metaException;
    }
  }

  private static HiveMetaStoreClient openApacheClient(
      ProxyConfig proxyConfig,
      ProxyConfig.CatalogConfig catalogConfig,
      HiveConf conf,
      boolean backendKerberosEnabled
  ) throws MetaException {
    if (!backendKerberosEnabled) {
      return new HiveMetaStoreClient(conf);
    }

    ProxyConfig.SecurityConfig security = proxyConfig.security();
    String principal = KerberosPrincipalUtil.resolveForLocalHost(security.outboundPrincipal());
    String keytab = security.outboundKeytab();
    LOG.info("Connecting to backend catalog '{}' with Kerberos principal {} using keytab {}",
        catalogConfig.name(), principal, keytab);

    configureKerberosAuthentication();

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

  private static synchronized void configureKerberosAuthentication() {
    Configuration securityConf = new Configuration(false);
    securityConf.set("hadoop.security.authentication", "kerberos");
    UserGroupInformation.setConfiguration(securityConf);
  }

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
}
