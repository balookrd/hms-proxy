package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.server.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.observability.BackendKerberosLoginTracker;
import io.github.mmalykhin.hmsproxy.security.KerberosPrincipalUtil;
import io.github.mmalykhin.hmsproxy.security.LoginSubjects;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftValueConverter;
import io.github.mmalykhin.hmsproxy.util.PrincipalUtil;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfig;

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
      CatalogConfig catalogConfig,
      HiveConf conf,
      boolean backendKerberosEnabled,
      MetastoreRuntimeProfile runtimeProfile
  ) throws MetaException {
    return open(proxyConfig, catalogConfig, conf, backendKerberosEnabled, runtimeProfile, null);
  }

  static BackendInvocationSession open(
      ProxyConfig proxyConfig,
      CatalogConfig catalogConfig,
      HiveConf conf,
      boolean backendKerberosEnabled,
      MetastoreRuntimeProfile runtimeProfile,
      ClassLoader isolatedClassLoader
  ) throws MetaException {
    return open(proxyConfig, catalogConfig, conf, backendKerberosEnabled, runtimeProfile, isolatedClassLoader, null);
  }

  static BackendInvocationSession open(
      ProxyConfig proxyConfig,
      CatalogConfig catalogConfig,
      HiveConf conf,
      boolean backendKerberosEnabled,
      MetastoreRuntimeProfile runtimeProfile,
      ClassLoader isolatedClassLoader,
      String impersonatedUser
  ) throws MetaException {
    return open(proxyConfig, catalogConfig, conf, backendKerberosEnabled, runtimeProfile, isolatedClassLoader, impersonatedUser, null);
  }

  static BackendInvocationSession open(
      ProxyConfig proxyConfig,
      CatalogConfig catalogConfig,
      HiveConf conf,
      boolean backendKerberosEnabled,
      MetastoreRuntimeProfile runtimeProfile,
      ClassLoader isolatedClassLoader,
      String impersonatedUser,
      String delegationToken
  ) throws MetaException {
    return runtimeProfile != null && runtimeProfile.requiresIsolation()
        ? openIsolated(proxyConfig, catalogConfig, conf, backendKerberosEnabled, runtimeProfile, isolatedClassLoader, impersonatedUser, delegationToken)
        : openApache(proxyConfig, catalogConfig, conf, backendKerberosEnabled, impersonatedUser, delegationToken);
  }

  public static BackendInvocationSession openImpersonating(
      ProxyConfig proxyConfig,
      CatalogConfig catalogConfig,
      HiveConf conf,
      boolean backendKerberosEnabled,
      MetastoreRuntimeProfile runtimeProfile,
      String userName,
      List<String> groupNames
  ) throws MetaException {
    return openImpersonating(
        proxyConfig, catalogConfig, conf, backendKerberosEnabled, runtimeProfile, userName, groupNames, null, null);
  }

  static BackendInvocationSession openImpersonating(
      ProxyConfig proxyConfig,
      CatalogConfig catalogConfig,
      HiveConf conf,
      boolean backendKerberosEnabled,
      MetastoreRuntimeProfile runtimeProfile,
      String userName,
      List<String> groupNames,
      ClassLoader isolatedClassLoader
  ) throws MetaException {
    return openImpersonating(
        proxyConfig, catalogConfig, conf, backendKerberosEnabled, runtimeProfile, userName, groupNames, isolatedClassLoader, null);
  }

  static BackendInvocationSession openImpersonating(
      ProxyConfig proxyConfig,
      CatalogConfig catalogConfig,
      HiveConf conf,
      boolean backendKerberosEnabled,
      MetastoreRuntimeProfile runtimeProfile,
      String userName,
      List<String> groupNames,
      ClassLoader isolatedClassLoader,
      String delegationToken
  ) throws MetaException {
    BackendInvocationSession session = open(
        proxyConfig, catalogConfig, conf, backendKerberosEnabled, runtimeProfile, isolatedClassLoader, userName, delegationToken);
    if (delegationToken != null && !delegationToken.isBlank()) {
      return session;
    }
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
              + PrincipalUtil.shortUserName(proxyConfig.security().outboundPrincipal())
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
    Method method = ThriftMethodCache.lookup(
        ThriftHiveMetastore.Iface.class,
        methodName,
        parameterTypes,
        () -> resolveApacheMethod(methodName, parameterTypes));
    Object[] convertedArgs = convertArgumentsForApache(args, method.getParameterTypes());
    return invoke(method, convertedArgs);
  }

  private static Method resolveApacheMethod(String methodName, Class<?>[] parameterTypes) throws NoSuchMethodException {
    try {
      return ThriftHiveMetastore.Iface.class.getMethod(methodName, parameterTypes);
    } catch (NoSuchMethodException ignored) {
      for (Method candidate : ThriftHiveMetastore.Iface.class.getMethods()) {
        if (candidate.getName().equals(methodName) && candidate.getParameterCount() == parameterTypes.length) {
          return candidate;
        }
      }
      throw new NoSuchMethodException(methodName);
    }
  }

  private static Object[] convertArgumentsForApache(Object[] args, Class<?>[] targetTypes) throws Exception {
    if (args == null || args.length == 0) {
      return args;
    }
    Object[] converted = new Object[args.length];
    for (int i = 0; i < args.length; i++) {
      converted[i] = ThriftValueConverter.convertValue(args[i], targetTypes[i],
          BackendInvocationSession.class.getClassLoader());
    }
    return converted;
  }

  @Override
  public void close() {
    CatalogBackend.closeQuietly(client, "backend metastore client session");
    CatalogBackend.closeQuietly(isolatedClient, "isolated backend metastore client session");
  }

  private static BackendInvocationSession openApache(
      ProxyConfig proxyConfig,
      CatalogConfig catalogConfig,
      HiveConf conf,
      boolean backendKerberosEnabled
  ) throws MetaException {
    return openApache(proxyConfig, catalogConfig, conf, backendKerberosEnabled, null, null);
  }

  private static BackendInvocationSession openApache(
      ProxyConfig proxyConfig,
      CatalogConfig catalogConfig,
      HiveConf conf,
      boolean backendKerberosEnabled,
      String impersonatedUser
  ) throws MetaException {
    return openApache(proxyConfig, catalogConfig, conf, backendKerberosEnabled, impersonatedUser, null);
  }

  private static BackendInvocationSession openApache(
      ProxyConfig proxyConfig,
      CatalogConfig catalogConfig,
      HiveConf conf,
      boolean backendKerberosEnabled,
      String impersonatedUser,
      String delegationToken
  ) throws MetaException {
    HiveMetaStoreClient client = openApacheClient(proxyConfig, catalogConfig, conf, backendKerberosEnabled, impersonatedUser, delegationToken);
    ThriftHiveMetastore.Iface thriftClient = extractThriftClientOrClose(client);
    return new BackendInvocationSession(client, thriftClient, null);
  }

  private static BackendInvocationSession openIsolated(
      ProxyConfig proxyConfig,
      CatalogConfig catalogConfig,
      HiveConf conf,
      boolean backendKerberosEnabled,
      MetastoreRuntimeProfile runtimeProfile,
      ClassLoader isolatedClassLoader
  ) throws MetaException {
    return openIsolated(proxyConfig, catalogConfig, conf, backendKerberosEnabled, runtimeProfile, isolatedClassLoader, null, null);
  }

  private static BackendInvocationSession openIsolated(
      ProxyConfig proxyConfig,
      CatalogConfig catalogConfig,
      HiveConf conf,
      boolean backendKerberosEnabled,
      MetastoreRuntimeProfile runtimeProfile,
      ClassLoader isolatedClassLoader,
      String impersonatedUser
  ) throws MetaException {
    return openIsolated(proxyConfig, catalogConfig, conf, backendKerberosEnabled, runtimeProfile, isolatedClassLoader, impersonatedUser, null);
  }

  private static BackendInvocationSession openIsolated(
      ProxyConfig proxyConfig,
      CatalogConfig catalogConfig,
      HiveConf conf,
      boolean backendKerberosEnabled,
      MetastoreRuntimeProfile runtimeProfile,
      ClassLoader isolatedClassLoader,
      String impersonatedUser,
      String delegationToken
  ) throws MetaException {
    if (!backendKerberosEnabled) {
      try {
        return new BackendInvocationSession(
            null,
            null,
            IsolatedMetastoreClient.open(proxyConfig, catalogConfig, runtimeProfile, isolatedClassLoader, conf));
      } catch (Exception e) {
        MetaException metaException = new MetaException(
            "Unable to open isolated backend metastore client for catalog " + catalogConfig.name());
        metaException.initCause(e);
        throw metaException;
      }
    }

    SecurityConfig security = proxyConfig.security();
    String principal = KerberosPrincipalUtil.resolveForLocalHost(security.outboundPrincipal());
    String keytab = security.outboundKeytab();
    if (delegationToken != null && !delegationToken.isBlank()) {
      LOG.info("Connecting to backend catalog '{}' with isolated runtime {} using delegation token impersonating user '{}'",
          catalogConfig.name(), runtimeProfile, impersonatedUser);
    } else if (impersonatedUser != null && !impersonatedUser.isBlank()) {
      LOG.info("Connecting to backend catalog '{}' with isolated runtime {} using Kerberos principal {} and keytab {} impersonating user '{}'",
          catalogConfig.name(), runtimeProfile, principal, keytab, impersonatedUser);
    } else {
      LOG.info("Connecting to backend catalog '{}' with isolated runtime {} using Kerberos principal {} and keytab {}",
          catalogConfig.name(), runtimeProfile, principal, keytab);
    }

    try {
      IsolatedMetastoreClient isolatedClient = IsolatedMetastoreClient.open(
          proxyConfig,
          catalogConfig,
          runtimeProfile,
          isolatedClassLoader,
          principal,
          keytab,
          impersonatedUser,
          delegationToken,
          conf);
      return new BackendInvocationSession(null, null, isolatedClient);
    } catch (Exception e) {
      MetaException metaException = new MetaException(
          "Unable to open isolated backend metastore client for catalog "
              + catalogConfig.name()
              + (delegationToken != null
                  ? " with delegation token for user '" + impersonatedUser + "'"
                  : " with Kerberos principal " + principal + (impersonatedUser != null ? " impersonating user '" + impersonatedUser + "'" : "")));
      metaException.initCause(e);
      throw metaException;
    }
  }

  private static HiveMetaStoreClient openApacheClient(
      ProxyConfig proxyConfig,
      CatalogConfig catalogConfig,
      HiveConf conf,
      boolean backendKerberosEnabled
  ) throws MetaException {
    return openApacheClient(proxyConfig, catalogConfig, conf, backendKerberosEnabled, null, null);
  }

  private static HiveMetaStoreClient openApacheClient(
      ProxyConfig proxyConfig,
      CatalogConfig catalogConfig,
      HiveConf conf,
      boolean backendKerberosEnabled,
      String impersonatedUser
  ) throws MetaException {
    return openApacheClient(proxyConfig, catalogConfig, conf, backendKerberosEnabled, impersonatedUser, null);
  }

  private static HiveMetaStoreClient openApacheClient(
      ProxyConfig proxyConfig,
      CatalogConfig catalogConfig,
      HiveConf conf,
      boolean backendKerberosEnabled,
      String impersonatedUser,
      String delegationToken
  ) throws MetaException {
    if (!backendKerberosEnabled) {
      return new HiveMetaStoreClient(conf);
    }

    if (delegationToken != null && !delegationToken.isBlank()) {
      LOG.info("Connecting to backend catalog '{}' using delegation token impersonating user '{}'",
          catalogConfig.name(), impersonatedUser);
      try {
        org.apache.hadoop.security.token.Token<?> token = new org.apache.hadoop.security.token.Token<>();
        token.decodeFromUrlString(delegationToken);
        UserGroupInformation tokenUgi = UserGroupInformation.createRemoteUser(impersonatedUser);
        tokenUgi.addToken(token);
        return tokenUgi.doAs((PrivilegedExceptionAction<HiveMetaStoreClient>) () -> new HiveMetaStoreClient(conf));
      } catch (Exception e) {
        LOG.error("Failed to open backend metastore client for catalog '{}' with delegation token for user '{}'",
            catalogConfig.name(), impersonatedUser, e);
        MetaException metaException = new MetaException(
            "Unable to open backend metastore client for catalog "
                + catalogConfig.name()
                + " with delegation token for user '"
                + impersonatedUser
                + "'");
        metaException.initCause(e);
        throw metaException;
      }
    }

    SecurityConfig security = proxyConfig.security();
    String principal = KerberosPrincipalUtil.resolveForLocalHost(security.outboundPrincipal());
    String keytab = security.outboundKeytab();
    if (impersonatedUser != null && !impersonatedUser.isBlank()) {
      LOG.info("Connecting to backend catalog '{}' with Kerberos principal {} using keytab {} impersonating user '{}'",
          catalogConfig.name(), principal, keytab, impersonatedUser);
    } else {
      LOG.info("Connecting to backend catalog '{}' with Kerberos principal {} using keytab {}",
          catalogConfig.name(), principal, keytab);
    }

    // The process-wide UGI configuration is installed once at startup (FrontDoorSecurity.open or
    // CatalogBackend.open); replacing it here would race with in-flight SASL handshakes.
    try {
      UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
      BackendKerberosLoginTracker.processWide().record(principal, LoginSubjects.of(ugi));
      return ugi.doAs((PrivilegedExceptionAction<HiveMetaStoreClient>) () -> new HiveMetaStoreClient(conf));
    } catch (Exception e) {
      LOG.error("Failed to open backend metastore client for catalog '{}' with Kerberos principal {}",
          catalogConfig.name(), principal, e);
      MetaException metaException = new MetaException(
          "Unable to open backend metastore client for catalog "
              + catalogConfig.name()
              + " with Kerberos principal "
              + principal
              + (impersonatedUser != null ? " impersonating user '" + impersonatedUser + "'" : ""));
      metaException.initCause(e);
      throw metaException;
    }
  }

  /**
   * Extracts the thrift client, closing the freshly opened backend connection when extraction fails:
   * the caller has no other handle on it yet.
   */
  static ThriftHiveMetastore.Iface extractThriftClientOrClose(AutoCloseable client) throws MetaException {
    try {
      return extractThriftClient(client);
    } catch (Throwable t) {
      CatalogBackend.closeQuietly(client, "backend metastore client (thrift client extraction failed)");
      throw t;
    }
  }

  private static ThriftHiveMetastore.Iface extractThriftClient(AutoCloseable client)
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
