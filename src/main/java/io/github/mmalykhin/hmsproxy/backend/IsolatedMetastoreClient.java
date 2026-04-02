package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.compatibility.MetastoreRuntimeProfile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

public final class IsolatedMetastoreClient implements AutoCloseable {
  private static final String THRIFT_HMS_CLASS = "org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore";
  private static final String HIVE_METASTORE_CLIENT_CLASS = "org.apache.hadoop.hive.metastore.HiveMetaStoreClient";
  private static final String HIVE_METASTORE_URI_SELECTION_KEY = "hive.metastore.uri.selection";
  private static final String HIVE_METASTORE_URI_SELECTION_SEQUENTIAL = "SEQUENTIAL";

  private final Object client;
  private final IsolatedInvocationBridge bridge;

  private IsolatedMetastoreClient(
      Object client,
      IsolatedInvocationBridge bridge
  ) {
    this.client = client;
    this.bridge = bridge;
  }

  static IsolatedMetastoreClient open(
      ProxyConfig config,
      ProxyConfig.CatalogConfig catalogConfig,
      MetastoreRuntimeProfile runtimeProfile,
      Configuration conf
  ) throws Exception {
    return open(config, catalogConfig, runtimeProfile, null, null, conf);
  }

  static IsolatedMetastoreClient open(
      ProxyConfig config,
      ProxyConfig.CatalogConfig catalogConfig,
      MetastoreRuntimeProfile runtimeProfile,
      String principal,
      String keytab,
      Configuration conf
  ) throws Exception {
    Path jarPath = MetastoreRuntimeJarResolver.resolveBackendJar(config, catalogConfig, runtimeProfile);
    ClassLoader classLoader = new MetastoreApiClassLoader(
        MetastoreApiClassLoader.buildIsolatedRuntimeUrls(jarPath),
        IsolatedMetastoreClient.class.getClassLoader());
    Class<?> childConfigurationClass = Class.forName("org.apache.hadoop.conf.Configuration", true, classLoader);
    Object isolatedConf = withContextClassLoader(classLoader, () -> childConfigurationClass.getConstructor(boolean.class)
        .newInstance(false));
    Method setClassLoader = childConfigurationClass.getMethod("setClassLoader", ClassLoader.class);
    setClassLoader.invoke(isolatedConf, classLoader);
    Method set = childConfigurationClass.getMethod("set", String.class, String.class);
    for (Map.Entry<String, String> entry : conf) {
      set.invoke(isolatedConf, entry.getKey(), entry.getValue());
    }
    applyHortonworksCompatibilityWorkarounds(isolatedConf, childConfigurationClass, runtimeProfile);
    Class<?> clientClass = Class.forName(HIVE_METASTORE_CLIENT_CLASS, true, classLoader);
    Object client = principal == null || keytab == null
        ? withContextClassLoader(classLoader, () ->
            clientClass.getConstructor(childConfigurationClass).newInstance(isolatedConf))
        : loginAndOpenClient(classLoader, clientClass, childConfigurationClass, isolatedConf, principal, keytab);
    Field clientField = clientClass.getDeclaredField("client");
    clientField.setAccessible(true);
    Object thriftClient = clientField.get(client);
    Class<?> ifaceClass = Class.forName(THRIFT_HMS_CLASS + "$Iface", true, classLoader);
    return new IsolatedMetastoreClient(client, new IsolatedInvocationBridge(classLoader, thriftClient, ifaceClass));
  }

  void setUgi(String userName, List<String> groupNames) throws Throwable {
    bridge.setUgi(userName, groupNames);
  }

  Object invoke(java.lang.reflect.Method method, Object[] args) throws Throwable {
    return bridge.invoke(method, args);
  }

  Object invokeByName(String methodName, Class<?>[] parameterTypes, Object[] args) throws Throwable {
    return bridge.invokeByName(methodName, parameterTypes, args);
  }

  @Override
  public void close() throws Exception {
    withContextClassLoader(client.getClass().getClassLoader(), () -> {
      client.getClass().getMethod("close").invoke(client);
      return null;
    });
  }

  private static <T> T withContextClassLoader(ClassLoader classLoader, ThrowingSupplier<T> supplier) throws Exception {
    Thread thread = Thread.currentThread();
    ClassLoader previous = thread.getContextClassLoader();
    thread.setContextClassLoader(classLoader);
    try {
      return supplier.get();
    } finally {
      thread.setContextClassLoader(previous);
    }
  }

  static void applyHortonworksCompatibilityWorkarounds(
      Object isolatedConf,
      Class<?> childConfigurationClass,
      MetastoreRuntimeProfile runtimeProfile
  ) throws ReflectiveOperationException {
    if (runtimeProfile != MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78) {
      return;
    }

    Method set = childConfigurationClass.getMethod("set", String.class, String.class);
    set.invoke(isolatedConf, HIVE_METASTORE_URI_SELECTION_KEY, HIVE_METASTORE_URI_SELECTION_SEQUENTIAL);
  }

  private static Object loginAndOpenClient(
      ClassLoader classLoader,
      Class<?> clientClass,
      Class<?> childConfigurationClass,
      Object isolatedConf,
      String principal,
      String keytab
  ) throws Exception {
    Class<?> childUgiClass = Class.forName("org.apache.hadoop.security.UserGroupInformation", true, classLoader);
    Method set = childConfigurationClass.getMethod("set", String.class, String.class);
    set.invoke(isolatedConf, "hadoop.security.authentication", "kerberos");
    Method setConfiguration = childUgiClass.getMethod("setConfiguration", childConfigurationClass);
    setConfiguration.invoke(null, isolatedConf);
    Method loginUserFromKeytabAndReturnUgi =
        childUgiClass.getMethod("loginUserFromKeytabAndReturnUGI", String.class, String.class);
    Object childUgi = loginUserFromKeytabAndReturnUgi.invoke(null, principal, keytab);
    Method doAs = childUgiClass.getMethod("doAs", java.security.PrivilegedExceptionAction.class);
    return doAs.invoke(childUgi, (java.security.PrivilegedExceptionAction<Object>) () ->
        withContextClassLoader(classLoader, () ->
            clientClass.getConstructor(childConfigurationClass).newInstance(isolatedConf)));
  }

  @FunctionalInterface
  private interface ThrowingSupplier<T> {
    T get() throws Exception;
  }
}
