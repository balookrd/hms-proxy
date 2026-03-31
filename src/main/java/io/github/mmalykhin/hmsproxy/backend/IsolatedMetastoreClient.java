package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.compatibility.MetastoreRuntimeProfile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

public final class IsolatedMetastoreClient implements AutoCloseable {
  private static final String THRIFT_HMS_CLASS = "org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore";
  private static final String HIVE_METASTORE_CLIENT_CLASS = "org.apache.hadoop.hive.metastore.HiveMetaStoreClient";

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
    Path jarPath = MetastoreRuntimeJarResolver.resolveBackendJar(config, catalogConfig, runtimeProfile);
    ClassLoader classLoader = new MetastoreApiClassLoader(
        new URL[] {
            jarPath.toUri().toURL(),
            Configuration.class.getProtectionDomain().getCodeSource().getLocation()
        },
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
    Class<?> clientClass = Class.forName(HIVE_METASTORE_CLIENT_CLASS, true, classLoader);
    Object client = withContextClassLoader(classLoader, () ->
        clientClass.getConstructor(childConfigurationClass).newInstance(isolatedConf));
    Field clientField = clientClass.getDeclaredField("client");
    clientField.setAccessible(true);
    Object thriftClient = clientField.get(client);
    Class<?> ifaceClass = Class.forName(THRIFT_HMS_CLASS + "$Iface", true, classLoader);
    return new IsolatedMetastoreClient(client, new IsolatedInvocationBridge(classLoader, thriftClient, ifaceClass));
  }

  String getVersion() throws Throwable {
    return bridge.getVersion();
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

  @FunctionalInterface
  private interface ThrowingSupplier<T> {
    T get() throws Exception;
  }
}
