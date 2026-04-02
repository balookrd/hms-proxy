package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.compatibility.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.thrift.TApplicationException;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class HortonworksBackendAdapterTest {
  private static final Path HDP_JAR =
      Path.of("hive-metastore", "hive-standalone-metastore-3.1.0.3.1.0.0-78.jar").toAbsolutePath();

  @Test
  public void invokeRequestUsesWrapperMethodWhenRuntimeSupportsIt() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HDP_JAR));
    AtomicReference<String> invokedMethod = new AtomicReference<>();
    AtomicReference<Object> capturedRequest = new AtomicReference<>();

    CatalogBackend backend = newIsolatedBackend((proxy, method, args) -> {
      invokedMethod.set(method.getName());
      if ("get_table_req".equals(method.getName())) {
        capturedRequest.set(args[0]);
        return childGetTableResult(proxy.getClass().getClassLoader(), "sales", "events");
      }
      throw new UnsupportedOperationException(method.getName());
    });

    try {
      GetTableRequest request = new GetTableRequest("sales", "events");
      request.setCatName("hive");

      GetTableResult result = (GetTableResult) backend.invokeRequest("get_table_req", request, null);

      Assert.assertEquals("get_table_req", invokedMethod.get());
      Assert.assertEquals("hive", capturedRequest.get().getClass().getMethod("getCatName").invoke(capturedRequest.get()));
      Assert.assertEquals("sales", result.getTable().getDbName());
      Assert.assertEquals("events", result.getTable().getTableName());
    } finally {
      backend.close();
    }
  }

  @Test
  public void invokeRequestFallsBackToLegacyMethodWhenWrapperIsUnsupported() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HDP_JAR));
    AtomicReference<String> invokedMethod = new AtomicReference<>();
    AtomicReference<String> legacyDb = new AtomicReference<>();
    AtomicReference<String> legacyTable = new AtomicReference<>();

    CatalogBackend backend = newIsolatedBackend((proxy, method, args) -> {
      invokedMethod.set(method.getName());
      if ("get_table_req".equals(method.getName())) {
        throw new TApplicationException(TApplicationException.UNKNOWN_METHOD, "unsupported");
      }
      if ("get_table".equals(method.getName())) {
        legacyDb.set((String) args[0]);
        legacyTable.set((String) args[1]);
        return childTable(proxy.getClass().getClassLoader(), (String) args[0], (String) args[1]);
      }
      throw new UnsupportedOperationException(method.getName());
    });

    try {
      GetTableRequest request = new GetTableRequest("sales", "events");
      request.setCatName("hive");

      GetTableResult result = (GetTableResult) backend.invokeRequest("get_table_req", request, null);

      Assert.assertEquals("get_table", invokedMethod.get());
      Assert.assertEquals("sales", legacyDb.get());
      Assert.assertEquals("events", legacyTable.get());
      Assert.assertEquals("sales", result.getTable().getDbName());
      Assert.assertEquals("events", result.getTable().getTableName());
    } finally {
      backend.close();
    }
  }

  @Test
  public void invokeRequestCachesUnsupportedWrapperAndSkipsSecondProbe() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HDP_JAR));
    AtomicInteger wrapperCalls = new AtomicInteger();
    AtomicInteger legacyCalls = new AtomicInteger();

    CatalogBackend backend = newIsolatedBackend((proxy, method, args) -> {
      if ("get_table_req".equals(method.getName())) {
        wrapperCalls.incrementAndGet();
        throw new TApplicationException(TApplicationException.UNKNOWN_METHOD, "unsupported");
      }
      if ("get_table".equals(method.getName())) {
        legacyCalls.incrementAndGet();
        return childTable(proxy.getClass().getClassLoader(), (String) args[0], (String) args[1]);
      }
      throw new UnsupportedOperationException(method.getName());
    });

    try {
      GetTableRequest first = new GetTableRequest("sales", "events");
      GetTableRequest second = new GetTableRequest("sales", "events");

      backend.invokeRequest("get_table_req", first, null);
      int wrapperCallsAfterFirstRequest = wrapperCalls.get();
      backend.invokeRequest("get_table_req", second, null);

      Assert.assertEquals(2, wrapperCallsAfterFirstRequest);
      Assert.assertEquals(wrapperCallsAfterFirstRequest, wrapperCalls.get());
      Assert.assertEquals(2, legacyCalls.get());
    } finally {
      backend.close();
    }
  }

  private static CatalogBackend newIsolatedBackend(InvocationHandler invocationHandler) throws Exception {
    ProxyConfig proxyConfig = config();
    ProxyConfig.CatalogConfig catalogConfig = proxyConfig.catalogs().get("catalog1");
    return newBackend(
        proxyConfig,
        catalogConfig,
        new HortonworksBackendAdapter(MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78),
        newBackendRuntime(proxyConfig, catalogConfig, invocationHandler));
  }

  private static Object childGetTableResult(ClassLoader classLoader, String dbName, String tableName) throws Exception {
    Object result = classFor(classLoader, "org.apache.hadoop.hive.metastore.api.GetTableResult")
        .getConstructor()
        .newInstance();
    Object table = childTable(classLoader, dbName, tableName);
    result.getClass().getMethod("setTable", table.getClass()).invoke(result, table);
    return result;
  }

  private static Object childTable(ClassLoader classLoader, String dbName, String tableName) throws Exception {
    Object table = classFor(classLoader, "org.apache.hadoop.hive.metastore.api.Table")
        .getConstructor()
        .newInstance();
    table.getClass().getMethod("setDbName", String.class).invoke(table, dbName);
    table.getClass().getMethod("setTableName", String.class).invoke(table, tableName);
    return table;
  }

  private static Class<?> classFor(ClassLoader classLoader, String className) throws Exception {
    return Class.forName(className, true, classLoader);
  }

  private static CatalogBackend newBackend(
      ProxyConfig proxyConfig,
      ProxyConfig.CatalogConfig catalogConfig,
      BackendAdapter adapter,
      BackendRuntime runtime
  ) throws Exception {
    Catalog catalog = new Catalog();
    catalog.setName(catalogConfig.name());
    catalog.setDescription(catalogConfig.description());
    catalog.setLocationUri(catalogConfig.locationUri());
    Constructor<CatalogBackend> ctor = CatalogBackend.class.getDeclaredConstructor(
        ProxyConfig.class,
        ProxyConfig.CatalogConfig.class,
        HiveConf.class,
        BackendAdapter.class,
        BackendRuntime.class,
        Catalog.class);
    ctor.setAccessible(true);
    return ctor.newInstance(proxyConfig, catalogConfig, new HiveConf(), adapter, runtime, catalog);
  }

  private static BackendRuntime newBackendRuntime(
      ProxyConfig proxyConfig,
      ProxyConfig.CatalogConfig catalogConfig,
      InvocationHandler invocationHandler
  ) throws Exception {
    BackendRuntime.SessionFactory sessionFactory = new BackendRuntime.SessionFactory() {
      @Override
      public BackendInvocationSession open(
          ProxyConfig ignoredProxyConfig,
          ProxyConfig.CatalogConfig ignoredCatalogConfig,
          HiveConf ignoredHiveConf,
          boolean ignoredBackendKerberosEnabled,
          MetastoreRuntimeProfile ignoredRuntimeProfile
      ) throws org.apache.hadoop.hive.metastore.api.MetaException {
        try {
          return newSession(invocationHandler);
        } catch (Exception e) {
          org.apache.hadoop.hive.metastore.api.MetaException metaException =
              new org.apache.hadoop.hive.metastore.api.MetaException("test session factory failed");
          metaException.initCause(e);
          throw metaException;
        }
      }

      @Override
      public BackendInvocationSession openImpersonating(
          ProxyConfig ignoredProxyConfig,
          ProxyConfig.CatalogConfig ignoredCatalogConfig,
          HiveConf ignoredHiveConf,
          boolean ignoredBackendKerberosEnabled,
          MetastoreRuntimeProfile ignoredRuntimeProfile,
          String ignoredUserName,
          java.util.List<String> ignoredGroupNames
      ) throws org.apache.hadoop.hive.metastore.api.MetaException {
        return open(
            ignoredProxyConfig,
            ignoredCatalogConfig,
            ignoredHiveConf,
            ignoredBackendKerberosEnabled,
            ignoredRuntimeProfile);
      }
    };

    Constructor<BackendRuntime> ctor = BackendRuntime.class.getDeclaredConstructor(
        ProxyConfig.class,
        ProxyConfig.CatalogConfig.class,
        HiveConf.class,
        boolean.class,
        BackendRuntime.SessionFactory.class,
        BackendInvocationSession.class);
    ctor.setAccessible(true);
    return ctor.newInstance(
        proxyConfig,
        catalogConfig,
        new HiveConf(),
        false,
        sessionFactory,
        newSession(invocationHandler));
  }

  private static BackendInvocationSession newSession(InvocationHandler invocationHandler) throws Exception {
    ClassLoader classLoader = new MetastoreApiClassLoader(
        new java.net.URL[] {HDP_JAR.toUri().toURL()},
        HortonworksBackendAdapterTest.class.getClassLoader());
    Class<?> ifaceClass = Class.forName("org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore$Iface", true, classLoader);
    Object delegate = Proxy.newProxyInstance(classLoader, new Class<?>[] {ifaceClass}, invocationHandler);
    IsolatedInvocationBridge bridge = new IsolatedInvocationBridge(classLoader, delegate, ifaceClass);

    Constructor<IsolatedMetastoreClient> isolatedCtor =
        IsolatedMetastoreClient.class.getDeclaredConstructor(Object.class, IsolatedInvocationBridge.class);
    isolatedCtor.setAccessible(true);
    Object closableClient = new Object() {
      @SuppressWarnings("unused")
      public void close() {
      }
    };
    IsolatedMetastoreClient isolatedClient = isolatedCtor.newInstance(closableClient, bridge);

    Constructor<BackendInvocationSession> sessionCtor = BackendInvocationSession.class.getDeclaredConstructor(
        org.apache.hadoop.hive.metastore.HiveMetaStoreClient.class,
        org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface.class,
        IsolatedMetastoreClient.class);
    sessionCtor.setAccessible(true);
    return sessionCtor.newInstance(null, null, isolatedClient);
  }

  private static ProxyConfig config() {
    return new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog1",
        Map.of("catalog1", new ProxyConfig.CatalogConfig(
            "catalog1",
            "c1",
            "file:///c1",
            false,
            ProxyConfig.CatalogAccessMode.READ_WRITE,
            java.util.List.of(),
            MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78,
            HDP_JAR.toString(),
            Map.of("hive.metastore.uris", "thrift://one"))));
  }
}
