package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.config.server.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityMode;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;

public class Hive4BackendAdapterTest {
  private static final Path HIVE_4_JAR =
      Path.of("hive-metastore", "hive-standalone-metastore-common-4.1.0.jar").toAbsolutePath();

  @Test
  public void invokeUpgradesGetTableToGetTableReq() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HIVE_4_JAR));
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
      Method getTable = ThriftHiveMetastore.Iface.class.getMethod("get_table", String.class, String.class);

      Object result = backend.invoke(getTable, new Object[]{"sales", "events"}, null);

      Assert.assertEquals("get_table_req", invokedMethod.get());
      Object request = capturedRequest.get();
      Assert.assertEquals("sales", request.getClass().getMethod("getDbName").invoke(request));
      Assert.assertEquals("events", request.getClass().getMethod("getTblName").invoke(request));
      Assert.assertTrue("expected unwrapped Table, got " + result.getClass().getName(),
          result instanceof Table);
      Assert.assertEquals("sales", ((Table) result).getDbName());
      Assert.assertEquals("events", ((Table) result).getTableName());
    } finally {
      backend.close();
    }
  }

  @Test
  public void invokeUpgradesGetTableObjectsByNameToReqVersion() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HIVE_4_JAR));
    AtomicReference<String> invokedMethod = new AtomicReference<>();
    AtomicReference<Object> capturedRequest = new AtomicReference<>();

    CatalogBackend backend = newIsolatedBackend((proxy, method, args) -> {
      invokedMethod.set(method.getName());
      if ("get_table_objects_by_name_req".equals(method.getName())) {
        capturedRequest.set(args[0]);
        return childGetTablesResult(
            proxy.getClass().getClassLoader(),
            "sales",
            List.of("events", "orders"));
      }
      throw new UnsupportedOperationException(method.getName());
    });

    try {
      Method getTableObjects = ThriftHiveMetastore.Iface.class.getMethod(
          "get_table_objects_by_name", String.class, java.util.List.class);

      Object result = backend.invoke(
          getTableObjects,
          new Object[]{"sales", List.of("events", "orders")},
          null);

      Assert.assertEquals("get_table_objects_by_name_req", invokedMethod.get());
      Object request = capturedRequest.get();
      Assert.assertEquals("sales", request.getClass().getMethod("getDbName").invoke(request));
      Object names = request.getClass().getMethod("getTblNames").invoke(request);
      Assert.assertEquals(List.of("events", "orders"), names);
      Assert.assertTrue("expected unwrapped List<Table>, got " + result.getClass().getName(),
          result instanceof List);
      @SuppressWarnings("unchecked")
      List<Table> tables = (List<Table>) result;
      Assert.assertEquals(2, tables.size());
      Assert.assertEquals("events", tables.get(0).getTableName());
      Assert.assertEquals("orders", tables.get(1).getTableName());
    } finally {
      backend.close();
    }
  }

  @Test
  public void invokeDelegatesCommonMethodWithoutTouchingIt() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HIVE_4_JAR));
    AtomicInteger callCount = new AtomicInteger();
    AtomicReference<String> invokedMethod = new AtomicReference<>();

    CatalogBackend backend = newIsolatedBackend((proxy, method, args) -> {
      callCount.incrementAndGet();
      invokedMethod.set(method.getName());
      if ("get_database".equals(method.getName())) {
        return childDatabase(proxy.getClass().getClassLoader(), (String) args[0]);
      }
      throw new UnsupportedOperationException(method.getName());
    });

    try {
      Method getDatabase = ThriftHiveMetastore.Iface.class.getMethod("get_database", String.class);

      Object result = backend.invoke(getDatabase, new Object[]{"sales"}, null);

      Assert.assertEquals(1, callCount.get());
      Assert.assertEquals("get_database", invokedMethod.get());
      Assert.assertTrue(result instanceof Database);
      Assert.assertEquals("sales", ((Database) result).getName());
    } finally {
      backend.close();
    }
  }

  @Test
  public void invokeRequestDelegatesGetTableReqUnchanged() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HIVE_4_JAR));
    AtomicReference<String> invokedMethod = new AtomicReference<>();
    AtomicReference<Object> capturedRequest = new AtomicReference<>();

    CatalogBackend backend = newIsolatedBackend((proxy, method, args) -> {
      invokedMethod.set(method.getName());
      if ("get_table_req".equals(method.getName())) {
        capturedRequest.set(args[0]);
        return childGetTableResult(proxy.getClass().getClassLoader(), "marketing", "leads");
      }
      throw new UnsupportedOperationException(method.getName());
    });

    try {
      GetTableRequest request = new GetTableRequest("marketing", "leads");
      Object result = backend.invokeRequest("get_table_req", request, null);

      Assert.assertEquals("get_table_req", invokedMethod.get());
      Assert.assertNotNull(capturedRequest.get());
      // adapter returns the response wrapper as-is when invokeRequest is used;
      // RoutingMetaStoreProxy is the layer that unwraps it for Iceberg-style callers.
      Assert.assertTrue(result.getClass().getName().endsWith("GetTableResult"));
    } finally {
      backend.close();
    }
  }

  private static CatalogBackend newIsolatedBackend(InvocationHandler invocationHandler) throws Exception {
    ProxyConfig proxyConfig = config();
    CatalogConfig catalogConfig = proxyConfig.catalogs().get("catalog1");
    return newBackend(
        proxyConfig,
        catalogConfig,
        new Hive4BackendAdapter(),
        newBackendRuntime(proxyConfig, catalogConfig, invocationHandler));
  }

  private static Object childGetTableResult(ClassLoader classLoader, String dbName, String tableName) throws Exception {
    Object table = childTable(classLoader, dbName, tableName);
    Object result = classFor(classLoader, "org.apache.hadoop.hive.metastore.api.GetTableResult")
        .getConstructor(table.getClass())
        .newInstance(table);
    return result;
  }

  private static Object childGetTablesResult(ClassLoader classLoader, String dbName, List<String> tableNames) throws Exception {
    java.util.List<Object> tables = new java.util.ArrayList<>();
    for (String tableName : tableNames) {
      tables.add(childTable(classLoader, dbName, tableName));
    }
    Object result = classFor(classLoader, "org.apache.hadoop.hive.metastore.api.GetTablesResult")
        .getConstructor(java.util.List.class)
        .newInstance(tables);
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

  private static Object childDatabase(ClassLoader classLoader, String name) throws Exception {
    Object database = classFor(classLoader, "org.apache.hadoop.hive.metastore.api.Database")
        .getConstructor()
        .newInstance();
    database.getClass().getMethod("setName", String.class).invoke(database, name);
    return database;
  }

  private static Class<?> classFor(ClassLoader classLoader, String className) throws Exception {
    return Class.forName(className, true, classLoader);
  }

  private static CatalogBackend newBackend(
      ProxyConfig proxyConfig,
      CatalogConfig catalogConfig,
      BackendAdapter adapter,
      BackendRuntime runtime
  ) throws Exception {
    Catalog catalog = new Catalog();
    catalog.setName(catalogConfig.name());
    catalog.setDescription(catalogConfig.description());
    catalog.setLocationUri(catalogConfig.locationUri());
    Constructor<CatalogBackend> ctor = CatalogBackend.class.getDeclaredConstructor(
        ProxyConfig.class,
        CatalogConfig.class,
        HiveConf.class,
        BackendAdapter.class,
        BackendRuntime.class,
        Catalog.class,
        io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics.class);
    ctor.setAccessible(true);
    return ctor.newInstance(proxyConfig, catalogConfig, new HiveConf(), adapter, runtime, catalog, null);
  }

  private static BackendRuntime newBackendRuntime(
      ProxyConfig proxyConfig,
      CatalogConfig catalogConfig,
      InvocationHandler invocationHandler
  ) throws Exception {
    BackendRuntime.SessionFactory sessionFactory = new BackendRuntime.SessionFactory() {
      @Override
      public BackendInvocationSession open(
          ProxyConfig ignoredProxyConfig,
          CatalogConfig ignoredCatalogConfig,
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
          CatalogConfig ignoredCatalogConfig,
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
        CatalogConfig.class,
        HiveConf.class,
        boolean.class,
        BackendRuntime.SessionFactory.class,
        MetastoreRuntimeProfile.class,
        BackendInvocationSession.class,
        io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics.class);
    ctor.setAccessible(true);
    MetastoreRuntimeProfile profile = catalogConfig.runtimeProfile() != null
        ? catalogConfig.runtimeProfile()
        : MetastoreRuntimeProfile.APACHE_3_1_3;
    return ctor.newInstance(
        proxyConfig,
        catalogConfig,
        new HiveConf(),
        false,
        sessionFactory,
        profile,
        newSession(invocationHandler),
        null);
  }

  private static BackendInvocationSession newSession(InvocationHandler invocationHandler) throws Exception {
    ClassLoader classLoader = new MetastoreApiClassLoader(
        new java.net.URL[]{HIVE_4_JAR.toUri().toURL()},
        Hive4BackendAdapterTest.class.getClassLoader());
    Class<?> ifaceClass = Class.forName(
        "org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore$Iface", true, classLoader);
    Object delegate = Proxy.newProxyInstance(classLoader, new Class<?>[]{ifaceClass}, invocationHandler);
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
    return ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", new CatalogConfig(
            "catalog1", "c1", "file:///c1", false,
            CatalogAccessMode.READ_WRITE, java.util.List.of(),
            MetastoreRuntimeProfile.APACHE_4_1_0, HIVE_4_JAR.toString(),
            Map.of("hive.metastore.uris", "thrift://one"))))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();
  }

  // Used to silence import-cleanup for GetTablesRequest (referenced indirectly via reflection).
  @SuppressWarnings("unused")
  private static final Class<GetTablesRequest> GET_TABLES_REQUEST_REF = GetTablesRequest.class;
}
