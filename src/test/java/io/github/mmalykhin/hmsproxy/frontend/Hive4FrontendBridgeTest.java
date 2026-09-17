package io.github.mmalykhin.hmsproxy.frontend;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TApplicationException;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.compatibility.CompatibilityConfig;
import io.github.mmalykhin.hmsproxy.config.server.FrontendProfile;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityMode;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;

public class Hive4FrontendBridgeTest {
  private static final Path HIVE_4_JAR =
      Path.of("hive-metastore", "hive-standalone-metastore-common-4.1.0.jar").toAbsolutePath();

  @Test
  public void bridgeDelegatesCommonRequestWrapperMethods() throws Exception {
    Assume.assumeTrue(Files.isReadable(HIVE_4_JAR));
    AtomicReference<String> invokedMethod = new AtomicReference<>();

    ThriftHiveMetastore.Iface apacheHandler = proxyHandler((proxy, method, args) -> {
      invokedMethod.set(method.getName());
      if ("get_table_req".equals(method.getName())) {
        org.apache.hadoop.hive.metastore.api.GetTableRequest request =
            (org.apache.hadoop.hive.metastore.api.GetTableRequest) args[0];
        Table table = new Table();
        table.setDbName(request.getDbName());
        table.setTableName(request.getTblName());
        return new GetTableResult(table);
      }
      throw new UnsupportedOperationException(method.getName());
    });

    Hive4FrontendBridge.BridgeBundle bridge =
        Hive4FrontendBridge.createBridge(config(), apacheHandler);
    Class<?> requestClass = bridge.classLoader()
        .loadClass("org.apache.hadoop.hive.metastore.api.GetTableRequest");
    Object request = requestClass.getConstructor(String.class, String.class).newInstance("sales", "events");
    Method method = bridge.ifaceClass().getMethod("get_table_req", requestClass);

    Object response = method.invoke(bridge.handlerProxy(), request);

    Assert.assertEquals("get_table_req", invokedMethod.get());
    Assert.assertEquals("org.apache.hadoop.hive.metastore.api.GetTableResult", response.getClass().getName());
    Object table = response.getClass().getMethod("getTable").invoke(response);
    Assert.assertEquals("sales", table.getClass().getMethod("getDbName").invoke(table));
    Assert.assertEquals("events", table.getClass().getMethod("getTableName").invoke(table));
  }

  @Test
  public void bridgeMapsHive4OnlyGetDatabaseReqToLegacyApacheMethod() throws Exception {
    Assume.assumeTrue(Files.isReadable(HIVE_4_JAR));
    AtomicReference<String> invokedMethod = new AtomicReference<>();
    AtomicReference<String> capturedDb = new AtomicReference<>();

    ThriftHiveMetastore.Iface apacheHandler = proxyHandler((proxy, method, args) -> {
      invokedMethod.set(method.getName());
      if ("get_database".equals(method.getName())) {
        capturedDb.set((String) args[0]);
        org.apache.hadoop.hive.metastore.api.Database db = new org.apache.hadoop.hive.metastore.api.Database();
        db.setName((String) args[0]);
        return db;
      }
      throw new UnsupportedOperationException(method.getName());
    });

    Hive4FrontendBridge.BridgeBundle bridge =
        Hive4FrontendBridge.createBridge(config(), apacheHandler);
    Class<?> requestClass = bridge.classLoader()
        .loadClass("org.apache.hadoop.hive.metastore.api.GetDatabaseRequest");
    Object request = requestClass.getConstructor().newInstance();
    requestClass.getMethod("setName", String.class).invoke(request, "sales");
    Method method = bridge.ifaceClass().getMethod("get_database_req", requestClass);

    Object response = method.invoke(bridge.handlerProxy(), request);

    Assert.assertEquals("get_database", invokedMethod.get());
    Assert.assertEquals("sales", capturedDb.get());
    Assert.assertEquals("sales", response.getClass().getMethod("getName").invoke(response));
  }

  @Test
  public void bridgeMapsHive4OnlyGetDatabasesReqWithPatternToGetDatabases() throws Exception {
    Assume.assumeTrue(Files.isReadable(HIVE_4_JAR));
    AtomicReference<String> invokedMethod = new AtomicReference<>();
    AtomicReference<String> capturedPattern = new AtomicReference<>();

    ThriftHiveMetastore.Iface apacheHandler = proxyHandler((proxy, method, args) -> {
      if ("get_databases".equals(method.getName())) {
        invokedMethod.set(method.getName());
        capturedPattern.set((String) args[0]);
        return List.of("sales", "marketing");
      }
      if ("get_database".equals(method.getName())) {
        org.apache.hadoop.hive.metastore.api.Database database =
            new org.apache.hadoop.hive.metastore.api.Database();
        database.setName((String) args[0]);
        return database;
      }
      throw new UnsupportedOperationException(method.getName());
    });

    Hive4FrontendBridge.BridgeBundle bridge =
        Hive4FrontendBridge.createBridge(config(), apacheHandler);
    Class<?> requestClass = bridge.classLoader()
        .loadClass("org.apache.hadoop.hive.metastore.api.GetDatabaseObjectsRequest");
    Object request = requestClass.getConstructor().newInstance();
    requestClass.getMethod("setPattern", String.class).invoke(request, "s*");
    Method method = bridge.ifaceClass().getMethod("get_databases_req", requestClass);

    Object response = method.invoke(bridge.handlerProxy(), request);

    Assert.assertEquals("get_databases", invokedMethod.get());
    Assert.assertEquals("s*", capturedPattern.get());
    List<?> dbs = (List<?>) response.getClass().getMethod("getDatabases").invoke(response);
    Assert.assertEquals(List.of("sales", "marketing"), databaseNames(dbs));
  }

  @Test
  public void bridgeMapsHive4OnlyGetDatabasesReqWithoutPatternFallsBackToGetAll() throws Exception {
    Assume.assumeTrue(Files.isReadable(HIVE_4_JAR));
    AtomicReference<String> invokedMethod = new AtomicReference<>();

    ThriftHiveMetastore.Iface apacheHandler = proxyHandler((proxy, method, args) -> {
      if ("get_all_databases".equals(method.getName())) {
        invokedMethod.set(method.getName());
        return List.of("sales", "marketing", "warehouse");
      }
      if ("get_database".equals(method.getName())) {
        org.apache.hadoop.hive.metastore.api.Database database =
            new org.apache.hadoop.hive.metastore.api.Database();
        database.setName((String) args[0]);
        return database;
      }
      throw new UnsupportedOperationException(method.getName());
    });

    Hive4FrontendBridge.BridgeBundle bridge =
        Hive4FrontendBridge.createBridge(config(), apacheHandler);
    Class<?> requestClass = bridge.classLoader()
        .loadClass("org.apache.hadoop.hive.metastore.api.GetDatabaseObjectsRequest");
    Object request = requestClass.getConstructor().newInstance();
    Method method = bridge.ifaceClass().getMethod("get_databases_req", requestClass);

    Object response = method.invoke(bridge.handlerProxy(), request);

    Assert.assertEquals("get_all_databases", invokedMethod.get());
    List<?> dbs = (List<?>) response.getClass().getMethod("getDatabases").invoke(response);
    Assert.assertEquals(List.of("sales", "marketing", "warehouse"), databaseNames(dbs));
  }

  @Test
  public void bridgeMapsHive4OnlyTruncateTableReqToLegacyApacheMethod() throws Exception {
    Assume.assumeTrue(Files.isReadable(HIVE_4_JAR));
    AtomicReference<String> invokedMethod = new AtomicReference<>();
    List<Object> capturedArgs = new ArrayList<>();

    ThriftHiveMetastore.Iface apacheHandler = proxyHandler((proxy, method, args) -> {
      invokedMethod.set(method.getName());
      capturedArgs.clear();
      if (args != null) {
        capturedArgs.addAll(List.of(args));
      }
      return null;
    });

    Hive4FrontendBridge.BridgeBundle bridge =
        Hive4FrontendBridge.createBridge(config(), apacheHandler);
    Class<?> requestClass = bridge.classLoader()
        .loadClass("org.apache.hadoop.hive.metastore.api.TruncateTableRequest");
    Object request = requestClass.getConstructor(String.class, String.class).newInstance("sales", "events");
    requestClass.getMethod("setPartNames", List.class).invoke(request, List.of("ds=2026-03-31"));
    Method method = bridge.ifaceClass().getMethod("truncate_table_req", requestClass);

    Object response = method.invoke(bridge.handlerProxy(), request);

    Assert.assertEquals("truncate_table", invokedMethod.get());
    Assert.assertEquals("sales", capturedArgs.get(0));
    Assert.assertEquals("events", capturedArgs.get(1));
    Assert.assertEquals(List.of("ds=2026-03-31"), capturedArgs.get(2));
    Assert.assertEquals("org.apache.hadoop.hive.metastore.api.TruncateTableResponse", response.getClass().getName());
  }

  @Test
  public void bridgeMapsHive4OnlyDropTableReqToLegacyApacheMethod() throws Exception {
    Assume.assumeTrue(Files.isReadable(HIVE_4_JAR));
    AtomicReference<String> invokedMethod = new AtomicReference<>();
    List<Object> capturedArgs = new ArrayList<>();

    ThriftHiveMetastore.Iface apacheHandler = proxyHandler((proxy, method, args) -> {
      invokedMethod.set(method.getName());
      capturedArgs.clear();
      if (args != null) {
        capturedArgs.addAll(List.of(args));
      }
      return null;
    });

    Hive4FrontendBridge.BridgeBundle bridge =
        Hive4FrontendBridge.createBridge(config(), apacheHandler);
    Class<?> requestClass = bridge.classLoader()
        .loadClass("org.apache.hadoop.hive.metastore.api.DropTableRequest");
    Object request = requestClass.getConstructor(String.class, String.class).newInstance("sales", "events");
    requestClass.getMethod("setDeleteData", boolean.class).invoke(request, true);
    Method method = bridge.ifaceClass().getMethod("drop_table_req", requestClass);

    Object response = method.invoke(bridge.handlerProxy(), request);

    Assert.assertEquals("drop_table", invokedMethod.get());
    Assert.assertEquals("sales", capturedArgs.get(0));
    Assert.assertEquals("events", capturedArgs.get(1));
    Assert.assertEquals(true, capturedArgs.get(2));
    // drop_table_req is typed as void in Hive 4 — Thrift returns no body.
    Assert.assertNull(response);
  }

  @Test
  public void bridgeRejectsHive4OnlyDataConnectorMethod() throws Exception {
    Assume.assumeTrue(Files.isReadable(HIVE_4_JAR));

    Hive4FrontendBridge.BridgeBundle bridge =
        Hive4FrontendBridge.createBridge(config(), noopHandler());
    Class<?> requestClass = bridge.classLoader()
        .loadClass("org.apache.hadoop.hive.metastore.api.GetDataConnectorRequest");
    Object request = requestClass.getConstructor(String.class).newInstance("connector1");
    Method method = bridge.ifaceClass().getMethod("get_dataconnector_req", requestClass);

    InvocationTargetException error = Assert.assertThrows(
        InvocationTargetException.class,
        () -> method.invoke(bridge.handlerProxy(), request));

    Throwable cause = error.getCause();
    Assert.assertTrue("expected TApplicationException, got " + cause.getClass().getName(),
        cause instanceof TApplicationException);
    Assert.assertEquals(TApplicationException.UNKNOWN_METHOD, ((TApplicationException) cause).getType());
    Assert.assertTrue(cause.getMessage(),
        cause.getMessage().contains("get_dataconnector_req"));
  }

  @Test
  public void bridgeDelegatesAlterTableReqToExtensionPreservingWriteState() throws Exception {
    Assume.assumeTrue(Files.isReadable(HIVE_4_JAR));
    AtomicReference<Long> capturedWriteId = new AtomicReference<>();
    AtomicReference<String> capturedValidWriteIds = new AtomicReference<>();

    ThriftHiveMetastore.Iface apacheHandler = proxyHandler((proxy, method, args) -> {
      if ("alter_table_req".equals(method.getName())) {
        Object request = args[0];
        try {
          capturedWriteId.set((Long) request.getClass().getMethod("getWriteId").invoke(request));
          capturedValidWriteIds.set((String) request.getClass().getMethod("getValidWriteIdList").invoke(request));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        return null;
      }
      throw new UnsupportedOperationException(method.getName());
    }, HortonworksFrontendExtension.class);

    Hive4FrontendBridge.BridgeBundle bridge =
        Hive4FrontendBridge.createBridge(config(), apacheHandler);
    Class<?> requestClass = bridge.classLoader().loadClass("org.apache.hadoop.hive.metastore.api.AlterTableRequest");
    Class<?> tableClass = bridge.classLoader().loadClass("org.apache.hadoop.hive.metastore.api.Table");
    Object table = tableClass.getConstructor().newInstance();
    tableClass.getMethod("setDbName", String.class).invoke(table, "sales");
    tableClass.getMethod("setTableName", String.class).invoke(table, "events");

    Object request = requestClass.getConstructor(String.class, String.class, tableClass)
        .newInstance("sales", "events", table);
    requestClass.getMethod("setWriteId", long.class).invoke(request, 101L);
    requestClass.getMethod("setValidWriteIdList", String.class).invoke(request, "sales.events:101:1::");
    Method method = bridge.ifaceClass().getMethod("alter_table_req", requestClass);

    method.invoke(bridge.handlerProxy(), request);

    Assert.assertEquals(Long.valueOf(101L), capturedWriteId.get());
    Assert.assertEquals("sales.events:101:1::", capturedValidWriteIds.get());
  }

  @Test
  public void bridgeDelegatesTruncateTableReqToExtensionPreservingWriteState() throws Exception {
    Assume.assumeTrue(Files.isReadable(HIVE_4_JAR));
    AtomicReference<Long> capturedWriteId = new AtomicReference<>();
    AtomicReference<String> capturedValidWriteIds = new AtomicReference<>();

    ThriftHiveMetastore.Iface apacheHandler = proxyHandler((proxy, method, args) -> {
      if ("truncate_table_req".equals(method.getName())) {
        Object request = args[0];
        try {
          capturedWriteId.set((Long) request.getClass().getMethod("getWriteId").invoke(request));
          capturedValidWriteIds.set((String) request.getClass().getMethod("getValidWriteIdList").invoke(request));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        return null;
      }
      throw new UnsupportedOperationException(method.getName());
    }, HortonworksFrontendExtension.class);

    Hive4FrontendBridge.BridgeBundle bridge =
        Hive4FrontendBridge.createBridge(config(), apacheHandler);
    Class<?> requestClass = bridge.classLoader().loadClass("org.apache.hadoop.hive.metastore.api.TruncateTableRequest");
    Object request = requestClass.getConstructor(String.class, String.class).newInstance("sales", "events");
    requestClass.getMethod("setWriteId", long.class).invoke(request, 55L);
    requestClass.getMethod("setValidWriteIdList", String.class).invoke(request, "sales.events:55:1::");
    Method method = bridge.ifaceClass().getMethod("truncate_table_req", requestClass);

    method.invoke(bridge.handlerProxy(), request);

    Assert.assertEquals(Long.valueOf(55L), capturedWriteId.get());
    Assert.assertEquals("sales.events:55:1::", capturedValidWriteIds.get());
  }

  @Test
  public void bridgeDelegatesGetTableStatisticsReqToExtensionPreservingWriteState() throws Exception {
    Assume.assumeTrue(Files.isReadable(HIVE_4_JAR));
    AtomicReference<String> capturedValidWriteIds = new AtomicReference<>();

    ThriftHiveMetastore.Iface apacheHandler = proxyHandler((proxy, method, args) -> {
      if ("get_table_statistics_req".equals(method.getName())) {
        Object request = args[0];
        try {
          capturedValidWriteIds.set((String) request.getClass().getMethod("getValidWriteIdList").invoke(request));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        return null;
      }
      throw new UnsupportedOperationException(method.getName());
    }, HortonworksFrontendExtension.class);

    Hive4FrontendBridge.BridgeBundle bridge =
        Hive4FrontendBridge.createBridge(config(), apacheHandler);
    Class<?> requestClass = bridge.classLoader().loadClass("org.apache.hadoop.hive.metastore.api.TableStatsRequest");
    Object request = requestClass.getConstructor(String.class, String.class, List.class).newInstance("sales", "events", List.of("id"));
    requestClass.getMethod("setValidWriteIdList", String.class).invoke(request, "sales.events:77:1::");
    Method method = bridge.ifaceClass().getMethod("get_table_statistics_req", requestClass);

    method.invoke(bridge.handlerProxy(), request);

    Assert.assertEquals("sales.events:77:1::", capturedValidWriteIds.get());
  }

  @Test
  public void bridgeDelegatesAddWriteNotificationLogInBatchToExtension() throws Exception {
    Assume.assumeTrue(Files.isReadable(HIVE_4_JAR));
    AtomicReference<Integer> capturedCount = new AtomicReference<>();

    ThriftHiveMetastore.Iface apacheHandler = proxyHandler((proxy, method, args) -> {
      if ("add_write_notification_log_in_batch".equals(method.getName())) {
        Object request = args[0];
        try {
          List<?> reqs = (List<?>) request.getClass().getMethod("getRequestList").invoke(request);
          capturedCount.set(reqs.size());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        return null;
      }
      throw new UnsupportedOperationException(method.getName());
    }, HortonworksFrontendExtension.class);

    Hive4FrontendBridge.BridgeBundle bridge =
        Hive4FrontendBridge.createBridge(config(), apacheHandler);
    Class<?> requestClass = bridge.classLoader().loadClass("org.apache.hadoop.hive.metastore.api.WriteNotificationLogBatchRequest");
    Object request = requestClass.getConstructor().newInstance();
    requestClass.getMethod("setDb", String.class).invoke(request, "sales");
    requestClass.getMethod("setTable", String.class).invoke(request, "events");
    requestClass.getMethod("setRequestList", List.class).invoke(request, List.of());
    Method method = bridge.ifaceClass().getMethod("add_write_notification_log_in_batch", requestClass);

    method.invoke(bridge.handlerProxy(), request);

    Assert.assertEquals(Integer.valueOf(0), capturedCount.get());
  }

  @Test
  public void bridgeConvertsApacheThriftExceptionsToHive4Types() throws Exception {
    Assume.assumeTrue(Files.isReadable(HIVE_4_JAR));

    ThriftHiveMetastore.Iface apacheHandler = proxyHandler((proxy, method, args) -> {
      if ("get_database".equals(method.getName())) {
        throw new org.apache.hadoop.hive.metastore.api.NoSuchObjectException("missing db");
      }
      throw new UnsupportedOperationException(method.getName());
    });

    Hive4FrontendBridge.BridgeBundle bridge =
        Hive4FrontendBridge.createBridge(config(), apacheHandler);
    Class<?> requestClass = bridge.classLoader()
        .loadClass("org.apache.hadoop.hive.metastore.api.GetDatabaseRequest");
    Object request = requestClass.getConstructor().newInstance();
    requestClass.getMethod("setName", String.class).invoke(request, "missing");
    Method method = bridge.ifaceClass().getMethod("get_database_req", requestClass);

    InvocationTargetException error = Assert.assertThrows(
        InvocationTargetException.class,
        () -> method.invoke(bridge.handlerProxy(), request));

    Throwable cause = error.getCause();
    Assert.assertEquals("org.apache.hadoop.hive.metastore.api.NoSuchObjectException",
        cause.getClass().getName());
    Assert.assertEquals("missing db", cause.getMessage());
    Assert.assertSame("exception must come from Hive 4 classloader",
        bridge.classLoader(), cause.getClass().getClassLoader());
  }

  private static List<String> databaseNames(List<?> databases) throws Exception {
    List<String> names = new ArrayList<>(databases.size());
    for (Object database : databases) {
      names.add((String) database.getClass().getMethod("getName").invoke(database));
    }
    return names;
  }

  private static ProxyConfig config() {
    return ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", new CatalogConfig(
            "catalog1", "c1", "file:///c1", false, CatalogAccessMode.READ_WRITE, java.util.List.of(),
            null, null, Map.of("hive.metastore.uris", "thrift://one"))))
        .compatibility(new CompatibilityConfig(FrontendProfile.APACHE_4_1_0, HIVE_4_JAR.toString(), null, false))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();
  }

  private static ThriftHiveMetastore.Iface proxyHandler(InvocationHandler invocationHandler, Class<?>... extraInterfaces) {
    Class<?>[] interfaces = new Class<?>[1 + extraInterfaces.length];
    interfaces[0] = ThriftHiveMetastore.Iface.class;
    System.arraycopy(extraInterfaces, 0, interfaces, 1, extraInterfaces.length);
    return (ThriftHiveMetastore.Iface) java.lang.reflect.Proxy.newProxyInstance(
        ThriftHiveMetastore.Iface.class.getClassLoader(),
        interfaces,
        invocationHandler);
  }

  private static ThriftHiveMetastore.Iface noopHandler() {
    return proxyHandler((proxy, method, args) -> {
      throw new UnsupportedOperationException(method.getName());
    });
  }
}
