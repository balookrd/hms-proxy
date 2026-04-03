package io.github.mmalykhin.hmsproxy.frontend;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class HortonworksFrontendBridgeTest {
  private static final Path HDP_78_JAR =
      Path.of("hive-metastore", "hive-standalone-metastore-3.1.0.3.1.0.0-78.jar").toAbsolutePath();
  private static final Path HDP_6150_JAR =
      Path.of("hive-metastore", "hive-standalone-metastore-3.1.0.3.1.5.6150-1.jar").toAbsolutePath();

  @Test
  public void bridgeDelegatesCommonRequestWrapperMethods() throws Exception {
    Assume.assumeTrue(Files.isReadable(HDP_78_JAR));
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

    HortonworksFrontendBridge.BridgeBundle bridge =
        HortonworksFrontendBridge.createBridge(config(ProxyConfig.FrontendProfile.HORTONWORKS_3_1_0_3_1_0_78, HDP_78_JAR), apacheHandler);
    Class<?> requestClass = bridge.classLoader().loadClass("org.apache.hadoop.hive.metastore.api.GetTableRequest");
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
  public void bridgeConvertsApacheThriftExceptionsToHdpTypes() throws Exception {
    Assume.assumeTrue(Files.isReadable(HDP_78_JAR));

    ThriftHiveMetastore.Iface apacheHandler = proxyHandler((proxy, method, args) -> {
      if ("get_table_req".equals(method.getName())) {
        throw new org.apache.hadoop.hive.metastore.api.NoSuchObjectException("missing table");
      }
      throw new UnsupportedOperationException(method.getName());
    });

    HortonworksFrontendBridge.BridgeBundle bridge =
        HortonworksFrontendBridge.createBridge(config(ProxyConfig.FrontendProfile.HORTONWORKS_3_1_0_3_1_0_78, HDP_78_JAR), apacheHandler);
    Class<?> requestClass = bridge.classLoader().loadClass("org.apache.hadoop.hive.metastore.api.GetTableRequest");
    Object request = requestClass.getConstructor(String.class, String.class).newInstance("sales", "events");
    Method method = bridge.ifaceClass().getMethod("get_table_req", requestClass);

    InvocationTargetException error = Assert.assertThrows(
        InvocationTargetException.class,
        () -> method.invoke(bridge.handlerProxy(), request));

    Assert.assertEquals("org.apache.hadoop.hive.metastore.api.NoSuchObjectException", error.getCause().getClass().getName());
    Assert.assertEquals("missing table", error.getCause().getMessage());
    Assert.assertSame(bridge.classLoader(), error.getCause().getClass().getClassLoader());
  }

  @Test
  public void bridgeMapsHdpOnlyTruncateTableReqToLegacyApacheMethod() throws Exception {
    Assume.assumeTrue(Files.isReadable(HDP_78_JAR));
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

    HortonworksFrontendBridge.BridgeBundle bridge =
        HortonworksFrontendBridge.createBridge(config(ProxyConfig.FrontendProfile.HORTONWORKS_3_1_0_3_1_0_78, HDP_78_JAR), apacheHandler);
    Class<?> requestClass = bridge.classLoader().loadClass("org.apache.hadoop.hive.metastore.api.TruncateTableRequest");
    Object request = requestClass.getConstructor(String.class, String.class).newInstance("sales", "events");
    requestClass.getMethod("setPartNames", List.class).invoke(request, List.of("ds=2026-03-31"));
    Method method = bridge.ifaceClass().getMethod("truncate_table_req", requestClass);

    Object response = method.invoke(bridge.handlerProxy(), request);

    Assert.assertEquals("truncate_table", invokedMethod.get());
    Assert.assertEquals(List.of("sales", "events", List.of("ds=2026-03-31")), capturedArgs);
    Assert.assertEquals("org.apache.hadoop.hive.metastore.api.TruncateTableResponse", response.getClass().getName());
  }

  @Test
  public void bridgeMapsHdpOnlyStatsUpdateRequestToSetAggrStatsFor() throws Exception {
    Assume.assumeTrue(Files.isReadable(HDP_78_JAR));
    AtomicReference<String> invokedMethod = new AtomicReference<>();

    ThriftHiveMetastore.Iface apacheHandler = proxyHandler((proxy, method, args) -> {
      invokedMethod.set(method.getName());
      if ("set_aggr_stats_for".equals(method.getName())) {
        Assert.assertTrue(args[0] instanceof SetPartitionsStatsRequest);
        return true;
      }
      throw new UnsupportedOperationException(method.getName());
    });

    HortonworksFrontendBridge.BridgeBundle bridge =
        HortonworksFrontendBridge.createBridge(config(ProxyConfig.FrontendProfile.HORTONWORKS_3_1_0_3_1_0_78, HDP_78_JAR), apacheHandler);
    Class<?> requestClass = bridge.classLoader().loadClass("org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest");
    Object request = requestClass.getConstructor().newInstance();
    requestClass.getMethod("setColStats", List.class).invoke(request, List.of());
    Method method = bridge.ifaceClass().getMethod("update_table_column_statistics_req", requestClass);

    Object response = method.invoke(bridge.handlerProxy(), request);

    Assert.assertEquals("set_aggr_stats_for", invokedMethod.get());
    Assert.assertEquals("org.apache.hadoop.hive.metastore.api.SetPartitionsStatsResponse", response.getClass().getName());
    Assert.assertEquals(Boolean.TRUE, response.getClass().getMethod("isResult").invoke(response));
  }

  @Test
  public void bridgeDelegatesAddWriteNotificationLogToHortonworksExtension() throws Exception {
    Assume.assumeTrue(Files.isReadable(HDP_78_JAR));
    AtomicReference<String> capturedDb = new AtomicReference<>();

    ThriftHiveMetastore.Iface apacheHandler = proxyHandler((proxy, method, args) -> {
      if ("addWriteNotificationLog".equals(method.getName())) {
        Object request = args[0];
        capturedDb.set((String) request.getClass().getMethod("getDb").invoke(request));
        return request.getClass()
            .getClassLoader()
            .loadClass("org.apache.hadoop.hive.metastore.api.WriteNotificationLogResponse")
            .getConstructor()
            .newInstance();
      }
      throw new UnsupportedOperationException(method.getName());
    }, HortonworksFrontendExtension.class);

    HortonworksFrontendBridge.BridgeBundle bridge =
        HortonworksFrontendBridge.createBridge(config(ProxyConfig.FrontendProfile.HORTONWORKS_3_1_0_3_1_0_78, HDP_78_JAR), apacheHandler);
    Class<?> requestClass =
        bridge.classLoader().loadClass("org.apache.hadoop.hive.metastore.api.WriteNotificationLogRequest");
    Class<?> fileInfoClass =
        bridge.classLoader().loadClass("org.apache.hadoop.hive.metastore.api.InsertEventRequestData");
    Object fileInfo = fileInfoClass.getConstructor().newInstance();
    fileInfoClass.getMethod("setFilesAdded", List.class).invoke(fileInfo, List.of());
    Object request = requestClass
        .getConstructor(long.class, long.class, String.class, String.class, fileInfoClass)
        .newInstance(1L, 2L, "sales", "events", fileInfo);
    Method method = bridge.ifaceClass().getMethod("add_write_notification_log", requestClass);

    Object response = method.invoke(bridge.handlerProxy(), request);

    Assert.assertEquals("sales", capturedDb.get());
    Assert.assertEquals(
        "org.apache.hadoop.hive.metastore.api.WriteNotificationLogResponse",
        response.getClass().getName());
  }

  @Test
  public void bridgeMapsGetDatabaseReqForNewerHdpRuntime() throws Exception {
    Assume.assumeTrue(Files.isReadable(HDP_6150_JAR));
    AtomicReference<String> invokedMethod = new AtomicReference<>();

    ThriftHiveMetastore.Iface apacheHandler = proxyHandler((proxy, method, args) -> {
      invokedMethod.set(method.getName());
      if ("get_database".equals(method.getName())) {
        org.apache.hadoop.hive.metastore.api.Database database =
            new org.apache.hadoop.hive.metastore.api.Database();
        database.setName((String) args[0]);
        return database;
      }
      throw new UnsupportedOperationException(method.getName());
    });

    HortonworksFrontendBridge.BridgeBundle bridge =
        HortonworksFrontendBridge.createBridge(
            config(ProxyConfig.FrontendProfile.HORTONWORKS_3_1_0_3_1_5_6150_1, HDP_6150_JAR),
            apacheHandler);
    Class<?> requestClass = bridge.classLoader().loadClass("org.apache.hadoop.hive.metastore.api.GetDatabaseRequest");
    Object request = requestClass.getConstructor().newInstance();
    requestClass.getMethod("setName", String.class).invoke(request, "sales");
    Method method = bridge.ifaceClass().getMethod("get_database_req", requestClass);

    Object response = method.invoke(bridge.handlerProxy(), request);

    Assert.assertEquals("get_database", invokedMethod.get());
    Assert.assertEquals("org.apache.hadoop.hive.metastore.api.Database", response.getClass().getName());
    Assert.assertEquals("sales", response.getClass().getMethod("getName").invoke(response));
  }

  @Test
  public void bridgeMapsCreateTableReqToCreateTableWithEnvironmentContext() throws Exception {
    Assume.assumeTrue(Files.isReadable(HDP_6150_JAR));
    AtomicReference<String> invokedMethod = new AtomicReference<>();
    AtomicReference<List<Object>> capturedArgs = new AtomicReference<>();

    ThriftHiveMetastore.Iface apacheHandler = proxyHandler((proxy, method, args) -> {
      invokedMethod.set(method.getName());
      capturedArgs.set(args == null ? List.of() : List.of(args));
      return null;
    });

    HortonworksFrontendBridge.BridgeBundle bridge =
        HortonworksFrontendBridge.createBridge(
            config(ProxyConfig.FrontendProfile.HORTONWORKS_3_1_0_3_1_5_6150_1, HDP_6150_JAR),
            apacheHandler);
    Class<?> requestClass = bridge.classLoader().loadClass("org.apache.hadoop.hive.metastore.api.CreateTableRequest");
    Class<?> tableClass = bridge.classLoader().loadClass("org.apache.hadoop.hive.metastore.api.Table");
    Class<?> envClass = bridge.classLoader().loadClass("org.apache.hadoop.hive.metastore.api.EnvironmentContext");
    Object table = tableClass.getConstructor().newInstance();
    tableClass.getMethod("setDbName", String.class).invoke(table, "sales");
    tableClass.getMethod("setTableName", String.class).invoke(table, "events");
    Object env = envClass.getConstructor().newInstance();
    Method method = bridge.ifaceClass().getMethod("create_table_req", requestClass);
    Object request = requestClass.getConstructor(tableClass).newInstance(table);
    requestClass.getMethod("setEnvContext", envClass).invoke(request, env);

    method.invoke(bridge.handlerProxy(), request);

    Assert.assertEquals("create_table_with_environment_context", invokedMethod.get());
    Assert.assertEquals("sales", ((Table) capturedArgs.get().get(0)).getDbName());
    Assert.assertEquals("events", ((Table) capturedArgs.get().get(0)).getTableName());
    Assert.assertTrue(capturedArgs.get().get(1) instanceof org.apache.hadoop.hive.metastore.api.EnvironmentContext);
  }

  @Test
  public void bridgeMapsGetPartitionsByNamesReqToLegacyMethod() throws Exception {
    Assume.assumeTrue(Files.isReadable(HDP_6150_JAR));
    AtomicReference<String> invokedMethod = new AtomicReference<>();

    ThriftHiveMetastore.Iface apacheHandler = proxyHandler((proxy, method, args) -> {
      invokedMethod.set(method.getName());
      if ("get_partitions_by_names".equals(method.getName())) {
        org.apache.hadoop.hive.metastore.api.Partition partition =
            new org.apache.hadoop.hive.metastore.api.Partition();
        partition.setDbName((String) args[0]);
        partition.setTableName((String) args[1]);
        partition.setValues(List.of("ds=2026-04-02"));
        return List.of(partition);
      }
      throw new UnsupportedOperationException(method.getName());
    });

    HortonworksFrontendBridge.BridgeBundle bridge =
        HortonworksFrontendBridge.createBridge(
            config(ProxyConfig.FrontendProfile.HORTONWORKS_3_1_0_3_1_5_6150_1, HDP_6150_JAR),
            apacheHandler);
    Class<?> requestClass =
        bridge.classLoader().loadClass("org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest");
    Object request = requestClass.getConstructor(String.class, String.class).newInstance("sales", "events");
    requestClass.getMethod("setNames", List.class).invoke(request, List.of("ds=2026-04-02"));
    Method method = bridge.ifaceClass().getMethod("get_partitions_by_names_req", requestClass);

    Object response = method.invoke(bridge.handlerProxy(), request);

    Assert.assertEquals("get_partitions_by_names", invokedMethod.get());
    Object partitions = response.getClass().getMethod("getPartitions").invoke(response);
    Assert.assertEquals(1, ((List<?>) partitions).size());
  }

  @Test
  public void bridgeDelegatesGetTablesExtToHortonworksExtension() throws Exception {
    Assume.assumeTrue(Files.isReadable(HDP_6150_JAR));
    AtomicReference<String> capturedDb = new AtomicReference<>();

    ThriftHiveMetastore.Iface apacheHandler = proxyHandler((proxy, method, args) -> {
      if ("getTablesExt".equals(method.getName())) {
        Object request = args[0];
        capturedDb.set((String) request.getClass().getMethod("getDatabase").invoke(request));
        Class<?> infoClass = request.getClass().getClassLoader()
            .loadClass("org.apache.hadoop.hive.metastore.api.ExtendedTableInfo");
        Object info = infoClass.getConstructor(String.class).newInstance("events");
        return List.of(info);
      }
      throw new UnsupportedOperationException(method.getName());
    }, HortonworksFrontendExtension.class);

    HortonworksFrontendBridge.BridgeBundle bridge =
        HortonworksFrontendBridge.createBridge(
            config(ProxyConfig.FrontendProfile.HORTONWORKS_3_1_0_3_1_5_6150_1, HDP_6150_JAR),
            apacheHandler);
    Class<?> requestClass = bridge.classLoader().loadClass("org.apache.hadoop.hive.metastore.api.GetTablesExtRequest");
    Object request = requestClass.getConstructor(String.class, String.class, String.class, int.class)
        .newInstance("catalog1", "sales", "*", 1);
    Method method = bridge.ifaceClass().getMethod("get_tables_ext", requestClass);

    Object response = method.invoke(bridge.handlerProxy(), request);

    Assert.assertEquals("sales", capturedDb.get());
    Assert.assertEquals(1, ((List<?>) response).size());
  }

  @Test
  public void bridgeDelegatesGetAllMaterializedViewsToHortonworksExtension() throws Exception {
    Assume.assumeTrue(Files.isReadable(HDP_6150_JAR));

    ThriftHiveMetastore.Iface apacheHandler = proxyHandler((proxy, method, args) -> {
      if ("getAllMaterializedViewObjectsForRewriting".equals(method.getName())) {
        Table table = new Table();
        table.setDbName("sales");
        table.setTableName("mv_events");
        return List.of(table);
      }
      throw new UnsupportedOperationException(method.getName());
    }, HortonworksFrontendExtension.class);

    HortonworksFrontendBridge.BridgeBundle bridge =
        HortonworksFrontendBridge.createBridge(
            config(ProxyConfig.FrontendProfile.HORTONWORKS_3_1_0_3_1_5_6150_1, HDP_6150_JAR),
            apacheHandler);
    Method method = bridge.ifaceClass().getMethod("get_all_materialized_view_objects_for_rewriting");

    Object response = method.invoke(bridge.handlerProxy());

    Assert.assertEquals(1, ((List<?>) response).size());
  }

  @Test
  public void bridgeCoversAllHortonworksOnlyIfaceMethodsForLegacyRuntime() throws Exception {
    Assume.assumeTrue(Files.isReadable(HDP_78_JAR));

    HortonworksFrontendBridge.BridgeBundle bridge =
        HortonworksFrontendBridge.createBridge(
            config(ProxyConfig.FrontendProfile.HORTONWORKS_3_1_0_3_1_0_78, HDP_78_JAR),
            noopHandler());

    Set<String> apacheMethods = new HashSet<>();
    for (Method method : ThriftHiveMetastore.Iface.class.getMethods()) {
      apacheMethods.add(signature(method));
    }

    Set<String> hortonworksOnlyMethods = new HashSet<>();
    for (Method method : bridge.ifaceClass().getMethods()) {
      if (!apacheMethods.contains(signature(method))) {
        hortonworksOnlyMethods.add(method.getName());
      }
    }

    Assert.assertEquals(
        Set.of(
            "truncate_table_req",
            "alter_table_req",
            "alter_partitions_req",
            "rename_partition_req",
            "update_table_column_statistics_req",
            "update_partition_column_statistics_req",
            "add_write_notification_log"),
        hortonworksOnlyMethods);
    Assert.assertEquals(hortonworksOnlyMethods, HortonworksFrontendBridge.supportedHdpOnlyMethods(bridge.ifaceClass()));
  }

  @Test
  public void bridgeCoversAllHortonworksOnlyIfaceMethodsFor6150Runtime() throws Exception {
    Assume.assumeTrue(Files.isReadable(HDP_6150_JAR));

    HortonworksFrontendBridge.BridgeBundle bridge =
        HortonworksFrontendBridge.createBridge(
            config(ProxyConfig.FrontendProfile.HORTONWORKS_3_1_0_3_1_5_6150_1, HDP_6150_JAR),
            noopHandler());

    Set<String> apacheMethods = new HashSet<>();
    for (Method method : ThriftHiveMetastore.Iface.class.getMethods()) {
      apacheMethods.add(signature(method));
    }

    Set<String> hortonworksOnlyMethods = new HashSet<>();
    for (Method method : bridge.ifaceClass().getMethods()) {
      if (!apacheMethods.contains(signature(method))) {
        hortonworksOnlyMethods.add(method.getName());
      }
    }

    Set<String> adaptedMethods = HortonworksFrontendBridge.supportedHdpOnlyMethods(bridge.ifaceClass());
    Assert.assertEquals(
        Set.of(
            "get_database_req",
            "create_table_req",
            "truncate_table_req",
            "alter_table_req",
            "alter_partitions_req",
            "rename_partition_req",
            "update_table_column_statistics_req",
            "update_partition_column_statistics_req",
            "add_write_notification_log",
            "get_partitions_by_names_req",
            "get_tables_ext",
            "get_all_materialized_view_objects_for_rewriting"),
        adaptedMethods);
    Assert.assertTrue(hortonworksOnlyMethods.containsAll(adaptedMethods));
  }

  private static ProxyConfig config(ProxyConfig.FrontendProfile frontendProfile, Path jar) {
    return new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog1",
        Map.of("catalog1", new ProxyConfig.CatalogConfig(
            "catalog1", "c1", "file:///c1", false, ProxyConfig.CatalogAccessMode.READ_WRITE, java.util.List.of(),
            null, null, Map.of("hive.metastore.uris", "thrift://one"))),
        new ProxyConfig.CompatibilityConfig(
            frontendProfile,
            jar.toString(),
            null,
            false));
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

  private static String signature(Method method) {
    return method.getName() + Arrays.toString(method.getParameterTypes());
  }
}
