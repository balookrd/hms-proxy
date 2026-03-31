package io.github.mmalykhin.hmsproxy;

import java.lang.reflect.InvocationHandler;
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
  private static final Path HDP_JAR =
      Path.of("hive-metastore", "hive-standalone-metastore-3.1.0.3.1.0.0-78.jar").toAbsolutePath();

  @Test
  public void bridgeDelegatesCommonRequestWrapperMethods() throws Exception {
    Assume.assumeTrue(Files.isReadable(HDP_JAR));
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

    HortonworksFrontendBridge.BridgeBundle bridge = HortonworksFrontendBridge.createBridge(config(), apacheHandler);
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
  public void bridgeMapsHdpOnlyTruncateTableReqToLegacyApacheMethod() throws Exception {
    Assume.assumeTrue(Files.isReadable(HDP_JAR));
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

    HortonworksFrontendBridge.BridgeBundle bridge = HortonworksFrontendBridge.createBridge(config(), apacheHandler);
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
    Assume.assumeTrue(Files.isReadable(HDP_JAR));
    AtomicReference<String> invokedMethod = new AtomicReference<>();

    ThriftHiveMetastore.Iface apacheHandler = proxyHandler((proxy, method, args) -> {
      invokedMethod.set(method.getName());
      if ("set_aggr_stats_for".equals(method.getName())) {
        Assert.assertTrue(args[0] instanceof SetPartitionsStatsRequest);
        return true;
      }
      throw new UnsupportedOperationException(method.getName());
    });

    HortonworksFrontendBridge.BridgeBundle bridge = HortonworksFrontendBridge.createBridge(config(), apacheHandler);
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
    Assume.assumeTrue(Files.isReadable(HDP_JAR));
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

    HortonworksFrontendBridge.BridgeBundle bridge = HortonworksFrontendBridge.createBridge(config(), apacheHandler);
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
  public void bridgeCoversAllHortonworksOnlyIfaceMethods() throws Exception {
    Assume.assumeTrue(Files.isReadable(HDP_JAR));

    HortonworksFrontendBridge.BridgeBundle bridge =
        HortonworksFrontendBridge.createBridge(config(), noopHandler());

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
    Assert.assertEquals(hortonworksOnlyMethods, HortonworksFrontendBridge.supportedHdpOnlyMethods());
  }

  private static ProxyConfig config() {
    return new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog1",
        Map.of("catalog1", new ProxyConfig.CatalogConfig("catalog1", "c1", "file:///c1", false,
            null, null, Map.of("hive.metastore.uris", "thrift://one"))),
        new ProxyConfig.CompatibilityConfig(
            ProxyConfig.FrontendProfile.HORTONWORKS_3_1_0_3_1_0_78,
            HDP_JAR.toString(),
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
