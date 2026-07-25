package io.github.mmalykhin.hmsproxy.frontend;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.compatibility.CompatibilityConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityMode;
import io.github.mmalykhin.hmsproxy.config.server.FrontendProfile;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprResult;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

/**
 * Drives frontend bridge responses through the real Thrift stack: a client from the
 * isolated frontend jar writes the request, the bridge processor answers over a binary
 * protocol and the client deserializes the response. Direct handlerProxy calls skip the
 * generated write schemes, so wrong response shapes and cross-classloader value mixups
 * only surface here - and on live client connections.
 */
public class FrontendBridgeThriftSerializationTest {
  private static final String THRIFT_HMS_CLASS = "org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore";
  private static final String API_PACKAGE = "org.apache.hadoop.hive.metastore.api.";
  private static final Path HIVE_4_JAR =
      Path.of("hive-metastore", "hive-standalone-metastore-common-4.1.0.jar").toAbsolutePath();
  private static final Path HDP_6150_JAR =
      Path.of("hive-metastore", "hive-standalone-metastore-3.1.0.3.1.5.6150-1.jar").toAbsolutePath();

  @Test
  public void hive4DropPartitionReqSerializesBooleanResult() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HIVE_4_JAR));
    Hive4FrontendBridge.BridgeBundle bridge = hive4Bridge((proxy, method, args) -> {
      if ("drop_partition".equals(method.getName())) {
        return true;
      }
      throw new UnsupportedOperationException(method.getName());
    });
    Object request = newRequest(bridge.classLoader(), "DropPartitionRequest", "sales", "events");
    set(request, "setPartVals", List.class, List.of("2026-04-02"));
    set(request, "setDeleteData", boolean.class, true);

    Object result = roundTrip(bridge.processor(), bridge.classLoader(), "drop_partition_req", request);

    Assert.assertEquals(Boolean.TRUE, result);
  }

  @Test
  public void hive4GetPartitionsByFilterReqSerializesPartitionList() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HIVE_4_JAR));
    Hive4FrontendBridge.BridgeBundle bridge = hive4Bridge((proxy, method, args) -> {
      if ("get_partitions_by_filter".equals(method.getName())) {
        return List.of(apachePartition("sales", "events", "2026-04-02"));
      }
      throw new UnsupportedOperationException(method.getName());
    });
    Object request = newRequest(bridge.classLoader(), "GetPartitionsByFilterRequest", "sales", "events", "ds>'x'");

    Object result = roundTrip(bridge.processor(), bridge.classLoader(), "get_partitions_by_filter_req", request);

    List<?> partitions = (List<?>) result;
    Assert.assertEquals(1, partitions.size());
    Assert.assertEquals("events", get(partitions.get(0), "getTableName"));
  }

  @Test
  public void hive4GetPartitionNamesReqSerializesNameList() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HIVE_4_JAR));
    Hive4FrontendBridge.BridgeBundle bridge = hive4Bridge((proxy, method, args) -> {
      if ("get_partition_names".equals(method.getName())) {
        return List.of("ds=2026-04-02");
      }
      throw new UnsupportedOperationException(method.getName());
    });
    Object request = newRequest(bridge.classLoader(), "PartitionsByExprRequest");
    set(request, "setDbName", String.class, "sales");
    set(request, "setTblName", String.class, "events");
    set(request, "setExpr", byte[].class, new byte[0]);

    Object result = roundTrip(bridge.processor(), bridge.classLoader(), "get_partition_names_req", request);

    Assert.assertEquals(List.of("ds=2026-04-02"), result);
  }

  @Test
  public void hive4GetPartitionNamesReqAppliesExpressionFilter() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HIVE_4_JAR));
    AtomicReference<PartitionsByExprRequest> capturedRequest = new AtomicReference<>();
    Hive4FrontendBridge.BridgeBundle bridge = hive4Bridge((proxy, method, args) -> {
      if ("get_partitions_by_expr".equals(method.getName())) {
        capturedRequest.set((PartitionsByExprRequest) args[0]);
        return new PartitionsByExprResult(
            List.of(apachePartition("sales", "events", "2026-04-02")), false);
      }
      if ("get_table".equals(method.getName())) {
        Table table = new Table();
        table.setDbName((String) args[0]);
        table.setTableName((String) args[1]);
        table.setPartitionKeys(List.of(new FieldSchema("ds", "string", null)));
        return table;
      }
      throw new UnsupportedOperationException(method.getName());
    });
    Object request = newRequest(bridge.classLoader(), "PartitionsByExprRequest");
    set(request, "setDbName", String.class, "sales");
    set(request, "setTblName", String.class, "events");
    set(request, "setExpr", byte[].class, new byte[] {1, 2, 3});
    set(request, "setDefaultPartitionName", String.class, "__HIVE_DEFAULT_PARTITION__");

    Object result = roundTrip(bridge.processor(), bridge.classLoader(), "get_partition_names_req", request);

    Assert.assertEquals(List.of("ds=2026-04-02"), result);
    Assert.assertArrayEquals(new byte[] {1, 2, 3}, capturedRequest.get().getExpr());
    Assert.assertEquals("__HIVE_DEFAULT_PARTITION__", capturedRequest.get().getDefaultPartitionName());
  }

  @Test
  public void hive4GetPartitionsReqSerializesPartitionsResponse() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HIVE_4_JAR));
    Hive4FrontendBridge.BridgeBundle bridge = hive4Bridge((proxy, method, args) -> {
      if ("get_partitions".equals(method.getName())) {
        return List.of(apachePartition("sales", "events", "2026-04-02"));
      }
      throw new UnsupportedOperationException(method.getName());
    });
    Object request = newRequest(bridge.classLoader(), "PartitionsRequest", "sales", "events");

    Object response = roundTrip(bridge.processor(), bridge.classLoader(), "get_partitions_req", request);

    List<?> partitions = (List<?>) get(response, "getPartitions");
    Assert.assertEquals(1, partitions.size());
    Assert.assertEquals(List.of("2026-04-02"), get(partitions.get(0), "getValues"));
  }

  @Test
  public void hive4GetPartitionsByNamesReqSerializesPartitions() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HIVE_4_JAR));
    Hive4FrontendBridge.BridgeBundle bridge = hive4Bridge((proxy, method, args) -> {
      if ("get_partitions_by_names".equals(method.getName())) {
        return List.of(apachePartition("sales", "events", "2026-04-02"));
      }
      throw new UnsupportedOperationException(method.getName());
    });
    Object request = newRequest(bridge.classLoader(), "GetPartitionsByNamesRequest", "sales", "events");
    set(request, "setNames", List.class, List.of("ds=2026-04-02"));

    Object response = roundTrip(bridge.processor(), bridge.classLoader(), "get_partitions_by_names_req", request);

    List<?> partitions = (List<?>) get(response, "getPartitions");
    Assert.assertEquals(1, partitions.size());
    Assert.assertEquals("sales", get(partitions.get(0), "getDbName"));
  }

  @Test
  public void hive4GetFieldsReqSerializesFieldSchemas() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HIVE_4_JAR));
    Hive4FrontendBridge.BridgeBundle bridge = hive4Bridge((proxy, method, args) -> {
      if ("get_fields".equals(method.getName())) {
        return List.of(new FieldSchema("id", "bigint", "identifier"));
      }
      throw new UnsupportedOperationException(method.getName());
    });
    Object request = newRequest(bridge.classLoader(), "GetFieldsRequest", "sales", "events");

    Object response = roundTrip(bridge.processor(), bridge.classLoader(), "get_fields_req", request);

    List<?> fields = (List<?>) get(response, "getFields");
    Assert.assertEquals(1, fields.size());
    Assert.assertEquals("id", get(fields.get(0), "getName"));
    Assert.assertEquals("bigint", get(fields.get(0), "getType"));
  }

  @Test
  public void hive4GetPartitionReqSerializesPartition() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HIVE_4_JAR));
    Hive4FrontendBridge.BridgeBundle bridge = hive4Bridge((proxy, method, args) -> {
      if ("get_partition".equals(method.getName())) {
        return apachePartition("sales", "events", "2026-04-02");
      }
      throw new UnsupportedOperationException(method.getName());
    });
    Object request = newRequest(bridge.classLoader(), "GetPartitionRequest");
    set(request, "setDbName", String.class, "sales");
    set(request, "setTblName", String.class, "events");
    set(request, "setPartVals", List.class, List.of("2026-04-02"));

    Object response = roundTrip(bridge.processor(), bridge.classLoader(), "get_partition_req", request);

    Object partition = get(response, "getPartition");
    Assert.assertEquals("events", get(partition, "getTableName"));
  }

  @Test
  public void hive4GetDatabasesReqSerializesDatabaseObjects() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HIVE_4_JAR));
    Hive4FrontendBridge.BridgeBundle bridge = hive4Bridge((proxy, method, args) -> {
      if ("get_all_databases".equals(method.getName())) {
        return List.of("sales", "marketing");
      }
      if ("get_database".equals(method.getName())) {
        Database database = new Database();
        database.setName((String) args[0]);
        database.setLocationUri("file:///warehouse/" + args[0]);
        return database;
      }
      throw new UnsupportedOperationException(method.getName());
    });
    Object request = newRequest(bridge.classLoader(), "GetDatabaseObjectsRequest");

    Object response = roundTrip(bridge.processor(), bridge.classLoader(), "get_databases_req", request);

    List<?> databases = (List<?>) get(response, "getDatabases");
    Assert.assertEquals(2, databases.size());
    Assert.assertEquals("sales", get(databases.get(0), "getName"));
    Assert.assertEquals("file:///warehouse/marketing", get(databases.get(1), "getLocationUri"));
  }

  @Test
  public void hortonworksGetPartitionsByNamesReqSerializesPartitions() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HDP_6150_JAR));
    HortonworksFrontendBridge.BridgeBundle bridge = HortonworksFrontendBridge.createBridge(
        config(FrontendProfile.HORTONWORKS_3_1_0_3_1_5_6150_1, HDP_6150_JAR),
        proxyHandler((proxy, method, args) -> {
          if ("get_partitions_by_names".equals(method.getName())) {
            return List.of(apachePartition("sales", "events", "2026-04-02"));
          }
          throw new UnsupportedOperationException(method.getName());
        }));
    Object request = newRequest(bridge.classLoader(), "GetPartitionsByNamesRequest", "sales", "events");
    set(request, "setNames", List.class, List.of("ds=2026-04-02"));

    Object response = roundTrip(bridge.processor(), bridge.classLoader(), "get_partitions_by_names_req", request);

    List<?> partitions = (List<?>) get(response, "getPartitions");
    Assert.assertEquals(1, partitions.size());
    Assert.assertEquals("sales", get(partitions.get(0), "getDbName"));
  }

  private static Object roundTrip(TProcessor processor, ClassLoader classLoader, String methodName, Object request)
      throws Throwable {
    TMemoryBuffer requestBuffer = new TMemoryBuffer(1024);
    TMemoryBuffer responseBuffer = new TMemoryBuffer(1024);
    TProtocol requestProtocol = new TBinaryProtocol(requestBuffer);
    TProtocol responseProtocol = new TBinaryProtocol(responseBuffer);
    Class<?> clientClass = classLoader.loadClass(THRIFT_HMS_CLASS + "$Client");
    Object client = clientClass.getConstructor(TProtocol.class, TProtocol.class)
        .newInstance(responseProtocol, requestProtocol);
    try {
      clientClass.getMethod("send_" + methodName, request.getClass()).invoke(client, request);
      processor.process(requestProtocol, responseProtocol);
      return clientClass.getMethod("recv_" + methodName).invoke(client);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  private static Hive4FrontendBridge.BridgeBundle hive4Bridge(InvocationHandler apacheHandler) throws Exception {
    return Hive4FrontendBridge.createBridge(
        config(FrontendProfile.APACHE_4_1_0, HIVE_4_JAR), proxyHandler(apacheHandler));
  }

  private static Partition apachePartition(String dbName, String tableName, String value) {
    Partition partition = new Partition();
    partition.setDbName(dbName);
    partition.setTableName(tableName);
    partition.setValues(List.of(value));
    return partition;
  }

  private static Object newRequest(ClassLoader classLoader, String simpleName, String... constructorArgs)
      throws Exception {
    Class<?> requestClass = classLoader.loadClass(API_PACKAGE + simpleName);
    if (constructorArgs.length == 0) {
      return requestClass.getConstructor().newInstance();
    }
    Class<?>[] parameterTypes = new Class<?>[constructorArgs.length];
    Arrays.fill(parameterTypes, String.class);
    return requestClass.getConstructor(parameterTypes).newInstance((Object[]) constructorArgs);
  }

  private static void set(Object target, String setter, Class<?> parameterType, Object value) throws Exception {
    target.getClass().getMethod(setter, parameterType).invoke(target, value);
  }

  private static Object get(Object target, String getter) throws Exception {
    return target.getClass().getMethod(getter).invoke(target);
  }

  private static ProxyConfig config(FrontendProfile frontendProfile, Path jar) {
    return ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", new CatalogConfig(
            "catalog1", "c1", "file:///c1", false, CatalogAccessMode.READ_WRITE, List.of(),
            null, null, Map.of("hive.metastore.uris", "thrift://one"))))
        .compatibility(new CompatibilityConfig(frontendProfile, jar.toString(), null, false))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();
  }

  private static ThriftHiveMetastore.Iface proxyHandler(InvocationHandler invocationHandler) {
    return (ThriftHiveMetastore.Iface) Proxy.newProxyInstance(
        ThriftHiveMetastore.Iface.class.getClassLoader(),
        new Class<?>[] {ThriftHiveMetastore.Iface.class},
        invocationHandler);
  }
}
