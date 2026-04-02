package io.github.mmalykhin.hmsproxy.backend;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class IsolatedInvocationBridgeTest {
  private static final Path HDP_JAR =
      Path.of("hive-metastore", "hive-standalone-metastore-3.1.0.3.1.0.0-78.jar").toAbsolutePath();

  @Test
  public void invokeConvertsApacheRequestAndResponseForRequestWrapperMethods() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HDP_JAR));
    AtomicReference<Object> capturedRequest = new AtomicReference<>();

    BridgeBundle bundle = createBridge((proxy, method, args) -> {
      if ("get_table_req".equals(method.getName())) {
        capturedRequest.set(args[0]);
        Object response = bundleResponseClass(proxy.getClass().getClassLoader(), "org.apache.hadoop.hive.metastore.api.GetTableResult")
            .getConstructor()
            .newInstance();
        Object table = bundleResponseClass(proxy.getClass().getClassLoader(), "org.apache.hadoop.hive.metastore.api.Table")
            .getConstructor()
            .newInstance();
        table.getClass().getMethod("setDbName", String.class).invoke(table, "sales");
        table.getClass().getMethod("setTableName", String.class).invoke(table, "events");
        response.getClass().getMethod("setTable", table.getClass()).invoke(response, table);
        return response;
      }
      throw new UnsupportedOperationException(method.getName());
    });

    Method method = ThriftHiveMetastore.Iface.class.getMethod("get_table_req", GetTableRequest.class);
    GetTableResult result = (GetTableResult) bundle.bridge.invoke(method, new Object[] {new GetTableRequest("sales", "events")});

    Assert.assertEquals("org.apache.hadoop.hive.metastore.api.GetTableRequest", capturedRequest.get().getClass().getName());
    Assert.assertEquals("sales", capturedRequest.get().getClass().getMethod("getDbName").invoke(capturedRequest.get()));
    Assert.assertEquals("events", result.getTable().getTableName());
  }

  @Test
  public void invokeByNameConvertsLegacyTableResponseToApacheObjects() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HDP_JAR));

    BridgeBundle bundle = createBridge((proxy, method, args) -> {
      if ("get_table".equals(method.getName())) {
        Object table = bundleResponseClass(proxy.getClass().getClassLoader(), "org.apache.hadoop.hive.metastore.api.Table")
            .getConstructor()
            .newInstance();
        table.getClass().getMethod("setDbName", String.class).invoke(table, "sales");
        table.getClass().getMethod("setTableName", String.class).invoke(table, "events");
        return table;
      }
      throw new UnsupportedOperationException(method.getName());
    });

    Object result = bundle.bridge.invokeByName(
        "get_table",
        new Class<?>[] {String.class, String.class},
        new Object[] {"sales", "events"});

    Assert.assertTrue(result instanceof Table);
    Assert.assertEquals("sales", ((Table) result).getDbName());
    Assert.assertEquals("events", ((Table) result).getTableName());
  }

  @Test
  public void invokeByNameConvertsApacheRequestWrapperArguments() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HDP_JAR));
    AtomicReference<Object> capturedRequest = new AtomicReference<>();

    BridgeBundle bundle = createBridge((proxy, method, args) -> {
      if ("get_table_req".equals(method.getName())) {
        capturedRequest.set(args[0]);
        Object response =
            bundleResponseClass(proxy.getClass().getClassLoader(), "org.apache.hadoop.hive.metastore.api.GetTableResult")
                .getConstructor()
                .newInstance();
        Object table = bundleResponseClass(proxy.getClass().getClassLoader(), "org.apache.hadoop.hive.metastore.api.Table")
            .getConstructor()
            .newInstance();
        table.getClass().getMethod("setDbName", String.class).invoke(table, "sales");
        table.getClass().getMethod("setTableName", String.class).invoke(table, "events");
        response.getClass().getMethod("setTable", table.getClass()).invoke(response, table);
        return response;
      }
      throw new UnsupportedOperationException(method.getName());
    });

    GetTableRequest request = new GetTableRequest("sales", "events");
    request.setCatName("hive");
    GetTableResult result = (GetTableResult) bundle.bridge.invokeByName(
        "get_table_req",
        new Class<?>[] {GetTableRequest.class},
        new Object[] {request});

    Assert.assertEquals("org.apache.hadoop.hive.metastore.api.GetTableRequest", capturedRequest.get().getClass().getName());
    Assert.assertEquals("sales", capturedRequest.get().getClass().getMethod("getDbName").invoke(capturedRequest.get()));
    Assert.assertEquals("hive", capturedRequest.get().getClass().getMethod("getCatName").invoke(capturedRequest.get()));
    Assert.assertEquals("events", result.getTable().getTableName());
  }

  @Test
  public void invokeByNameConvertsChildThriftExceptionsToApacheTypes() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HDP_JAR));

    BridgeBundle bundle = createBridge((proxy, method, args) -> {
      if ("get_table".equals(method.getName())) {
        throw (Throwable) bundleResponseClass(
            proxy.getClass().getClassLoader(),
            "org.apache.hadoop.hive.metastore.api.NoSuchObjectException")
            .getConstructor(String.class)
            .newInstance("missing table");
      }
      throw new UnsupportedOperationException(method.getName());
    });

    NoSuchObjectException error = Assert.assertThrows(
        NoSuchObjectException.class,
        () -> bundle.bridge.invokeByName(
            "get_table",
            new Class<?>[] {String.class, String.class},
            new Object[] {"sales", "events"}));

    Assert.assertEquals("missing table", error.getMessage());
  }

  @Test
  public void setUgiPassesSimpleArgumentsWithoutThriftConversion() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HDP_JAR));
    AtomicReference<List<?>> capturedGroups = new AtomicReference<>();

    BridgeBundle bundle = createBridge((proxy, method, args) -> {
      if ("set_ugi".equals(method.getName())) {
        capturedGroups.set((List<?>) args[1]);
        return List.of("dev", "analytics");
      }
      throw new UnsupportedOperationException(method.getName());
    });

    bundle.bridge.setUgi("alice", List.of("dev", "analytics"));

    Assert.assertEquals(List.of("dev", "analytics"), capturedGroups.get());
  }

  private static BridgeBundle createBridge(InvocationHandler invocationHandler) throws Exception {
    ClassLoader classLoader = new MetastoreApiClassLoader(
        new java.net.URL[] {HDP_JAR.toUri().toURL()},
        IsolatedInvocationBridgeTest.class.getClassLoader());
    Class<?> ifaceClass = classLoader.loadClass("org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore$Iface");
    Object delegate = Proxy.newProxyInstance(classLoader, new Class<?>[] {ifaceClass}, invocationHandler);
    return new BridgeBundle(new IsolatedInvocationBridge(classLoader, delegate, ifaceClass), classLoader);
  }

  private static Class<?> bundleResponseClass(ClassLoader classLoader, String className) throws Exception {
    return Class.forName(className, true, classLoader);
  }

  private record BridgeBundle(IsolatedInvocationBridge bridge, ClassLoader classLoader) {
  }
}
