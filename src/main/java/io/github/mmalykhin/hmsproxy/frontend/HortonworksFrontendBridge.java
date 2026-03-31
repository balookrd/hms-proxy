package io.github.mmalykhin.hmsproxy.frontend;

import io.github.mmalykhin.hmsproxy.backend.MetastoreApiClassLoader;
import io.github.mmalykhin.hmsproxy.backend.MetastoreRuntimeJarResolver;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

public final class HortonworksFrontendBridge {
  private static final String THRIFT_HMS_CLASS = "org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore";
  private static final Set<String> HDP_ONLY_METHODS = Set.of(
      "truncate_table_req",
      "alter_table_req",
      "alter_partitions_req",
      "rename_partition_req",
      "update_table_column_statistics_req",
      "update_partition_column_statistics_req",
      "add_write_notification_log");

  private HortonworksFrontendBridge() {
  }

  static TProcessor createProcessor(ProxyConfig config, ThriftHiveMetastore.Iface apacheHandler) throws Exception {
    return createBridge(config, apacheHandler).processor();
  }

  static BridgeBundle createBridge(ProxyConfig config, ThriftHiveMetastore.Iface apacheHandler) throws Exception {
    Path jarPath = MetastoreRuntimeJarResolver.resolveFrontendJar(config);
    ClassLoader classLoader = new MetastoreApiClassLoader(
        new java.net.URL[] {jarPath.toUri().toURL()},
        HortonworksFrontendBridge.class.getClassLoader());
    Class<?> ifaceClass = Class.forName(THRIFT_HMS_CLASS + "$Iface", true, classLoader);
    Object handlerProxy = Proxy.newProxyInstance(
        classLoader,
        new Class<?>[] {ifaceClass},
        new BridgeInvocationHandler(classLoader, apacheHandler));
    Class<?> processorClass = Class.forName(THRIFT_HMS_CLASS + "$Processor", true, classLoader);
    Constructor<?> constructor = processorClass.getConstructor(ifaceClass);
    TProcessor processor = (TProcessor) constructor.newInstance(handlerProxy);
    return new BridgeBundle(processor, handlerProxy, ifaceClass, classLoader, jarPath);
  }

  static Set<String> supportedHdpOnlyMethods() {
    return Set.copyOf(HDP_ONLY_METHODS);
  }

  record BridgeBundle(
      TProcessor processor,
      Object handlerProxy,
      Class<?> ifaceClass,
      ClassLoader classLoader,
      Path jarPath
  ) {
  }

  private static final class BridgeInvocationHandler implements InvocationHandler {
    private final ClassLoader hdpClassLoader;
    private final ThriftHiveMetastore.Iface apacheHandler;
    private final HortonworksFrontendExtension extension;
    private final TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
    private final TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());

    private BridgeInvocationHandler(ClassLoader hdpClassLoader, ThriftHiveMetastore.Iface apacheHandler) {
      this.hdpClassLoader = hdpClassLoader;
      this.apacheHandler = apacheHandler;
      this.extension = apacheHandler instanceof HortonworksFrontendExtension hortonworksExtension
          ? hortonworksExtension
          : null;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      if (method.getDeclaringClass() == Object.class) {
        return method.invoke(this, args);
      }
      if (HDP_ONLY_METHODS.contains(method.getName())) {
        return invokeHdpOnly(method, args);
      }

      Method apacheMethod = findApacheMethod(method.getName(), method.getParameterCount());
      if (apacheMethod == null) {
        throw new TApplicationException(
            TApplicationException.UNKNOWN_METHOD,
            "Unsupported Hortonworks frontend method: " + method.getName());
      }

      Object[] convertedArgs = convertArguments(args, apacheMethod.getParameterTypes());
      try {
        Object result = apacheMethod.invoke(apacheHandler, convertedArgs);
        return convertResult(result, method.getReturnType());
      } catch (InvocationTargetException e) {
        throw e.getCause();
      }
    }

    private Object invokeHdpOnly(Method method, Object[] args) throws Throwable {
      String methodName = method.getName();
      Object request = args == null || args.length == 0 ? null : args[0];
      return switch (methodName) {
        case "truncate_table_req" -> handleTruncateTableReq(method, request);
        case "alter_table_req" -> handleAlterTableReq(method, request);
        case "alter_partitions_req" -> handleAlterPartitionsReq(method, request);
        case "rename_partition_req" -> handleRenamePartitionReq(method, request);
        case "update_table_column_statistics_req", "update_partition_column_statistics_req" ->
            handleUpdateColumnStatisticsReq(method, request);
        case "add_write_notification_log" -> handleAddWriteNotificationLog(method, request);
        default -> throw new TApplicationException(
            TApplicationException.UNKNOWN_METHOD,
            "Unsupported Hortonworks frontend method: " + methodName);
      };
    }

    private Object handleTruncateTableReq(Method method, Object request) throws Throwable {
      apacheHandler.truncate_table(
          (String) invokeNoArgs(request, "getDbName"),
          (String) invokeNoArgs(request, "getTableName"),
          stringList(invokeNoArgs(request, "getPartNames")));
      return emptyResponse(method.getReturnType());
    }

    private Object handleAlterTableReq(Method method, Object request) throws Throwable {
      String dbName = (String) invokeNoArgs(request, "getDbName");
      String tableName = (String) invokeNoArgs(request, "getTableName");
      Table table = (Table) convertTBase(invokeNoArgs(request, "getTable"), Table.class);
      EnvironmentContext environmentContext =
          (EnvironmentContext) convertIfPresent(invokeNoArgs(request, "getEnvironmentContext"), EnvironmentContext.class);
      if (environmentContext != null) {
        apacheHandler.alter_table_with_environment_context(dbName, tableName, table, environmentContext);
      } else {
        apacheHandler.alter_table(dbName, tableName, table);
      }
      return emptyResponse(method.getReturnType());
    }

    @SuppressWarnings("unchecked")
    private Object handleAlterPartitionsReq(Method method, Object request) throws Throwable {
      String dbName = (String) invokeNoArgs(request, "getDbName");
      String tableName = (String) invokeNoArgs(request, "getTableName");
      List<Partition> partitions =
          (List<Partition>) convertDynamicValue(invokeNoArgs(request, "getPartitions"), hdpClassLoader);
      EnvironmentContext environmentContext =
          (EnvironmentContext) convertIfPresent(invokeNoArgs(request, "getEnvironmentContext"), EnvironmentContext.class);
      if (environmentContext != null) {
        apacheHandler.alter_partitions_with_environment_context(dbName, tableName, partitions, environmentContext);
      } else {
        apacheHandler.alter_partitions(dbName, tableName, partitions);
      }
      return emptyResponse(method.getReturnType());
    }

    private Object handleRenamePartitionReq(Method method, Object request) throws Throwable {
      apacheHandler.rename_partition(
          (String) invokeNoArgs(request, "getDbName"),
          (String) invokeNoArgs(request, "getTableName"),
          stringList(invokeNoArgs(request, "getPartVals")),
          (Partition) convertTBase(invokeNoArgs(request, "getNewPart"), Partition.class));
      return emptyResponse(method.getReturnType());
    }

    private Object handleUpdateColumnStatisticsReq(Method method, Object request) throws Throwable {
      boolean result = apacheHandler.set_aggr_stats_for(
          (SetPartitionsStatsRequest) convertTBase(request, SetPartitionsStatsRequest.class));
      return booleanResponse(method.getReturnType(), result);
    }

    private Object handleAddWriteNotificationLog(Method method, Object request) throws Throwable {
      if (extension == null) {
        throw new TApplicationException(
            TApplicationException.UNKNOWN_METHOD,
            "Hortonworks frontend method add_write_notification_log requires proxy extension support");
      }
      Object response = extension.addWriteNotificationLog(request);
      return convertResult(response, method.getReturnType());
    }

    private Method findApacheMethod(String methodName, int argumentCount) {
      for (Method candidate : ThriftHiveMetastore.Iface.class.getMethods()) {
        if (candidate.getName().equals(methodName) && candidate.getParameterCount() == argumentCount) {
          return candidate;
        }
      }
      return null;
    }

    private Object[] convertArguments(Object[] args, Class<?>[] parameterTypes) throws Exception {
      if (args == null || args.length == 0) {
        return args;
      }
      Object[] converted = new Object[args.length];
      for (int index = 0; index < args.length; index++) {
        converted[index] = convertValue(args[index], parameterTypes[index]);
      }
      return converted;
    }

    private Object convertResult(Object result, Class<?> returnType) throws Exception {
      if (returnType == void.class || result == null) {
        return null;
      }
      return convertValue(result, returnType);
    }

    private Object convertValue(Object value, Class<?> targetType) throws Exception {
      if (value == null) {
        return null;
      }
      if (targetType.isPrimitive()
          || Number.class.isAssignableFrom(targetType)
          || targetType == Boolean.class
          || targetType == String.class) {
        return value;
      }
      if (targetType.isEnum() && value instanceof Enum<?> enumValue) {
        @SuppressWarnings({"rawtypes", "unchecked"})
        Object converted = Enum.valueOf((Class<? extends Enum>) targetType.asSubclass(Enum.class), enumValue.name());
        return converted;
      }
      if (value instanceof List<?> || value instanceof Map<?, ?>) {
        return convertDynamicValue(value, hdpClassLoader);
      }
      if (targetType.isInstance(value)) {
        return value;
      }
      if (value instanceof TBase<?, ?>) {
        return convertTBase(value, targetType);
      }
      return value;
    }

    private Object convertIfPresent(Object value, Class<?> targetType) throws Exception {
      return value == null ? null : convertValue(value, targetType);
    }

    private Object convertTBase(Object value, Class<?> targetType) throws Exception {
      if (value == null || targetType.isInstance(value)) {
        return value;
      }
      Object target = targetType.getConstructor().newInstance();
      byte[] bytes = serializer.serialize((TBase<?, ?>) value);
      deserializer.deserialize((TBase<?, ?>) target, bytes);
      return target;
    }

    private Object convertDynamicValue(Object value, ClassLoader targetClassLoader) throws Exception {
      if (value == null) {
        return null;
      }
      if (value instanceof List<?> list) {
        List<Object> converted = new ArrayList<>(list.size());
        for (Object element : list) {
          converted.add(convertDynamicValue(element, targetClassLoader));
        }
        return converted;
      }
      if (value instanceof Map<?, ?> map) {
        Map<Object, Object> converted = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
          converted.put(
              convertDynamicValue(entry.getKey(), targetClassLoader),
              convertDynamicValue(entry.getValue(), targetClassLoader));
        }
        return converted;
      }
      if (value instanceof String || value instanceof Number || value instanceof Boolean) {
        return value;
      }
      if (value.getClass().isEnum()) {
        Class<?> targetEnum = loadTargetClass(value.getClass().getName(), targetClassLoader);
        if (targetEnum != null && targetEnum.isEnum()) {
          @SuppressWarnings({"rawtypes", "unchecked"})
          Object converted = Enum.valueOf((Class<? extends Enum>) targetEnum.asSubclass(Enum.class),
              ((Enum<?>) value).name());
          return converted;
        }
      }
      if (value instanceof TBase<?, ?>) {
        Class<?> targetClass = loadTargetClass(value.getClass().getName(), targetClassLoader);
        if (targetClass != null && !targetClass.isInstance(value)) {
          return convertTBase(value, targetClass);
        }
      }
      return value;
    }

    private Object invokeNoArgs(Object target, String methodName) throws ReflectiveOperationException {
      return target.getClass().getMethod(methodName).invoke(target);
    }

    @SuppressWarnings("unchecked")
    private List<String> stringList(Object value) {
      return value == null ? List.of() : (List<String>) value;
    }

    private Object emptyResponse(Class<?> responseType) throws ReflectiveOperationException {
      return responseType.getConstructor().newInstance();
    }

    private Object booleanResponse(Class<?> responseType, boolean value) throws ReflectiveOperationException {
      try {
        return responseType.getConstructor(boolean.class).newInstance(value);
      } catch (NoSuchMethodException ignored) {
        Object response = responseType.getConstructor().newInstance();
        Method setter = responseType.getMethod("setResult", boolean.class);
        setter.invoke(response, value);
        return response;
      }
    }

    private Class<?> loadTargetClass(String className, ClassLoader targetClassLoader) {
      try {
        return Class.forName(className, true, targetClassLoader);
      } catch (ClassNotFoundException e) {
        return null;
      }
    }
  }

}
