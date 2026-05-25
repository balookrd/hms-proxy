package io.github.mmalykhin.hmsproxy.frontend;

import io.github.mmalykhin.hmsproxy.backend.MetastoreApiClassLoader;
import io.github.mmalykhin.hmsproxy.backend.MetastoreRuntimeJarResolver;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftValueConverter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TProcessor;

/**
 * Front-door bridge that accepts Hive 4.1.x Thrift clients and serves them
 * against an Apache Hive 3.1.3 handler. Symmetric to HortonworksFrontendBridge:
 * the Hive 4 ThriftHiveMetastore API is loaded in an isolated classloader, a
 * dynamic Proxy is created to satisfy that API, and each invocation is either
 * routed to a matching Apache 3.1.3 method via {@link ThriftValueConverter}
 * (works because Thrift binary serialization is backward/forward compatible
 * for the shared 199 methods) or explicitly mapped onto a positional Apache
 * method for the Hive 4-only request-wrapper variants.
 *
 * Out of scope (responds with TApplicationException.UNKNOWN_METHOD): data
 * connectors, scheduled queries, stored procedures, packages, ACID v2-only
 * compaction/replication APIs. These have no safe Apache 3.1.3 backend mapping.
 */
public final class Hive4FrontendBridge {
  private static final String THRIFT_HMS_CLASS = "org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore";

  /**
   * Hive 4-only request-wrapper methods we explicitly bridge to positional
   * Apache 3.1.3 APIs. Anything in this set takes the explicit handler path;
   * everything else falls through to generic delegation by method name.
   */
  private static final Set<String> HIVE4_REQUEST_WRAPPERS = Set.of(
      "get_database_req",
      "get_databases_req",
      "create_table_req",
      "drop_table_req",
      "alter_table_req",
      "truncate_table_req",
      "alter_partitions_req",
      "rename_partition_req",
      "drop_partition_req",
      "get_partition_req",
      "get_partitions_req",
      "get_partitions_by_names_req",
      "get_partitions_by_filter_req",
      "get_partition_names_req",
      "get_fields_req",
      "update_table_column_statistics_req",
      "update_partition_column_statistics_req",
      "add_write_notification_log",
      "get_tables_ext",
      "get_all_materialized_view_objects_for_rewriting");

  private Hive4FrontendBridge() {
  }

  static TProcessor createProcessor(ProxyConfig config, ThriftHiveMetastore.Iface apacheHandler) throws Exception {
    return createBridge(config, apacheHandler).processor();
  }

  static BridgeBundle createBridge(ProxyConfig config, ThriftHiveMetastore.Iface apacheHandler) throws Exception {
    Path jarPath = MetastoreRuntimeJarResolver.resolveFrontendJar(config);
    ClassLoader classLoader = new MetastoreApiClassLoader(
        MetastoreApiClassLoader.buildIsolatedRuntimeUrls(jarPath),
        Hive4FrontendBridge.class.getClassLoader());
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

  record BridgeBundle(
      TProcessor processor,
      Object handlerProxy,
      Class<?> ifaceClass,
      ClassLoader classLoader,
      Path jarPath
  ) {
  }

  private static final class BridgeInvocationHandler implements InvocationHandler {
    private final ClassLoader hive4ClassLoader;
    private final ThriftHiveMetastore.Iface apacheHandler;
    private final HortonworksFrontendExtension extension;

    private BridgeInvocationHandler(ClassLoader hive4ClassLoader, ThriftHiveMetastore.Iface apacheHandler) {
      this.hive4ClassLoader = hive4ClassLoader;
      this.apacheHandler = apacheHandler;
      // The HDP extension interface fits the three Hive 4 methods we care about
      // (add_write_notification_log, get_tables_ext, get_all_materialized_view_objects_for_rewriting),
      // so reuse it instead of defining a parallel Hive 4 marker.
      this.extension = apacheHandler instanceof HortonworksFrontendExtension hortonworksExtension
          ? hortonworksExtension
          : null;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      if (method.getDeclaringClass() == Object.class) {
        return method.invoke(this, args);
      }
      if (HIVE4_REQUEST_WRAPPERS.contains(method.getName())) {
        try {
          return invokeHive4Wrapper(method, args);
        } catch (Throwable t) {
          throw ThriftValueConverter.convertThrowable(t, hive4ClassLoader);
        }
      }

      Method apacheMethod = findApacheMethod(method.getName(), method.getParameterCount());
      if (apacheMethod == null) {
        throw new TApplicationException(
            TApplicationException.UNKNOWN_METHOD,
            "Hive 4 method has no Apache 3.1.3 backend mapping: " + method.getName());
      }

      Object[] convertedArgs = convertArguments(args, apacheMethod.getParameterTypes());
      try {
        Object result = apacheMethod.invoke(apacheHandler, convertedArgs);
        return convertResult(result, method.getReturnType());
      } catch (InvocationTargetException e) {
        throw ThriftValueConverter.convertThrowable(e.getCause(), hive4ClassLoader);
      }
    }

    private Object invokeHive4Wrapper(Method method, Object[] args) throws Throwable {
      String methodName = method.getName();
      Object request = args == null || args.length == 0 ? null : args[0];
      return switch (methodName) {
        case "get_database_req" -> handleGetDatabaseReq(method, request);
        case "get_databases_req" -> handleGetDatabasesReq(method, request);
        case "create_table_req" -> handleCreateTableReq(method, request);
        case "drop_table_req" -> handleDropTableReq(method, request);
        case "alter_table_req" -> handleAlterTableReq(method, request);
        case "truncate_table_req" -> handleTruncateTableReq(method, request);
        case "alter_partitions_req" -> handleAlterPartitionsReq(method, request);
        case "rename_partition_req" -> handleRenamePartitionReq(method, request);
        case "drop_partition_req" -> handleDropPartitionReq(method, request);
        case "get_partition_req" -> handleGetPartitionReq(method, request);
        case "get_partitions_req" -> handleGetPartitionsReq(method, request);
        case "get_partitions_by_names_req" -> handleGetPartitionsByNamesReq(method, request);
        case "get_partitions_by_filter_req" -> handleGetPartitionsByFilterReq(method, request);
        case "get_partition_names_req" -> handleGetPartitionNamesReq(method, request);
        case "get_fields_req" -> handleGetFieldsReq(method, request);
        case "update_table_column_statistics_req", "update_partition_column_statistics_req" ->
            handleUpdateColumnStatisticsReq(method, request);
        case "add_write_notification_log" -> handleAddWriteNotificationLog(method, request);
        case "get_tables_ext" -> handleGetTablesExt(method, request);
        case "get_all_materialized_view_objects_for_rewriting" ->
            handleGetAllMaterializedViewObjectsForRewriting(method);
        default -> throw new TApplicationException(
            TApplicationException.UNKNOWN_METHOD,
            "Unsupported Hive 4 frontend wrapper: " + methodName);
      };
    }

    private Object handleGetDatabaseReq(Method method, Object request) throws Throwable {
      Object database = apacheHandler.get_database((String) invokeNoArgs(request, "getName"));
      return convertResult(database, method.getReturnType());
    }

    private Object handleGetDatabasesReq(Method method, Object request) throws Throwable {
      String pattern = (String) invokeNoArgs(request, "getPattern");
      List<String> databases = pattern == null || pattern.isEmpty()
          ? apacheHandler.get_all_databases()
          : apacheHandler.get_databases(pattern);
      Object response = emptyResponse(method.getReturnType());
      response.getClass().getMethod("setDatabases", List.class).invoke(response, databases);
      return response;
    }

    private Object handleCreateTableReq(Method method, Object request) throws Throwable {
      Table table = (Table) ThriftValueConverter.convertTBase(invokeNoArgs(request, "getTable"), Table.class);
      EnvironmentContext environmentContext =
          (EnvironmentContext) convertIfPresent(invokeNoArgs(request, "getEnvContext"), EnvironmentContext.class);
      @SuppressWarnings("unchecked")
      List<Object> primaryKeys = (List<Object>) invokeNoArgs(request, "getPrimaryKeys");
      @SuppressWarnings("unchecked")
      List<Object> foreignKeys = (List<Object>) invokeNoArgs(request, "getForeignKeys");
      @SuppressWarnings("unchecked")
      List<Object> uniqueConstraints = (List<Object>) invokeNoArgs(request, "getUniqueConstraints");
      @SuppressWarnings("unchecked")
      List<Object> notNullConstraints = (List<Object>) invokeNoArgs(request, "getNotNullConstraints");
      @SuppressWarnings("unchecked")
      List<Object> defaultConstraints = (List<Object>) invokeNoArgs(request, "getDefaultConstraints");
      @SuppressWarnings("unchecked")
      List<Object> checkConstraints = (List<Object>) invokeNoArgs(request, "getCheckConstraints");
      boolean hasConstraints = hasAny(primaryKeys, foreignKeys, uniqueConstraints,
          notNullConstraints, defaultConstraints, checkConstraints);
      if (hasConstraints && environmentContext != null) {
        throw new TApplicationException(
            TApplicationException.UNKNOWN_METHOD,
            "create_table_req with both constraints and envContext has no safe Apache 3.1.3 mapping");
      }
      if (hasConstraints) {
        apacheHandler.create_table_with_constraints(
            table,
            typedList(primaryKeys, org.apache.hadoop.hive.metastore.api.SQLPrimaryKey.class),
            typedList(foreignKeys, org.apache.hadoop.hive.metastore.api.SQLForeignKey.class),
            typedList(uniqueConstraints, org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint.class),
            typedList(notNullConstraints, org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint.class),
            typedList(defaultConstraints, org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint.class),
            typedList(checkConstraints, org.apache.hadoop.hive.metastore.api.SQLCheckConstraint.class));
      } else if (environmentContext != null) {
        apacheHandler.create_table_with_environment_context(table, environmentContext);
      } else {
        apacheHandler.create_table(table);
      }
      return null;
    }

    private Object handleDropTableReq(Method method, Object request) throws Throwable {
      String dbName = (String) invokeNoArgs(request, "getDbName");
      String tableName = (String) invokeNoArgs(request, "getTableName");
      boolean deleteData = (boolean) invokeNoArgs(request, "isDeleteData");
      EnvironmentContext environmentContext =
          (EnvironmentContext) convertIfPresent(invokeNoArgs(request, "getEnvContext"), EnvironmentContext.class);
      if (environmentContext != null) {
        apacheHandler.drop_table_with_environment_context(dbName, tableName, deleteData, environmentContext);
      } else {
        apacheHandler.drop_table(dbName, tableName, deleteData);
      }
      return emptyResponse(method.getReturnType());
    }

    private Object handleAlterTableReq(Method method, Object request) throws Throwable {
      String dbName = (String) invokeNoArgs(request, "getDbName");
      String tableName = (String) invokeNoArgs(request, "getTableName");
      Table table = (Table) ThriftValueConverter.convertTBase(invokeNoArgs(request, "getTable"), Table.class);
      EnvironmentContext environmentContext =
          (EnvironmentContext) convertIfPresent(invokeNoArgs(request, "getEnvironmentContext"), EnvironmentContext.class);
      if (environmentContext != null) {
        apacheHandler.alter_table_with_environment_context(dbName, tableName, table, environmentContext);
      } else {
        apacheHandler.alter_table(dbName, tableName, table);
      }
      return emptyResponse(method.getReturnType());
    }

    private Object handleTruncateTableReq(Method method, Object request) throws Throwable {
      apacheHandler.truncate_table(
          (String) invokeNoArgs(request, "getDbName"),
          (String) invokeNoArgs(request, "getTableName"),
          stringList(invokeNoArgs(request, "getPartNames")));
      return emptyResponse(method.getReturnType());
    }

    @SuppressWarnings("unchecked")
    private Object handleAlterPartitionsReq(Method method, Object request) throws Throwable {
      String dbName = (String) invokeNoArgs(request, "getDbName");
      String tableName = (String) invokeNoArgs(request, "getTableName");
      List<Partition> partitions =
          (List<Partition>) ThriftValueConverter.convertDynamicValue(invokeNoArgs(request, "getPartitions"),
              Hive4FrontendBridge.class.getClassLoader());
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
          (Partition) ThriftValueConverter.convertTBase(invokeNoArgs(request, "getNewPart"), Partition.class));
      return emptyResponse(method.getReturnType());
    }

    private Object handleDropPartitionReq(Method method, Object request) throws Throwable {
      String dbName = (String) invokeNoArgs(request, "getDbName");
      String tableName = (String) invokeNoArgs(request, "getTblName");
      List<String> partVals = stringList(invokeNoArgs(request, "getPartVals"));
      boolean deleteData = (boolean) invokeNoArgs(request, "isDeleteData");
      EnvironmentContext environmentContext =
          (EnvironmentContext) convertIfPresent(invokeNoArgs(request, "getEnvironmentContext"), EnvironmentContext.class);
      boolean result;
      if (environmentContext != null) {
        result = apacheHandler.drop_partition_with_environment_context(
            dbName, tableName, partVals, deleteData, environmentContext);
      } else {
        result = apacheHandler.drop_partition(dbName, tableName, partVals, deleteData);
      }
      return booleanResponse(method.getReturnType(), result);
    }

    private Object handleGetPartitionReq(Method method, Object request) throws Throwable {
      Partition partition = apacheHandler.get_partition(
          (String) invokeNoArgs(request, "getDbName"),
          (String) invokeNoArgs(request, "getTblName"),
          stringList(invokeNoArgs(request, "getPartVals")));
      Object response = emptyResponse(method.getReturnType());
      response.getClass().getMethod("setPartition",
          response.getClass().getMethod("getPartition").getReturnType()).invoke(response,
          convertResult(partition, response.getClass().getMethod("getPartition").getReturnType()));
      return response;
    }

    private Object handleGetPartitionsReq(Method method, Object request) throws Throwable {
      short maxParts = invokeNoArgs(request, "getMaxParts") == null
          ? (short) -1
          : ((Number) invokeNoArgs(request, "getMaxParts")).shortValue();
      List<Partition> partitions = apacheHandler.get_partitions(
          (String) invokeNoArgs(request, "getDbName"),
          (String) invokeNoArgs(request, "getTblName"),
          maxParts);
      Object response = emptyResponse(method.getReturnType());
      response.getClass().getMethod("setPartitions", List.class).invoke(response, partitions);
      return response;
    }

    private Object handleGetPartitionsByNamesReq(Method method, Object request) throws Throwable {
      List<Partition> partitions = apacheHandler.get_partitions_by_names(
          (String) invokeNoArgs(request, "getDb_name"),
          (String) invokeNoArgs(request, "getTbl_name"),
          stringList(invokeNoArgs(request, "getNames")));
      Object response = emptyResponse(method.getReturnType());
      response.getClass().getMethod("setPartitions", List.class).invoke(response, partitions);
      return response;
    }

    private Object handleGetPartitionsByFilterReq(Method method, Object request) throws Throwable {
      short maxParts = invokeNoArgs(request, "getMaxParts") == null
          ? (short) -1
          : ((Number) invokeNoArgs(request, "getMaxParts")).shortValue();
      List<Partition> partitions = apacheHandler.get_partitions_by_filter(
          (String) invokeNoArgs(request, "getDbName"),
          (String) invokeNoArgs(request, "getTblName"),
          (String) invokeNoArgs(request, "getFilter"),
          maxParts);
      Object response = emptyResponse(method.getReturnType());
      response.getClass().getMethod("setPartitions", List.class).invoke(response, partitions);
      return response;
    }

    private Object handleGetPartitionNamesReq(Method method, Object request) throws Throwable {
      short maxParts = invokeNoArgs(request, "getMaxParts") == null
          ? (short) -1
          : ((Number) invokeNoArgs(request, "getMaxParts")).shortValue();
      List<String> names = apacheHandler.get_partition_names(
          (String) invokeNoArgs(request, "getDbName"),
          (String) invokeNoArgs(request, "getTblName"),
          maxParts);
      Object response = emptyResponse(method.getReturnType());
      response.getClass().getMethod("setNames", List.class).invoke(response, names);
      return response;
    }

    private Object handleGetFieldsReq(Method method, Object request) throws Throwable {
      String dbName = (String) invokeNoArgs(request, "getDbName");
      String tableName = (String) invokeNoArgs(request, "getTblName");
      EnvironmentContext environmentContext =
          (EnvironmentContext) convertIfPresent(invokeNoArgs(request, "getEnvContext"), EnvironmentContext.class);
      List<?> fields = environmentContext != null
          ? apacheHandler.get_fields_with_environment_context(dbName, tableName, environmentContext)
          : apacheHandler.get_fields(dbName, tableName);
      Object response = emptyResponse(method.getReturnType());
      response.getClass().getMethod("setFields", List.class).invoke(response, fields);
      return response;
    }

    private Object handleUpdateColumnStatisticsReq(Method method, Object request) throws Throwable {
      boolean result = apacheHandler.set_aggr_stats_for(
          (SetPartitionsStatsRequest) ThriftValueConverter.convertTBase(request, SetPartitionsStatsRequest.class));
      return booleanResponse(method.getReturnType(), result);
    }

    private Object handleAddWriteNotificationLog(Method method, Object request) throws Throwable {
      if (extension == null) {
        throw new TApplicationException(
            TApplicationException.UNKNOWN_METHOD,
            "Hive 4 method add_write_notification_log requires proxy extension support");
      }
      Object response = extension.addWriteNotificationLog(request);
      return convertResult(response, method.getReturnType());
    }

    private Object handleGetTablesExt(Method method, Object request) throws Throwable {
      if (extension == null) {
        throw new TApplicationException(
            TApplicationException.UNKNOWN_METHOD,
            "Hive 4 method get_tables_ext requires proxy extension support");
      }
      Object response = extension.getTablesExt(request);
      return convertResult(response, method.getReturnType());
    }

    private Object handleGetAllMaterializedViewObjectsForRewriting(Method method) throws Throwable {
      if (extension == null) {
        throw new TApplicationException(
            TApplicationException.UNKNOWN_METHOD,
            "Hive 4 method get_all_materialized_view_objects_for_rewriting requires proxy extension support");
      }
      Object response = extension.getAllMaterializedViewObjectsForRewriting();
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
        converted[index] = ThriftValueConverter.convertValue(args[index], parameterTypes[index],
            Hive4FrontendBridge.class.getClassLoader());
      }
      return converted;
    }

    private Object convertResult(Object result, Class<?> returnType) throws Exception {
      if (returnType == void.class || result == null) {
        return null;
      }
      return ThriftValueConverter.convertValue(result, returnType, hive4ClassLoader);
    }

    private Object convertIfPresent(Object value, Class<?> targetType) throws Exception {
      return value == null ? null : ThriftValueConverter.convertValue(value, targetType,
          Hive4FrontendBridge.class.getClassLoader());
    }

    private Object invokeNoArgs(Object target, String methodName) throws ReflectiveOperationException {
      return target.getClass().getMethod(methodName).invoke(target);
    }

    @SuppressWarnings("unchecked")
    private List<String> stringList(Object value) {
      return value == null ? List.of() : (List<String>) value;
    }

    @SuppressWarnings("unchecked")
    private <T> List<T> typedList(List<Object> value, Class<T> type) {
      return value == null ? List.of() : (List<T>) value;
    }

    private boolean hasAny(List<?>... values) {
      for (List<?> value : values) {
        if (value != null && !value.isEmpty()) {
          return true;
        }
      }
      return false;
    }

    private Object emptyResponse(Class<?> responseType) throws ReflectiveOperationException {
      if (responseType == void.class) {
        // Some Hive 4 *_req methods are typed `void <name>_req(<Request> req)` and have no
        // response wrapper, only exception types in the *_result class. Returning null here
        // matches Thrift's expectation that void RPCs produce no body.
        return null;
      }
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
  }
}
