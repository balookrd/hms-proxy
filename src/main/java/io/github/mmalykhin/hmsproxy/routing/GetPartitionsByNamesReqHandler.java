package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftFailureClassifier;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftValueConverter;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;

final class GetPartitionsByNamesReqHandler implements SpecialCaseHandler {
  private static final Method GET_PARTITIONS_BY_NAMES = findMethod("get_partitions_by_names",
      String.class, String.class, List.class);

  private final RoutingSupport support;

  GetPartitionsByNamesReqHandler(RoutingSupport support) {
    this.support = support;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    Object request = args[0];
    String dbName = ThriftReflectionCache.readString(request, "getDb_name", "getDbName", "db_name", "dbName");
    String tblName = ThriftReflectionCache.readString(request, "getTbl_name", "getTblName", "tbl_name", "tblName");
    if (dbName == null) {
      throw new MetaException("get_partitions_by_names_req requires a target database");
    }

    CatalogRouter.ResolvedNamespace namespace = support.router.resolveDatabase(dbName);
    RequestContext.currentObservation().recordNamespace(namespace);
    support.recordDefaultCatalogRouteIfImplicit(method.getName(), dbName, namespace);
    CatalogBackend backend = namespace.backend();
    support.validateExposedDatabaseAccess(method.getName(), namespace);
    support.validateExposedTableAccess(method.getName(), namespace, tblName);

    Object routedRequest = support.federationLayer.internalizeArgument(request, namespace);
    Object result;
    try {
      result = support.invokeBackendNamed(backend, "get_partitions_by_names_req", routedRequest);
    } catch (Throwable cause) {
      if (ThriftFailureClassifier.isUnsupportedMethod(cause)) {
        result = fallbackToLegacy(backend, routedRequest);
      } else {
        throw cause;
      }
    }
    return support.federationLayer.externalizeResult(result, namespace);
  }

  @SuppressWarnings("unchecked")
  private Object fallbackToLegacy(CatalogBackend backend, Object routedRequest) throws Throwable {
    String db = (String) ThriftReflectionCache.invokeGetter(routedRequest, "getDb_name");
    if (db == null) {
      db = (String) ThriftReflectionCache.invokeGetter(routedRequest, "getDbName");
    }
    String tbl = (String) ThriftReflectionCache.invokeGetter(routedRequest, "getTbl_name");
    if (tbl == null) {
      tbl = (String) ThriftReflectionCache.invokeGetter(routedRequest, "getTblName");
    }
    Object names = ThriftReflectionCache.invokeGetter(routedRequest, "getNames");
    List<String> namesList = names == null ? List.of() : (List<String>) names;

    Object partitions = support.invokeDirect(backend, GET_PARTITIONS_BY_NAMES, new Object[]{db, tbl, namesList});
    ClassLoader cl = routedRequest.getClass().getClassLoader();
    Class<?> responseClass = Class.forName("org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesResult", true, cl);
    Object response = responseClass.getConstructor().newInstance();
    Object convertedPartitions = ThriftValueConverter.convertDynamicValue(partitions, cl);
    responseClass.getMethod("setPartitions", List.class).invoke(response, convertedPartitions);
    return response;
  }

  private static Method findMethod(String name, Class<?>... parameterTypes) {
    try {
      return ThriftHiveMetastore.Iface.class.getMethod(name, parameterTypes);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException("Missing expected Iface method: " + name, e);
    }
  }
}
