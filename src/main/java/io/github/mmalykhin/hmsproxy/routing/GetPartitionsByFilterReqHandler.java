package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftValueConverter;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;

final class GetPartitionsByFilterReqHandler implements SpecialCaseHandler {
  private static final Method GET_PARTITIONS_BY_FILTER = findMethod("get_partitions_by_filter",
      String.class, String.class, String.class, short.class);

  private final RoutingSupport support;

  GetPartitionsByFilterReqHandler(RoutingSupport support) {
    this.support = support;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    Object request = args[0];
    String catName = ThriftReflectionCache.readString(request, "catName", "getCatName");
    String dbName = ThriftReflectionCache.readString(request, "dbName", "getDbName");
    String tblName = ThriftReflectionCache.readString(request, "tblName", "getTblName");
    if (dbName == null) {
      throw new MetaException("get_partitions_by_filter_req requires a target database");
    }

    CatalogRouter.ResolvedNamespace namespace = support.federationLayer.resolveRequestNamespace(catName, dbName);
    RequestContext.currentObservation().recordNamespace(namespace);
    support.recordDefaultCatalogRouteIfImplicit(method.getName(), catName, dbName, namespace);
    CatalogBackend backend = namespace.backend();
    support.validateExposedDatabaseAccess(method.getName(), namespace);
    support.validateExposedTableAccess(method.getName(), namespace, tblName);

    Object routedRequest = support.federationLayer.internalizeArgument(request, namespace);
    Object result;
    if (backend.runtimeProfile().isHive4()) {
      result = support.invokeBackendNamed(backend, "get_partitions_by_filter_req", routedRequest);
    } else {
      result = fallbackToLegacy(backend, routedRequest);
    }
    return support.federationLayer.externalizeResult(result, namespace);
  }

  private Object fallbackToLegacy(CatalogBackend backend, Object routedRequest) throws Throwable {
    String db = (String) ThriftReflectionCache.invokeGetter(routedRequest, "getDbName");
    String tbl = (String) ThriftReflectionCache.invokeGetter(routedRequest, "getTblName");
    String filter = (String) ThriftReflectionCache.invokeGetter(routedRequest, "getFilter");
    Object maxPartsObj = ThriftReflectionCache.invokeGetter(routedRequest, "getMaxParts");
    short maxParts = maxPartsObj instanceof Number number ? number.shortValue() : (short) -1;

    Object partitions = support.invokeDirect(backend, GET_PARTITIONS_BY_FILTER, new Object[]{db, tbl, filter, maxParts});
    ClassLoader cl = routedRequest.getClass().getClassLoader();
    return ThriftValueConverter.convertDynamicValue(partitions, cl);
  }

  private static Method findMethod(String name, Class<?>... parameterTypes) {
    try {
      return ThriftHiveMetastore.Iface.class.getMethod(name, parameterTypes);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException("Missing expected Iface method: " + name, e);
    }
  }
}
