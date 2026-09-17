package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftFailureClassifier;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftValueConverter;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;

final class RenamePartitionReqHandler implements SpecialCaseHandler {
  private static final Method RENAME_PARTITION = findMethod("rename_partition",
      String.class, String.class, List.class, Partition.class);

  private final RoutingSupport support;

  RenamePartitionReqHandler(RoutingSupport support) {
    this.support = support;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    Object request = args[0];
    String dbName = NamespaceTranslator.extractDbName(request);
    if (dbName == null) {
      throw new MetaException("rename_partition_req requires a target database");
    }

    CatalogRouter.ResolvedNamespace namespace = support.router.resolveDatabase(dbName);
    RequestContext.currentObservation().recordNamespace(namespace);
    support.recordDefaultCatalogRouteIfImplicit("rename_partition_req", dbName, namespace);
    CatalogBackend backend = namespace.backend();
    support.validateCatalogAccess(backend, "rename_partition_req", namespace.backendDbName());

    Object routedRequest = support.federationLayer.internalizeObjectArguments(new Object[]{request}, namespace)[0];
    try {
      return support.invokeBackendNamed(backend, "rename_partition_req", routedRequest);
    } catch (Throwable cause) {
      if (ThriftFailureClassifier.isUnsupportedMethod(cause)) {
        return fallbackToLegacy(backend, routedRequest);
      }
      throw cause;
    }
  }

  @SuppressWarnings("unchecked")
  private Object fallbackToLegacy(CatalogBackend backend, Object routedRequest) throws Throwable {
    String db = (String) ThriftReflectionCache.invokeGetter(routedRequest, "getDbName");
    String tbl = (String) ThriftReflectionCache.invokeGetter(routedRequest, "getTableName");
    Object partVals = ThriftReflectionCache.invokeGetter(routedRequest, "getPartVals");
    Object newPart = ThriftReflectionCache.invokeGetter(routedRequest, "getNewPart");

    List<String> partValsList = partVals == null ? List.of() : (List<String>) partVals;
    Partition apacheNewPart = (Partition) ThriftValueConverter.convertTBase(newPart, Partition.class);
    support.invokeDirect(backend, RENAME_PARTITION, new Object[]{db, tbl, partValsList, apacheNewPart});
    return null;
  }

  private static Method findMethod(String name, Class<?>... parameterTypes) {
    try {
      return ThriftHiveMetastore.Iface.class.getMethod(name, parameterTypes);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException("ThriftHiveMetastore.Iface missing method " + name, e);
    }
  }
}
