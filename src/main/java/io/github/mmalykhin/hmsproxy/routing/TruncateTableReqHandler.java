package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftFailureClassifier;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;

final class TruncateTableReqHandler implements SpecialCaseHandler {
  private static final Method TRUNCATE_TABLE = findMethod("truncate_table", String.class, String.class, List.class);

  private final RoutingSupport support;

  TruncateTableReqHandler(RoutingSupport support) {
    this.support = support;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    Object request = args[0];
    String dbName = NamespaceTranslator.extractDbName(request);
    if (dbName == null) {
      throw new MetaException("truncate_table_req requires a target database");
    }

    CatalogRouter.ResolvedNamespace namespace = support.router.resolveDatabase(dbName);
    RequestContext.currentObservation().recordNamespace(namespace);
    support.recordDefaultCatalogRouteIfImplicit("truncate_table_req", dbName, namespace);
    CatalogBackend backend = namespace.backend();
    support.validateCatalogAccess(backend, "truncate_table_req", namespace.backendDbName());

    Long writeId = (Long) ThriftReflectionCache.invokeGetter(request, "getWriteId");
    if (writeId != null && writeId > 0 && !namespace.catalogName().equals(support.config.defaultCatalog())) {
      throw new MetaException("ACID transactional operation 'truncate_table_req' is not supported for non-default catalog '"
          + namespace.catalogName() + "'. Transaction management is only available in the default catalog '"
          + support.config.defaultCatalog() + "'");
    }

    Object routedRequest = support.federationLayer.internalizeObjectArguments(new Object[]{request}, namespace)[0];
    try {
      return support.invokeBackendNamed(backend, "truncate_table_req", routedRequest);
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
    Object partNames = ThriftReflectionCache.invokeGetter(routedRequest, "getPartNames");
    List<String> partNamesList = partNames == null ? List.of() : (List<String>) partNames;
    support.invokeDirect(backend, TRUNCATE_TABLE, new Object[]{db, tbl, partNamesList});
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
