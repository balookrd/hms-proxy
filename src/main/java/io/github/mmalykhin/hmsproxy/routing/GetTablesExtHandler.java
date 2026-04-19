package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import java.lang.reflect.Method;
import org.apache.hadoop.hive.metastore.api.MetaException;

final class GetTablesExtHandler implements SpecialCaseHandler {
  private final RoutingSupport support;

  GetTablesExtHandler(RoutingSupport support) {
    this.support = support;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    Object request = args[0];
    String catalogName = ThriftReflectionCache.readString(request, "getCatalog");
    String dbName = ThriftReflectionCache.readString(request, "getDatabase");
    CatalogRouter.ResolvedNamespace namespace = support.resolveRequestNamespace(catalogName, dbName);
    RequestContext.currentObservation().recordNamespace(namespace);
    support.recordDefaultCatalogRouteIfImplicit("get_tables_ext", catalogName, dbName, namespace);
    CatalogBackend backend = namespace.backend();
    if (!backend.runtimeProfile().isHortonworks()) {
      throw new MetaException(
          "Hortonworks get_tables_ext requires a Hortonworks backend runtime for catalog '"
              + backend.name()
              + "'");
    }
    support.validateCatalogAccess(backend, "get_tables_ext", namespace.backendDbName());
    support.validateExposedDatabaseAccess("get_tables_ext", namespace);
    Object routedRequest = ThriftReflectionCache.deepCopy(request);
    ThriftReflectionCache.invokeStringSetter(routedRequest, "setDatabase", namespace.backendDbName());
    String internalCatalog = NamespaceTranslator.internalCatalogName(catalogName, dbName, namespace,
        support.federationLayer.preserveBackendCatalogName());
    ThriftReflectionCache.invokeStringSetter(routedRequest, "setCatalog",
        internalCatalog == null ? catalogName : internalCatalog);
    return support.filterTableCollectionResult(
        "get_tables_ext",
        namespace,
        support.invokeBackendNamed(backend, "get_tables_ext", routedRequest));
  }
}
