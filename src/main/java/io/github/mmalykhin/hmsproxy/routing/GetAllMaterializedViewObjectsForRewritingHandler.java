package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.MetaException;

final class GetAllMaterializedViewObjectsForRewritingHandler implements SpecialCaseHandler {
  private static final String METHOD_NAME = "get_all_materialized_view_objects_for_rewriting";

  private final RoutingSupport support;

  GetAllMaterializedViewObjectsForRewritingHandler(RoutingSupport support) {
    this.support = support;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    CatalogBackend backend = support.router.defaultBackend();
    RequestContext.currentObservation().recordNamespace(
        support.router.resolveCatalog(support.config.defaultCatalog(), ""));
    RequestContext.currentObservation().markDefaultCatalogRoute();
    support.observability.metrics().recordDefaultCatalogRoute(METHOD_NAME);
    if (!backend.runtimeProfile().isHortonworks()) {
      throw new MetaException(
          "Hortonworks " + METHOD_NAME + " requires a Hortonworks backend runtime for catalog '"
              + backend.name()
              + "'");
    }
    support.validateCatalogAccess(backend, METHOD_NAME, null);
    Object result = support.invokeByReflection(backend, METHOD_NAME, new Class<?>[0], new Object[0]);
    if (result instanceof List<?> tables) {
      List<Object> externalized = new ArrayList<>(tables.size());
      for (Object table : tables) {
        String dbName = NamespaceTranslator.extractDbName(table);
        CatalogRouter.ResolvedNamespace namespace =
            support.router.resolveCatalog(support.config.defaultCatalog(), dbName);
        String tableName = RoutingSupport.extractTableName(table);
        if (!support.federationLayer.isDatabaseExposed(namespace)
            || (tableName != null && !support.federationLayer.isTableExposed(namespace, tableName))) {
          support.recordFilteredObject(METHOD_NAME, namespace.catalogName(), "table");
          continue;
        }
        externalized.add(support.federationLayer.externalizeResult(table, namespace));
      }
      return externalized;
    }
    return result;
  }
}
