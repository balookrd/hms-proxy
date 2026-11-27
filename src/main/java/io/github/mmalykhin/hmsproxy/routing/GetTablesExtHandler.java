package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.TableMeta;

final class GetTablesExtHandler implements SpecialCaseHandler {
  private static final Method GET_TABLE_META = RequestObjectRouting.findIfaceMethod(
      "get_table_meta", String.class, String.class, List.class);

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
    support.validateCatalogAccess(backend, "get_tables_ext", namespace.backendDbName());
    if (!support.isDatabaseAccessible("get_tables_ext", namespace)) {
      return new ArrayList<>();
    }
    if (!backend.runtimeProfile().isHortonworks()) {
      return fallbackToGetTableMeta(request, namespace, backend);
    }
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

  private List<Object> fallbackToGetTableMeta(
      Object request,
      CatalogRouter.ResolvedNamespace namespace,
      CatalogBackend backend
  ) throws Throwable {
    String pattern = ThriftReflectionCache.readString(request, "getTablesPattern", "getPattern");
    Object rawMetas = support.invokeDirect(
        backend,
        GET_TABLE_META,
        new Object[] {namespace.backendDbName(), pattern == null ? "*" : pattern, null});
    if (!(rawMetas instanceof List<?> metas) || metas.isEmpty()) {
      return new ArrayList<>();
    }
    ClassLoader cl = request.getClass().getClassLoader();
    Class<?> infoClass = Class.forName("org.apache.hadoop.hive.metastore.api.ExtendedTableInfo", true, cl);
    Constructor<?> ctor = infoClass.getConstructor(String.class);
    List<Object> result = new ArrayList<>(metas.size());
    for (Object item : metas) {
      String tableName = item instanceof TableMeta meta
          ? meta.getTableName()
          : RoutingSupport.extractTableName(item);
      if (tableName != null && support.isTableAccessible("get_tables_ext", namespace, tableName)) {
        result.add(ctor.newInstance(tableName));
      }
    }
    return result;
  }
}
