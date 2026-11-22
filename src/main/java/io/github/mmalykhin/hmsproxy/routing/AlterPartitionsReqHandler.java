package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftValueConverter;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;

final class AlterPartitionsReqHandler implements SpecialCaseHandler {
  private static final Method ALTER_PARTS = RequestObjectRouting.findIfaceMethod(
      "alter_partitions", String.class, String.class, List.class);
  private static final Method ALTER_PARTS_WITH_ENV = RequestObjectRouting.findIfaceMethod(
      "alter_partitions_with_environment_context",
      String.class, String.class, List.class, EnvironmentContext.class);

  private final RoutingSupport support;

  AlterPartitionsReqHandler(RoutingSupport support) {
    this.support = support;
  }

  AlterPartitionsReqHandler(RoutingSupport support, ExternalTableLocationRewriter ignored) {
    this(support);
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    Object request = args[0];
    RequestObjectRouting.ResolvedRequest resolved =
        RequestObjectRouting.resolveWrite(support, "alter_partitions_req", request);
    String dbName = resolved.dbName();
    CatalogRouter.ResolvedNamespace namespace = resolved.namespace();
    CatalogBackend backend = resolved.backend();

    Long writeId = (Long) ThriftReflectionCache.invokeGetter(request, "getWriteId");
    if (writeId != null && writeId > 0 && !namespace.catalogName().equals(support.config.defaultCatalog())) {
      throw new MetaException("ACID transactional operation 'alter_partitions_req' is not supported for non-default catalog '"
          + namespace.catalogName() + "'. Transaction management is only available in the default catalog '"
          + support.config.defaultCatalog() + "'");
    }

    Object routedRequest = support.federationLayer.internalizeObjectArguments(new Object[]{request}, namespace)[0];
    ThriftReflectionCache.invokeStringSetter(routedRequest, "setDbName", namespace.backendDbName());
    String catName = ThriftReflectionCache.readString(routedRequest, "getCatName");
    if (catName != null) {
      ThriftReflectionCache.invokeStringSetter(routedRequest, "setCatName",
          NamespaceTranslator.internalCatalogName(catName, dbName, namespace,
              support.federationLayer.preserveBackendCatalogName()));
    }
    String validWriteIdList = ThriftReflectionCache.readString(routedRequest, "getValidWriteIdList");
    if (validWriteIdList != null) {
      ThriftReflectionCache.invokeStringSetter(routedRequest, "setValidWriteIdList",
          NamespaceInternalizer.transformValidWriteIdList(validWriteIdList, namespace));
    }
    Object rawPartitions = ThriftReflectionCache.invokeGetter(routedRequest, "getPartitions");
    if (rawPartitions instanceof Iterable<?> iterable) {
      for (Object part : iterable) {
        String partDb = ThriftReflectionCache.readString(part, "getDbName");
        if (partDb != null && NamespaceTranslator.matchesExternalDatabaseAlias(partDb, namespace.externalDbName())) {
          ThriftReflectionCache.invokeStringSetter(part, "setDbName", namespace.backendDbName());
        }
        String partCat = ThriftReflectionCache.readString(part, "getCatName");
        if (partCat != null) {
          ThriftReflectionCache.invokeStringSetter(part, "setCatName",
              NamespaceTranslator.internalCatalogName(partCat, dbName, namespace,
                  support.federationLayer.preserveBackendCatalogName()));
        }
      }
    }
    ExternalTableLocationRewriter externalTableLocationRewriter = support.externalTableLocationRewriter();
    if (externalTableLocationRewriter != null) {
      externalTableLocationRewriter.rewriteObjectArguments(new Object[]{routedRequest}, namespace, "alter_partitions_req");
    }
    Object result = RequestObjectRouting.invokeWithUnsupportedFallback(
        support,
        backend,
        "alter_partitions_req",
        routedRequest,
        () -> fallbackToLegacy(backend, routedRequest));
    String tableName = RoutingSupport.extractTableName(routedRequest);
    if (tableName != null) {
      support.invalidateTable(namespace.catalogName(), namespace.backendDbName(), tableName);
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  private Object fallbackToLegacy(CatalogBackend backend, Object routedRequest) throws Throwable {
    String db = (String) ThriftReflectionCache.invokeGetter(routedRequest, "getDbName");
    String tbl = (String) ThriftReflectionCache.invokeGetter(routedRequest, "getTableName");
    Object rawPartitions = ThriftReflectionCache.invokeGetter(routedRequest, "getPartitions");
    Object rawEnv = ThriftReflectionCache.invokeGetter(routedRequest, "getEnvironmentContext");

    List<Partition> apachePartitions =
        (List<Partition>) ThriftValueConverter.convertDynamicValue(rawPartitions, Partition.class.getClassLoader());
    EnvironmentContext apacheEnv = rawEnv == null ? null : (EnvironmentContext) ThriftValueConverter.convertTBase(rawEnv, EnvironmentContext.class);

    if (apacheEnv != null) {
      support.invokeDirect(backend, ALTER_PARTS_WITH_ENV, new Object[]{db, tbl, apachePartitions, apacheEnv});
    } else {
      support.invokeDirect(backend, ALTER_PARTS, new Object[]{db, tbl, apachePartitions});
    }
    return null;
  }
}
