package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftFailureClassifier;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftValueConverter;
import java.lang.reflect.Method;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;

final class AlterTableReqHandler implements SpecialCaseHandler {
  private static final Method ALTER_TABLE = findMethod("alter_table", String.class, String.class, Table.class);
  private static final Method ALTER_TABLE_WITH_ENV = findMethod("alter_table_with_environment_context",
      String.class, String.class, Table.class, EnvironmentContext.class);

  private final RoutingSupport support;
  private final IcebergTablePointerGuard icebergTablePointerGuard;

  AlterTableReqHandler(RoutingSupport support, IcebergTablePointerGuard icebergTablePointerGuard) {
    this.support = support;
    this.icebergTablePointerGuard = icebergTablePointerGuard;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    Object request = args[0];
    String dbName = NamespaceTranslator.extractDbName(request);
    if (dbName == null) {
      throw new MetaException("alter_table_req requires a target database");
    }

    CatalogRouter.ResolvedNamespace namespace = support.router.resolveDatabase(dbName);
    RequestContext.currentObservation().recordNamespace(namespace);
    support.recordDefaultCatalogRouteIfImplicit("alter_table_req", dbName, namespace);
    CatalogBackend backend = namespace.backend();
    support.validateCatalogAccess(backend, "alter_table_req", namespace.backendDbName());

    Long writeId = (Long) ThriftReflectionCache.invokeGetter(request, "getWriteId");
    if (writeId != null && writeId > 0 && !namespace.catalogName().equals(support.config.defaultCatalog())) {
      throw new MetaException("ACID transactional operation 'alter_table_req' is not supported for non-default catalog '"
          + namespace.catalogName() + "'. Transaction management is only available in the default catalog '"
          + support.config.defaultCatalog() + "'");
    }

    Object routedRequest = support.federationLayer.internalizeObjectArguments(new Object[]{request}, namespace)[0];
    Object rawTable = ThriftReflectionCache.invokeGetter(routedRequest, "getTable");
    Table table = rawTable instanceof Table t ? t : null;

    Object[] guardArgs = table != null ? new Object[]{routedRequest, table} : new Object[]{routedRequest};
    try (IcebergTablePointerGuard.Protection ignored =
             icebergTablePointerGuard != null ? icebergTablePointerGuard.protect(guardArgs, namespace, "alter_table_req") : null) {
      try {
        return support.invokeBackendNamed(backend, "alter_table_req", routedRequest);
      } catch (Throwable cause) {
        if (ThriftFailureClassifier.isUnsupportedMethod(cause)) {
          return fallbackToLegacy(backend, routedRequest);
        }
        throw cause;
      }
    }
  }

  private Object fallbackToLegacy(CatalogBackend backend, Object routedRequest) throws Throwable {
    String db = (String) ThriftReflectionCache.invokeGetter(routedRequest, "getDbName");
    String tbl = (String) ThriftReflectionCache.invokeGetter(routedRequest, "getTableName");
    Object rawTable = ThriftReflectionCache.invokeGetter(routedRequest, "getTable");
    Object rawEnv = ThriftReflectionCache.invokeGetter(routedRequest, "getEnvironmentContext");

    Table apacheTable = (Table) ThriftValueConverter.convertTBase(rawTable, Table.class);
    EnvironmentContext apacheEnv = rawEnv == null ? null : (EnvironmentContext) ThriftValueConverter.convertTBase(rawEnv, EnvironmentContext.class);

    if (apacheEnv != null) {
      support.invokeDirect(backend, ALTER_TABLE_WITH_ENV, new Object[]{db, tbl, apacheTable, apacheEnv});
    } else {
      support.invokeDirect(backend, ALTER_TABLE, new Object[]{db, tbl, apacheTable});
    }
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
