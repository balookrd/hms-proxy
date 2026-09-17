package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftFailureClassifier;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftValueConverter;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;

final class AlterPartitionsReqHandler implements SpecialCaseHandler {
  private static final Method ALTER_PARTS = findMethod("alter_partitions", String.class, String.class, List.class);
  private static final Method ALTER_PARTS_WITH_ENV = findMethod("alter_partitions_with_environment_context",
      String.class, String.class, List.class, EnvironmentContext.class);

  private final RoutingSupport support;

  AlterPartitionsReqHandler(RoutingSupport support) {
    this.support = support;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    Object request = args[0];
    String dbName = NamespaceTranslator.extractDbName(request);
    if (dbName == null) {
      throw new MetaException("alter_partitions_req requires a target database");
    }

    CatalogRouter.ResolvedNamespace namespace = support.router.resolveDatabase(dbName);
    RequestContext.currentObservation().recordNamespace(namespace);
    support.recordDefaultCatalogRouteIfImplicit("alter_partitions_req", dbName, namespace);
    CatalogBackend backend = namespace.backend();
    support.validateCatalogAccess(backend, "alter_partitions_req", namespace.backendDbName());

    Long writeId = (Long) ThriftReflectionCache.invokeGetter(request, "getWriteId");
    if (writeId != null && writeId > 0 && !namespace.catalogName().equals(support.config.defaultCatalog())) {
      throw new MetaException("ACID transactional operation 'alter_partitions_req' is not supported for non-default catalog '"
          + namespace.catalogName() + "'. Transaction management is only available in the default catalog '"
          + support.config.defaultCatalog() + "'");
    }

    Object routedRequest = support.federationLayer.internalizeObjectArguments(new Object[]{request}, namespace)[0];
    try {
      return support.invokeBackendNamed(backend, "alter_partitions_req", routedRequest);
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

  private static Method findMethod(String name, Class<?>... parameterTypes) {
    try {
      return ThriftHiveMetastore.Iface.class.getMethod(name, parameterTypes);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException("ThriftHiveMetastore.Iface missing method " + name, e);
    }
  }
}
