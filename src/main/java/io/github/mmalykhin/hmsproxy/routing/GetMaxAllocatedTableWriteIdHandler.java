package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftFailureClassifier;
import java.lang.reflect.Method;
import org.apache.hadoop.hive.metastore.api.MetaException;

final class GetMaxAllocatedTableWriteIdHandler implements SpecialCaseHandler {
  private final RoutingSupport support;

  GetMaxAllocatedTableWriteIdHandler(RoutingSupport support) {
    this.support = support;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    Object request = args[0];
    String dbName = (String) ThriftReflectionCache.invokeGetter(request, "getDbName");
    if (dbName == null) {
      throw new MetaException("get_max_allocated_table_write_id requires a target database");
    }

    CatalogRouter.ResolvedNamespace namespace = support.router.resolveDatabase(dbName);
    RequestContext.currentObservation().recordNamespace(namespace);
    support.recordDefaultCatalogRouteIfImplicit("get_max_allocated_table_write_id", dbName, namespace);
    CatalogBackend backend = namespace.backend();
    support.validateCatalogAccess(backend, "get_max_allocated_table_write_id", namespace.backendDbName());

    ThriftReflectionCache.invokeStringSetter(request, "setDbName", namespace.backendDbName());

    try {
      return support.invokeBackendNamed(backend, "get_max_allocated_table_write_id", request);
    } catch (Throwable cause) {
      if (ThriftFailureClassifier.isUnsupportedMethod(cause)) {
        return fallbackToLegacy(request);
      }
      throw cause;
    }
  }

  private Object fallbackToLegacy(Object request) throws Throwable {
    ClassLoader cl = request.getClass().getClassLoader();
    Class<?> responseClass = Class.forName("org.apache.hadoop.hive.metastore.api.MaxAllocatedTableWriteIdResponse", true, cl);
    try {
      return responseClass.getConstructor(long.class).newInstance(0L);
    } catch (NoSuchMethodException e) {
      Object response = responseClass.getConstructor().newInstance();
      responseClass.getMethod("setMaxWriteId", long.class).invoke(response, 0L);
      return response;
    }
  }
}
