package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import java.lang.reflect.Method;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;

final class GetMaxAllocatedTableWriteIdHandler implements SpecialCaseHandler {
  private final RoutingSupport support;

  GetMaxAllocatedTableWriteIdHandler(RoutingSupport support) {
    this.support = support;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    Object request = args[0];
    RequestObjectRouting.ResolvedRequest resolved =
        RequestObjectRouting.resolveWrite(support, "get_max_allocated_table_write_id", request);
    CatalogRouter.ResolvedNamespace namespace = resolved.namespace();
    CatalogBackend backend = resolved.backend();
    String tblName = resolved.tableName();
    try {
      support.validateExposedDatabaseAccess(method, namespace);
      if (tblName != null && !tblName.isBlank()) {
        support.validateExposedTableAccess(method, namespace, tblName);
      }
    } catch (NoSuchObjectException e) {
      // get_max_allocated_table_write_id IDL declares MetaException, not NoSuchObjectException.
      throw new MetaException(e.getMessage());
    }

    Object routedRequest = ThriftReflectionCache.deepCopy(request);
    ThriftReflectionCache.invokeStringSetter(routedRequest, "setDbName", namespace.backendDbName());

    return RequestObjectRouting.invokeWithUnsupportedFallback(
        support,
        backend,
        "get_max_allocated_table_write_id",
        routedRequest,
        () -> fallbackToLegacy(routedRequest));
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
