package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.compatibility.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.routing.ImpersonationContext;
import java.util.Collections;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesResult;
import org.apache.thrift.TApplicationException;

public final class HortonworksBackendAdapter extends AbstractBackendAdapter {
  private static final Map<String, RequestCompatibilityHandler> REQUEST_COMPATIBILITY_HANDLERS = Map.of(
      "get_database_req",
      (backend, request, impersonation) -> {
        return (Database) backend.invokeRawByName(
            "get_database",
            new Class<?>[] {String.class},
            new Object[] {request.getClass().getMethod("getName").invoke(request)},
            impersonation);
      },
      "get_table_req",
      (backend, request, impersonation) -> {
        GetTableRequest getTableRequest = (GetTableRequest) request;
        Object result = backend.invokeRawByName(
            "get_table",
            new Class<?>[] {String.class, String.class},
            new Object[] {getTableRequest.getDbName(), getTableRequest.getTblName()},
            impersonation);
        return new GetTableResult((org.apache.hadoop.hive.metastore.api.Table) result);
      },
      "get_table_objects_by_name_req",
      (backend, request, impersonation) -> {
        GetTablesRequest getTablesRequest = (GetTablesRequest) request;
        Object result = backend.invokeRawByName(
            "get_table_objects_by_name",
            new Class<?>[] {String.class, List.class},
            new Object[] {getTablesRequest.getDbName(), getTablesRequest.getTblNames()},
            impersonation);
        @SuppressWarnings("unchecked")
        List<org.apache.hadoop.hive.metastore.api.Table> tables =
            (List<org.apache.hadoop.hive.metastore.api.Table>) result;
        return new GetTablesResult(tables);
      });
  private final Set<String> unsupportedRequestWrappers = Collections.newSetFromMap(new ConcurrentHashMap<>());

  HortonworksBackendAdapter(MetastoreRuntimeProfile runtimeProfile) {
    super(runtimeProfile);
  }

  @Override
  public Object invokeRequest(
      CatalogBackend backend,
      String methodName,
      Object request,
      ImpersonationContext impersonation
  ) throws Throwable {
    RequestCompatibilityHandler compatibilityHandler = REQUEST_COMPATIBILITY_HANDLERS.get(methodName);
    if (compatibilityHandler == null) {
      return super.invokeRequest(backend, methodName, request, impersonation);
    }

    if (unsupportedRequestWrappers.contains(methodName)) {
      return compatibilityHandler.invoke(backend, request, impersonation);
    }

    try {
      return super.invokeRequest(
          backend,
          methodName,
          request,
          impersonation);
    } catch (Throwable cause) {
      if (!shouldFallbackToLegacy(cause)) {
        throw cause;
      }
      unsupportedRequestWrappers.add(methodName);
    }

    return compatibilityHandler.invoke(backend, request, impersonation);
  }

  private boolean shouldFallbackToLegacy(Throwable cause) {
    return cause instanceof TApplicationException || cause instanceof NoSuchMethodException;
  }

  @FunctionalInterface
  private interface RequestCompatibilityHandler {
    Object invoke(
        CatalogBackend backend,
        Object request,
        ImpersonationContext impersonation
    ) throws Throwable;
  }
}
