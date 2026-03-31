package io.github.mmalykhin.hmsproxy;

import java.lang.reflect.Method;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesResult;

final class HortonworksBackendAdapter extends AbstractBackendAdapter {
  HortonworksBackendAdapter(String backendVersion) {
    super(backendVersion);
  }

  HortonworksBackendAdapter(String backendVersion, MetastoreRuntimeProfile runtimeProfileOverride) {
    super(backendVersion, runtimeProfileOverride);
  }

  @Override
  public Object invokeGetTableReq(
      CatalogBackend backend,
      GetTableRequest request,
      RoutingMetaStoreHandler.ImpersonationContext impersonation
  ) throws Throwable {
    if (!usesLegacyRequestApi()) {
      Method method = org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface.class
          .getMethod("get_table_req", GetTableRequest.class);
      return invoke(backend, method, new Object[] {request}, impersonation);
    }

    Object result = backend.invokeRawByName(
        "get_table",
        new Class<?>[] {String.class, String.class},
        new Object[] {request.getDbName(), request.getTblName()},
        impersonation);
    return new GetTableResult((org.apache.hadoop.hive.metastore.api.Table) result);
  }

  @Override
  public Object invokeGetTablesReq(
      CatalogBackend backend,
      GetTablesRequest request,
      RoutingMetaStoreHandler.ImpersonationContext impersonation
  ) throws Throwable {
    if (!usesLegacyRequestApi()) {
      Method method = org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface.class
          .getMethod("get_table_objects_by_name_req", GetTablesRequest.class);
      return invoke(backend, method, new Object[] {request}, impersonation);
    }

    Object result = backend.invokeRawByName(
        "get_table_objects_by_name",
        new Class<?>[] {String.class, List.class},
        new Object[] {request.getDbName(), request.getTblNames()},
        impersonation);
    @SuppressWarnings("unchecked")
    List<org.apache.hadoop.hive.metastore.api.Table> tables =
        (List<org.apache.hadoop.hive.metastore.api.Table>) result;
    return new GetTablesResult(tables);
  }
}
