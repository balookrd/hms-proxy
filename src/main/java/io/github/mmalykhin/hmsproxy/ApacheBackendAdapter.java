package io.github.mmalykhin.hmsproxy;

import java.lang.reflect.Method;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;

final class ApacheBackendAdapter extends AbstractBackendAdapter {
  ApacheBackendAdapter(String backendVersion) {
    super(backendVersion);
  }

  ApacheBackendAdapter(String backendVersion, MetastoreRuntimeProfile runtimeProfileOverride) {
    super(backendVersion, runtimeProfileOverride);
  }

  @Override
  public Object invokeGetTableReq(
      CatalogBackend backend,
      GetTableRequest request,
      RoutingMetaStoreHandler.ImpersonationContext impersonation
  ) throws Throwable {
    Method method = org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface.class
        .getMethod("get_table_req", GetTableRequest.class);
    return invoke(backend, method, new Object[] {request}, impersonation);
  }

  @Override
  public Object invokeGetTablesReq(
      CatalogBackend backend,
      GetTablesRequest request,
      RoutingMetaStoreHandler.ImpersonationContext impersonation
  ) throws Throwable {
    Method method = org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface.class
        .getMethod("get_table_objects_by_name_req", GetTablesRequest.class);
    return invoke(backend, method, new Object[] {request}, impersonation);
  }
}
