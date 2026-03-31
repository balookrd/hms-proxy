package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.compatibility.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreHandler;
import java.lang.reflect.Method;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;

public final class ApacheBackendAdapter extends AbstractBackendAdapter {
  public ApacheBackendAdapter(String backendVersion) {
    super(backendVersion);
  }

  public ApacheBackendAdapter(String backendVersion, MetastoreRuntimeProfile runtimeProfileOverride) {
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
