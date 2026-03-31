package io.github.mmalykhin.hmsproxy;

import java.lang.reflect.Method;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;

interface BackendAdapter {
  Object invoke(
      CatalogBackend backend,
      Method method,
      Object[] args,
      RoutingMetaStoreHandler.ImpersonationContext impersonation
  ) throws Throwable;

  Object invokeGetTableReq(
      CatalogBackend backend,
      GetTableRequest request,
      RoutingMetaStoreHandler.ImpersonationContext impersonation
  ) throws Throwable;

  Object invokeGetTablesReq(
      CatalogBackend backend,
      GetTablesRequest request,
      RoutingMetaStoreHandler.ImpersonationContext impersonation
  ) throws Throwable;

  MetastoreCompatibility.BackendProfile backendProfile();

  MetastoreRuntimeProfile runtimeProfile();

  String backendVersion();

  void updateBackendVersion(String backendVersion);
}
