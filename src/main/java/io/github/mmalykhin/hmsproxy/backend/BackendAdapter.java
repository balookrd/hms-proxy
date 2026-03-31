package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.compatibility.MetastoreCompatibility;
import io.github.mmalykhin.hmsproxy.compatibility.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreHandler;
import java.lang.reflect.Method;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;

public interface BackendAdapter {
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
