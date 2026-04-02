package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.compatibility.MetastoreCompatibility;
import io.github.mmalykhin.hmsproxy.compatibility.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreHandler;
import java.lang.reflect.Method;

public interface BackendAdapter {
  Object invoke(
      CatalogBackend backend,
      Method method,
      Object[] args,
      RoutingMetaStoreHandler.ImpersonationContext impersonation
  ) throws Throwable;

  Object invokeRequest(
      CatalogBackend backend,
      String methodName,
      Object request,
      RoutingMetaStoreHandler.ImpersonationContext impersonation
  ) throws Throwable;

  MetastoreCompatibility.BackendProfile backendProfile();

  MetastoreRuntimeProfile runtimeProfile();

  String backendVersion();
}
