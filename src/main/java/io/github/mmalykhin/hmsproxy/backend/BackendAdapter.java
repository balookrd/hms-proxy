package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.compatibility.MetastoreCompatibility;
import io.github.mmalykhin.hmsproxy.compatibility.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.routing.ImpersonationContext;
import java.lang.reflect.Method;

public interface BackendAdapter {
  Object invoke(
      CatalogBackend backend,
      Method method,
      Object[] args,
      ImpersonationContext impersonation
  ) throws Throwable;

  Object invokeRequest(
      CatalogBackend backend,
      String methodName,
      Object request,
      ImpersonationContext impersonation
  ) throws Throwable;

  MetastoreCompatibility.BackendProfile backendProfile();

  MetastoreRuntimeProfile runtimeProfile();

  String backendVersion();
}
