package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.compatibility.MetastoreCompatibility;
import io.github.mmalykhin.hmsproxy.compatibility.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreHandler;
import java.lang.reflect.Method;

public abstract class AbstractBackendAdapter implements BackendAdapter {
  private final MetastoreCompatibility.BackendProfile backendProfile;
  private final MetastoreRuntimeProfile runtimeProfile;

  protected AbstractBackendAdapter(MetastoreRuntimeProfile runtimeProfile) {
    this.runtimeProfile = runtimeProfile;
    this.backendProfile = runtimeProfile == MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78
        ? MetastoreCompatibility.BackendProfile.HORTONWORKS_3_1_0_LEGACY_REQUESTS
        : MetastoreCompatibility.BackendProfile.MODERN_REQUESTS;
  }

  @Override
  public Object invoke(
      CatalogBackend backend,
      Method method,
      Object[] args,
      RoutingMetaStoreHandler.ImpersonationContext impersonation
  ) throws Throwable {
    return backend.invokeRaw(method, args, impersonation);
  }

  @Override
  public MetastoreCompatibility.BackendProfile backendProfile() {
    return backendProfile;
  }

  @Override
  public MetastoreRuntimeProfile runtimeProfile() {
    return runtimeProfile;
  }

  @Override
  public String backendVersion() {
    return null;
  }
}
