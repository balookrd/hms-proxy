package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.compatibility.MetastoreCompatibility;
import io.github.mmalykhin.hmsproxy.compatibility.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreHandler;
import java.lang.reflect.Method;
import java.util.Optional;

public abstract class AbstractBackendAdapter implements BackendAdapter {
  private volatile MetastoreCompatibility.BackendProfile backendProfile;
  private volatile MetastoreRuntimeProfile runtimeProfile;

  protected AbstractBackendAdapter(MetastoreRuntimeProfile runtimeProfile) {
    this.runtimeProfile = runtimeProfile;
    this.backendProfile = backendProfileFor(runtimeProfile);
  }

  @Override
  public Object invoke(
      CatalogBackend backend,
      Method method,
      Object[] args,
      RoutingMetaStoreHandler.ImpersonationContext impersonation
  ) throws Throwable {
    try {
      return backend.invokeRaw(method, args, impersonation);
    } catch (Throwable cause) {
      Optional<Object> downgradedResult = MetastoreCompatibility.downgradeRequest(
          method.getName(),
          args,
          (legacyMethodName, parameterTypes, legacyArgs) ->
              backend.invokeRawByName(legacyMethodName, parameterTypes, legacyArgs, impersonation),
          cause);
      if (downgradedResult.isPresent()) {
        rememberLegacyRequestApi(backend);
        return downgradedResult.get();
      }
      throw cause;
    }
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

  protected boolean usesLegacyRequestApi() {
    return MetastoreCompatibility.usesLegacyRequestApi(backendProfile);
  }

  protected void rememberLegacyRequestApi(CatalogBackend backend) {
    if (usesLegacyRequestApi()) {
      return;
    }
    backendProfile = MetastoreCompatibility.BackendProfile.HORTONWORKS_3_1_0_LEGACY_REQUESTS;
    runtimeProfile = MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78;
    backend.logLegacyRequestApiCompatibilitySwitch();
  }

  private static MetastoreCompatibility.BackendProfile backendProfileFor(MetastoreRuntimeProfile runtimeProfile) {
    return runtimeProfile == MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78
        ? MetastoreCompatibility.BackendProfile.HORTONWORKS_3_1_0_LEGACY_REQUESTS
        : MetastoreCompatibility.BackendProfile.MODERN_REQUESTS;
  }
}
