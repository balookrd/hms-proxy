package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.compatibility.MetastoreCompatibility;
import io.github.mmalykhin.hmsproxy.compatibility.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.compatibility.MetastoreRuntimeProfileResolver;
import io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreHandler;
import java.lang.reflect.Method;
import java.util.Optional;

public abstract class AbstractBackendAdapter implements BackendAdapter {
  private volatile String backendVersion;
  private volatile MetastoreCompatibility.BackendProfile backendProfile;
  private volatile MetastoreRuntimeProfile runtimeProfile;
  private final MetastoreRuntimeProfile runtimeProfileOverride;

  protected AbstractBackendAdapter(String backendVersion) {
    this(backendVersion, null);
  }

  protected AbstractBackendAdapter(String backendVersion, MetastoreRuntimeProfile runtimeProfileOverride) {
    this.backendVersion = backendVersion;
    this.backendProfile = MetastoreCompatibility.backendProfile(backendVersion);
    this.runtimeProfileOverride = runtimeProfileOverride;
    this.runtimeProfile = resolveRuntimeProfile(backendVersion);
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
    return backendVersion;
  }

  @Override
  public void updateBackendVersion(String backendVersion) {
    this.backendVersion = backendVersion;
    this.backendProfile = MetastoreCompatibility.backendProfile(backendVersion);
    this.runtimeProfile = resolveRuntimeProfile(backendVersion);
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
    backend.logLegacyRequestApiCompatibilitySwitch(backendVersion);
  }

  private MetastoreRuntimeProfile resolveRuntimeProfile(String backendVersion) {
    return runtimeProfileOverride != null
        ? runtimeProfileOverride
        : MetastoreRuntimeProfileResolver.forBackendVersion(backendVersion);
  }
}
