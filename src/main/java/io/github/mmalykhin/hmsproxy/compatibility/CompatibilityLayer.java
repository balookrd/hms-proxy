package io.github.mmalykhin.hmsproxy.compatibility;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.security.FrontDoorSecurity;
import java.util.Map;
import java.util.Optional;

public final class CompatibilityLayer {
  private final ProxyConfig config;
  private final FrontDoorSecurity frontDoorSecurity;

  public CompatibilityLayer(ProxyConfig config, FrontDoorSecurity frontDoorSecurity) {
    this.config = config;
    this.frontDoorSecurity = frontDoorSecurity;
  }

  public String frontendVersion() {
    return config.compatibility().frontendProfile().metastoreVersion();
  }

  public MetastoreRuntimeProfile frontendRuntimeProfile() {
    return config.compatibility().frontendProfile().runtimeProfile();
  }

  public Object handleLocalMethod(String methodName, Object[] args) throws Exception {
    return MetastoreCompatibility.handleLocally(methodName, args, frontDoorSecurity);
  }

  public Optional<Object> fallback(String methodName, Throwable cause) {
    return MetastoreCompatibility.fallback(methodName, cause);
  }

  public boolean shouldUseFallback(String methodName, Throwable cause) {
    return MetastoreCompatibility.shouldUseFallback(methodName, cause);
  }

  public Optional<String> compatibleConfigValue(String requestedName, String defaultValue, Map<String, String> hiveConf) {
    return MetastoreCompatibility.compatibleConfigValue(requestedName, defaultValue, hiveConf);
  }
}
