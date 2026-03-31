package io.github.mmalykhin.hmsproxy.compatibility;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;

public final class MetastoreRuntimeProfileResolver {
  private MetastoreRuntimeProfileResolver() {
  }

  public static MetastoreRuntimeProfile forFrontendProfile(ProxyConfig.FrontendProfile frontendProfile) {
    return frontendProfile.runtimeProfile();
  }

  public static MetastoreRuntimeProfile forBackendVersion(String backendVersion) {
    return switch (MetastoreCompatibility.backendProfile(backendVersion)) {
      case HORTONWORKS_3_1_0_LEGACY_REQUESTS -> MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78;
      case MODERN_REQUESTS, UNKNOWN -> MetastoreRuntimeProfile.APACHE_3_1_3;
    };
  }
}
