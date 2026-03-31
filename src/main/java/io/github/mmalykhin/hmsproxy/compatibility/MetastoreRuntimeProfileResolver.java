package io.github.mmalykhin.hmsproxy.compatibility;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;

public final class MetastoreRuntimeProfileResolver {
  private MetastoreRuntimeProfileResolver() {
  }

  public static MetastoreRuntimeProfile forFrontendProfile(ProxyConfig.FrontendProfile frontendProfile) {
    return frontendProfile.runtimeProfile();
  }
}
