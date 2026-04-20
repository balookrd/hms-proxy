package io.github.mmalykhin.hmsproxy.compatibility;

import io.github.mmalykhin.hmsproxy.config.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.FrontendProfile;

public final class MetastoreRuntimeProfileResolver {
  private MetastoreRuntimeProfileResolver() {
  }

  public static MetastoreRuntimeProfile forFrontendProfile(FrontendProfile frontendProfile) {
    return frontendProfile.runtimeProfile();
  }
}
