package io.github.mmalykhin.hmsproxy.compatibility;

import io.github.mmalykhin.hmsproxy.config.server.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.server.FrontendProfile;

public final class MetastoreRuntimeProfileResolver {
  private MetastoreRuntimeProfileResolver() {
  }

  public static MetastoreRuntimeProfile forFrontendProfile(FrontendProfile frontendProfile) {
    return frontendProfile.runtimeProfile();
  }
}
