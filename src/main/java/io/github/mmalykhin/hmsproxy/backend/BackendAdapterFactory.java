package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.compatibility.MetastoreCompatibility;
import io.github.mmalykhin.hmsproxy.compatibility.MetastoreRuntimeProfile;

final class BackendAdapterFactory {
  private BackendAdapterFactory() {
  }

  static BackendAdapter create(MetastoreRuntimeProfile runtimeProfileOverride, String backendVersion) {
    if (runtimeProfileOverride != null) {
      return switch (runtimeProfileOverride) {
        case HORTONWORKS_3_1_0_3_1_0_78 -> new HortonworksBackendAdapter(backendVersion, runtimeProfileOverride);
        case APACHE_3_1_3 -> new ApacheBackendAdapter(backendVersion, runtimeProfileOverride);
      };
    }
    MetastoreCompatibility.BackendProfile profile = MetastoreCompatibility.backendProfile(backendVersion);
    return switch (profile) {
      case HORTONWORKS_3_1_0_LEGACY_REQUESTS -> new HortonworksBackendAdapter(backendVersion);
      case MODERN_REQUESTS, UNKNOWN -> new ApacheBackendAdapter(backendVersion);
    };
  }
}
