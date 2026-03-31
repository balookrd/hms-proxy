package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.compatibility.MetastoreRuntimeProfile;

final class BackendAdapterFactory {
  private BackendAdapterFactory() {
  }

  static BackendAdapter create(MetastoreRuntimeProfile runtimeProfile) {
    return switch (runtimeProfile) {
      case HORTONWORKS_3_1_0_3_1_0_78 -> new HortonworksBackendAdapter();
      case APACHE_3_1_3 -> new ApacheBackendAdapter();
    };
  }
}
