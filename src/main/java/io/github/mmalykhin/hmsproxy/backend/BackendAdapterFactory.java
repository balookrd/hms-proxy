package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.config.server.MetastoreRuntimeProfile;

final class BackendAdapterFactory {
  private BackendAdapterFactory() {
  }

  static BackendAdapter create(MetastoreRuntimeProfile runtimeProfile) {
    return switch (runtimeProfile) {
      case HORTONWORKS_3_1_0_3_1_0_78, HORTONWORKS_3_1_0_3_1_5_6150_1 ->
          new HortonworksBackendAdapter(runtimeProfile);
      case APACHE_3_1_3 -> new ApacheBackendAdapter();
      case APACHE_4_1_0 -> throw new UnsupportedOperationException(
          "APACHE_4_1_0 is supported as a front-door profile only; the backend must remain "
              + "APACHE_3_1_3 or HORTONWORKS_3_1_0_*");
    };
  }
}
