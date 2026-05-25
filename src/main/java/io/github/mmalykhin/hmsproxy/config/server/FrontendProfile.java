package io.github.mmalykhin.hmsproxy.config.server;

public enum FrontendProfile {
  APACHE_3_1_3(MetastoreRuntimeProfile.APACHE_3_1_3),
  HORTONWORKS_3_1_0_3_1_0_78(MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78),
  HORTONWORKS_3_1_0_3_1_5_6150_1(MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_5_6150_1),
  APACHE_4_1_0(MetastoreRuntimeProfile.APACHE_4_1_0);

  private final MetastoreRuntimeProfile runtimeProfile;

  FrontendProfile(MetastoreRuntimeProfile runtimeProfile) {
    this.runtimeProfile = runtimeProfile;
  }

  public MetastoreRuntimeProfile runtimeProfile() {
    return runtimeProfile;
  }

  public String metastoreVersion() {
    return runtimeProfile.metastoreVersion();
  }

  public String defaultStandaloneMetastoreJar() {
    return runtimeProfile.defaultStandaloneMetastoreJar();
  }
}
