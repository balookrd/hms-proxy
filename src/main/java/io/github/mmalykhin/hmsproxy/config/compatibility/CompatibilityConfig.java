package io.github.mmalykhin.hmsproxy.config.compatibility;


import io.github.mmalykhin.hmsproxy.config.server.FrontendProfile;
public record CompatibilityConfig(
    FrontendProfile frontendProfile,
    String frontendStandaloneMetastoreJar,
    String backendStandaloneMetastoreJar,
    boolean preserveBackendCatalogName
) {
  public CompatibilityConfig {
    frontendProfile = frontendProfile == null ? FrontendProfile.APACHE_3_1_3 : frontendProfile;
  }

  public CompatibilityConfig(FrontendProfile frontendProfile) {
    this(frontendProfile, null, null, false);
  }

  public CompatibilityConfig(FrontendProfile frontendProfile, boolean preserveBackendCatalogName) {
    this(frontendProfile, null, null, preserveBackendCatalogName);
  }

  public CompatibilityConfig(boolean preserveBackendCatalogName) {
    this(FrontendProfile.APACHE_3_1_3, null, null, preserveBackendCatalogName);
  }

}
