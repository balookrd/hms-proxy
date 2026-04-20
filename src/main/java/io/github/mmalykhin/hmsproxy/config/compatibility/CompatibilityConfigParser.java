package io.github.mmalykhin.hmsproxy.config.compatibility;


import io.github.mmalykhin.hmsproxy.config.PropertyReader;
import io.github.mmalykhin.hmsproxy.config.server.FrontendProfile;
public final class CompatibilityConfigParser {
  private CompatibilityConfigParser() {
  }

  public static CompatibilityConfig parse(PropertyReader reader) {
    FrontendProfile frontendProfile = FrontendProfile.valueOf(
        reader.get("compatibility.frontend-profile", "APACHE_3_1_3").trim().toUpperCase());
    String frontendStandaloneMetastoreJar = reader.getOrNull("compatibility.frontend-standalone-metastore-jar");
    if (frontendStandaloneMetastoreJar == null) {
      frontendStandaloneMetastoreJar = reader.getOrNull("compatibility.hortonworks-standalone-metastore-jar");
    }
    String backendStandaloneMetastoreJar = reader.getOrNull("compatibility.backend-standalone-metastore-jar");
    boolean preserveBackendCatalogName = reader.getBoolean("federation.preserve-backend-catalog-name", false);
    return new CompatibilityConfig(
        frontendProfile,
        frontendStandaloneMetastoreJar,
        backendStandaloneMetastoreJar,
        preserveBackendCatalogName);
  }
}
