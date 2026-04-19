package io.github.mmalykhin.hmsproxy.config;

final class CompatibilityConfigParser {
  private CompatibilityConfigParser() {
  }

  static ProxyConfig.CompatibilityConfig parse(PropertyReader reader) {
    ProxyConfig.FrontendProfile frontendProfile = ProxyConfig.FrontendProfile.valueOf(
        reader.get("compatibility.frontend-profile", "APACHE_3_1_3").trim().toUpperCase());
    String frontendStandaloneMetastoreJar = reader.getOrNull("compatibility.frontend-standalone-metastore-jar");
    if (frontendStandaloneMetastoreJar == null) {
      frontendStandaloneMetastoreJar = reader.getOrNull("compatibility.hortonworks-standalone-metastore-jar");
    }
    String backendStandaloneMetastoreJar = reader.getOrNull("compatibility.backend-standalone-metastore-jar");
    boolean preserveBackendCatalogName = reader.getBoolean("federation.preserve-backend-catalog-name", false);
    return new ProxyConfig.CompatibilityConfig(
        frontendProfile,
        frontendStandaloneMetastoreJar,
        backendStandaloneMetastoreJar,
        preserveBackendCatalogName);
  }
}
