package io.github.mmalykhin.hmsproxy;

import java.util.List;
import java.util.Map;

public record ProxyConfig(
    ServerConfig server,
    SecurityConfig security,
    String catalogDbSeparator,
    String defaultCatalog,
    Map<String, CatalogConfig> catalogs,
    BackendConfig backend,
    CompatibilityConfig compatibility
) {
  public ProxyConfig {
    catalogs = Map.copyOf(catalogs);
    backend = backend == null ? new BackendConfig(Map.of()) : backend;
    compatibility = compatibility == null
        ? new CompatibilityConfig(FrontendProfile.GNU_3_1_3, false)
        : compatibility;
  }

  public ProxyConfig(
      ServerConfig server,
      SecurityConfig security,
      String catalogDbSeparator,
      String defaultCatalog,
      Map<String, CatalogConfig> catalogs
  ) {
    this(server, security, catalogDbSeparator, defaultCatalog, catalogs, new BackendConfig(Map.of()),
        new CompatibilityConfig(FrontendProfile.GNU_3_1_3, false));
  }

  public ProxyConfig(
      ServerConfig server,
      SecurityConfig security,
      String catalogDbSeparator,
      String defaultCatalog,
      Map<String, CatalogConfig> catalogs,
      CompatibilityConfig compatibility
  ) {
    this(server, security, catalogDbSeparator, defaultCatalog, catalogs, new BackendConfig(Map.of()), compatibility);
  }

  public record ServerConfig(
      String name,
      String bindHost,
      int port,
      int minWorkerThreads,
      int maxWorkerThreads
  ) {
  }

  public record SecurityConfig(
      SecurityMode mode,
      String serverPrincipal,
      String clientPrincipal,
      String keytab,
      String clientKeytab,
      boolean impersonationEnabled,
      Map<String, String> frontDoorConf
  ) {
    public SecurityConfig {
      frontDoorConf = Map.copyOf(frontDoorConf);
    }

    public boolean kerberosEnabled() {
      return mode == SecurityMode.KERBEROS;
    }

    public String outboundPrincipal() {
      return clientPrincipal != null ? clientPrincipal : serverPrincipal;
    }

    public String outboundKeytab() {
      return clientKeytab != null ? clientKeytab : keytab;
    }
  }

  public record CatalogConfig(
      String name,
      String description,
      String locationUri,
      boolean impersonationEnabled,
      Map<String, String> hiveConf
  ) {
    public CatalogConfig {
      hiveConf = Map.copyOf(hiveConf);
    }
  }

  public record BackendConfig(
      Map<String, String> hiveConf
  ) {
    public BackendConfig {
      hiveConf = Map.copyOf(hiveConf);
    }
  }

  public record CompatibilityConfig(
      FrontendProfile frontendProfile,
      String hortonworksStandaloneMetastoreJar,
      boolean preserveBackendCatalogName
  ) {
    public CompatibilityConfig {
      frontendProfile = frontendProfile == null ? FrontendProfile.GNU_3_1_3 : frontendProfile;
    }

    public CompatibilityConfig(FrontendProfile frontendProfile, boolean preserveBackendCatalogName) {
      this(frontendProfile, null, preserveBackendCatalogName);
    }

    public CompatibilityConfig(boolean preserveBackendCatalogName) {
      this(FrontendProfile.GNU_3_1_3, null, preserveBackendCatalogName);
    }
  }

  public enum FrontendProfile {
    GNU_3_1_3("3.1.3"),
    HORTONWORKS_3_1_0_3_1_0_78("3.1.0.3.1.0.0-78");

    private final String metastoreVersion;

    FrontendProfile(String metastoreVersion) {
      this.metastoreVersion = metastoreVersion;
    }

    public String metastoreVersion() {
      return metastoreVersion;
    }
  }

  public enum SecurityMode {
    NONE,
    KERBEROS;

    public String hadoopAuthValue() {
      return this == KERBEROS ? "kerberos" : "simple";
    }
  }

  public List<String> catalogNames() {
    return catalogs.keySet().stream().sorted().toList();
  }
}
