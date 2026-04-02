package io.github.mmalykhin.hmsproxy.config;

import io.github.mmalykhin.hmsproxy.compatibility.MetastoreRuntimeProfile;
import java.util.List;
import java.util.Map;

public record ProxyConfig(
    ServerConfig server,
    SecurityConfig security,
    String catalogDbSeparator,
    String defaultCatalog,
    Map<String, CatalogConfig> catalogs,
    BackendConfig backend,
    CompatibilityConfig compatibility,
    TransactionalDdlGuardConfig transactionalDdlGuard,
    ManagementConfig management
) {
  public ProxyConfig {
    catalogs = Map.copyOf(catalogs);
    backend = backend == null ? new BackendConfig(Map.of()) : backend;
    compatibility = compatibility == null
        ? new CompatibilityConfig(FrontendProfile.APACHE_3_1_3, false)
        : compatibility;
    transactionalDdlGuard = transactionalDdlGuard == null
        ? new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.DISABLED, List.of())
        : transactionalDdlGuard;
    management = management == null
        ? new ManagementConfig(false, server.bindHost(), server.port() + 1000)
        : management;
  }

  public ProxyConfig(
      ServerConfig server,
      SecurityConfig security,
      String catalogDbSeparator,
      String defaultCatalog,
      Map<String, CatalogConfig> catalogs
  ) {
    this(server, security, catalogDbSeparator, defaultCatalog, catalogs, new BackendConfig(Map.of()),
        new CompatibilityConfig(FrontendProfile.APACHE_3_1_3, false),
        new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.DISABLED, List.of()),
        new ManagementConfig(false, server.bindHost(), server.port() + 1000));
  }

  public ProxyConfig(
      ServerConfig server,
      SecurityConfig security,
      String catalogDbSeparator,
      String defaultCatalog,
      Map<String, CatalogConfig> catalogs,
      CompatibilityConfig compatibility
  ) {
    this(server, security, catalogDbSeparator, defaultCatalog, catalogs, new BackendConfig(Map.of()), compatibility,
        new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.DISABLED, List.of()),
        new ManagementConfig(false, server.bindHost(), server.port() + 1000));
  }

  public ProxyConfig(
      ServerConfig server,
      SecurityConfig security,
      String catalogDbSeparator,
      String defaultCatalog,
      Map<String, CatalogConfig> catalogs,
      BackendConfig backend,
      CompatibilityConfig compatibility
  ) {
    this(server, security, catalogDbSeparator, defaultCatalog, catalogs, backend, compatibility,
        new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.DISABLED, List.of()),
        new ManagementConfig(false, server.bindHost(), server.port() + 1000));
  }

  public ProxyConfig(
      ServerConfig server,
      SecurityConfig security,
      String catalogDbSeparator,
      String defaultCatalog,
      Map<String, CatalogConfig> catalogs,
      CompatibilityConfig compatibility,
      TransactionalDdlGuardConfig transactionalDdlGuard
  ) {
    this(server, security, catalogDbSeparator, defaultCatalog, catalogs, new BackendConfig(Map.of()), compatibility,
        transactionalDdlGuard,
        new ManagementConfig(false, server.bindHost(), server.port() + 1000));
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
      CatalogAccessMode accessMode,
      List<String> writeDbWhitelist,
      MetastoreRuntimeProfile runtimeProfile,
      String backendStandaloneMetastoreJar,
      Map<String, String> hiveConf
  ) {
    public CatalogConfig {
      accessMode = accessMode == null ? CatalogAccessMode.READ_WRITE : accessMode;
      writeDbWhitelist = List.copyOf(writeDbWhitelist);
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
      String frontendStandaloneMetastoreJar,
      String backendStandaloneMetastoreJar,
      boolean preserveBackendCatalogName
  ) {
    public CompatibilityConfig {
      frontendProfile = frontendProfile == null ? FrontendProfile.APACHE_3_1_3 : frontendProfile;
    }

    public CompatibilityConfig(FrontendProfile frontendProfile, boolean preserveBackendCatalogName) {
      this(frontendProfile, null, null, preserveBackendCatalogName);
    }

    public CompatibilityConfig(boolean preserveBackendCatalogName) {
      this(FrontendProfile.APACHE_3_1_3, null, null, preserveBackendCatalogName);
    }
  }

  public record TransactionalDdlGuardConfig(
      TransactionalDdlGuardMode mode,
      List<String> clientAddressRules
  ) {
    public TransactionalDdlGuardConfig {
      mode = mode == null ? TransactionalDdlGuardMode.DISABLED : mode;
      clientAddressRules = List.copyOf(clientAddressRules);
    }

    public boolean enabled() {
      return mode != TransactionalDdlGuardMode.DISABLED;
    }

    public boolean rewriteEnabled() {
      return mode == TransactionalDdlGuardMode.REWRITE;
    }

    public boolean rejectEnabled() {
      return mode == TransactionalDdlGuardMode.REJECT;
    }
  }

  public record ManagementConfig(
      boolean enabled,
      String bindHost,
      int port
  ) {
  }

  public enum TransactionalDdlGuardMode {
    DISABLED,
    REJECT,
    REWRITE
  }

  public enum CatalogAccessMode {
    READ_ONLY,
    READ_WRITE,
    READ_WRITE_DB_WHITELIST
  }

  public enum FrontendProfile {
    APACHE_3_1_3(MetastoreRuntimeProfile.APACHE_3_1_3),
    HORTONWORKS_3_1_0_3_1_0_78(MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78),
    HORTONWORKS_3_1_0_3_1_5_6150_1(MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_5_6150_1);

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
