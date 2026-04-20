package io.github.mmalykhin.hmsproxy.config;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public record ProxyConfig(
    ServerConfig server,
    SecurityConfig security,
    String catalogDbSeparator,
    String defaultCatalog,
    Map<String, CatalogConfig> catalogs,
    BackendConfig backend,
    CompatibilityConfig compatibility,
    FederationConfig federation,
    TransactionalDdlGuardConfig transactionalDdlGuard,
    ManagementConfig management,
    SyntheticReadLockStoreConfig syntheticReadLockStore,
    RateLimitConfig rateLimit,
    LatencyRoutingConfig latencyRouting
) {
  public ProxyConfig {
    catalogs = Map.copyOf(catalogs);
    backend = backend == null ? new BackendConfig(Map.of()) : backend;
    compatibility = compatibility == null
        ? new CompatibilityConfig(FrontendProfile.APACHE_3_1_3, null, null, false)
        : compatibility;
    federation = federation == null
        ? new FederationConfig(compatibility.preserveBackendCatalogName(), ViewTextRewriteMode.DISABLED, false)
        : federation;
    transactionalDdlGuard = transactionalDdlGuard == null
        ? new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.DISABLED, List.of())
        : transactionalDdlGuard;
    management = management == null
        ? new ManagementConfig(false, server.bindHost(), server.port() + 1000)
        : management;
    Objects.requireNonNull(syntheticReadLockStore,
        "syntheticReadLockStore must be set explicitly: use SyntheticReadLockStoreConfig.inMemory() "
            + "for single-instance deployments or a ZOOKEEPER-backed config for HA.");
    rateLimit = rateLimit == null ? RateLimitConfig.disabled() : rateLimit;
    latencyRouting = latencyRouting == null ? LatencyRoutingConfig.disabled() : latencyRouting;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private ServerConfig server;
    private SecurityConfig security;
    private String catalogDbSeparator;
    private String defaultCatalog;
    private Map<String, CatalogConfig> catalogs;
    private BackendConfig backend;
    private CompatibilityConfig compatibility;
    private FederationConfig federation;
    private TransactionalDdlGuardConfig transactionalDdlGuard;
    private ManagementConfig management;
    private SyntheticReadLockStoreConfig syntheticReadLockStore;
    private RateLimitConfig rateLimit;
    private LatencyRoutingConfig latencyRouting;

    public Builder server(ServerConfig server) { this.server = server; return this; }
    public Builder security(SecurityConfig security) { this.security = security; return this; }
    public Builder catalogDbSeparator(String sep) { this.catalogDbSeparator = sep; return this; }
    public Builder defaultCatalog(String defaultCatalog) { this.defaultCatalog = defaultCatalog; return this; }
    public Builder catalogs(Map<String, CatalogConfig> catalogs) { this.catalogs = catalogs; return this; }
    public Builder backend(BackendConfig backend) { this.backend = backend; return this; }
    public Builder compatibility(CompatibilityConfig compatibility) { this.compatibility = compatibility; return this; }
    public Builder federation(FederationConfig federation) { this.federation = federation; return this; }
    public Builder transactionalDdlGuard(TransactionalDdlGuardConfig guard) { this.transactionalDdlGuard = guard; return this; }
    public Builder management(ManagementConfig management) { this.management = management; return this; }
    public Builder syntheticReadLockStore(SyntheticReadLockStoreConfig store) { this.syntheticReadLockStore = store; return this; }
    public Builder rateLimit(RateLimitConfig rateLimit) { this.rateLimit = rateLimit; return this; }
    public Builder latencyRouting(LatencyRoutingConfig latencyRouting) { this.latencyRouting = latencyRouting; return this; }

    public ProxyConfig build() {
      return new ProxyConfig(server, security, catalogDbSeparator, defaultCatalog, catalogs,
          backend, compatibility, federation, transactionalDdlGuard, management,
          syntheticReadLockStore, rateLimit, latencyRouting);
    }
  }

  public List<String> catalogNames() {
    return catalogs.keySet().stream().sorted().toList();
  }
}
