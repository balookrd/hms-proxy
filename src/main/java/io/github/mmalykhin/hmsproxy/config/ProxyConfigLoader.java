package io.github.mmalykhin.hmsproxy.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfigParser;
import io.github.mmalykhin.hmsproxy.config.compatibility.CompatibilityConfig;
import io.github.mmalykhin.hmsproxy.config.compatibility.CompatibilityConfigParser;
import io.github.mmalykhin.hmsproxy.config.ddlguard.TransactionalDdlGuardConfig;
import io.github.mmalykhin.hmsproxy.config.ddlguard.TransactionalDdlGuardConfigParser;
import io.github.mmalykhin.hmsproxy.config.federation.FederationConfig;
import io.github.mmalykhin.hmsproxy.config.federation.FederationConfigParser;
import io.github.mmalykhin.hmsproxy.config.listener.AdditionalFrontendConfig;
import io.github.mmalykhin.hmsproxy.config.listener.AdditionalFrontendConfigParser;
import io.github.mmalykhin.hmsproxy.config.management.ManagementConfig;
import io.github.mmalykhin.hmsproxy.config.management.ManagementConfigParser;
import io.github.mmalykhin.hmsproxy.config.ratelimit.RateLimitConfig;
import io.github.mmalykhin.hmsproxy.config.ratelimit.RateLimitConfigParser;
import io.github.mmalykhin.hmsproxy.config.routing.BackendConfig;
import io.github.mmalykhin.hmsproxy.config.routing.LatencyRoutingConfig;
import io.github.mmalykhin.hmsproxy.config.routing.LatencyRoutingConfigParser;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfigParser;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfigParser;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfigParser;
public final class ProxyConfigLoader {
  private ProxyConfigLoader() {
  }

  public static ProxyConfig load(Path path) throws IOException {
    Properties properties = new Properties();
    try (InputStream input = Files.newInputStream(path)) {
      properties.load(input);
    }
    PropertyReader reader = new PropertyReader(properties);

    ServerConfig server = ServerConfigParser.parse(reader);
    String catalogDbSeparator = loadCatalogDbSeparator(reader);
    CompatibilityConfig compatibility = CompatibilityConfigParser.parse(reader);
    Map<String, String> backendConf = CatalogConfigParser.parseBackendConf(reader);
    // Read once: it is both the SecurityConfig flag and the default for per-catalog impersonation.
    boolean globalImpersonation = reader.getBoolean("security.impersonation-enabled", false);
    Map<String, CatalogConfig> catalogs =
        CatalogConfigParser.parse(reader, backendConf, globalImpersonation);
    String defaultCatalog = CatalogConfigParser.resolveDefaultCatalog(reader, catalogs);
    SecurityConfig security = SecurityConfigParser.parse(reader, catalogs, globalImpersonation);
    FederationConfig federation =
        FederationConfigParser.parse(reader, catalogs.get(defaultCatalog));
    TransactionalDdlGuardConfig transactionalDdlGuard =
        TransactionalDdlGuardConfigParser.parse(reader);
    ManagementConfig management = ManagementConfigParser.parse(reader, server);
    SyntheticReadLockStoreConfig syntheticReadLockStore =
        SyntheticReadLockStoreConfigParser.parse(reader);
    RateLimitConfig rateLimit = RateLimitConfigParser.parse(reader, catalogs);
    LatencyRoutingConfig latencyRouting =
        LatencyRoutingConfigParser.parse(reader, catalogs.size());
    List<AdditionalFrontendConfig> additionalFrontends =
        AdditionalFrontendConfigParser.parse(reader, server, management);

    return ProxyConfig.builder()
        .server(server)
        .security(security)
        .catalogDbSeparator(catalogDbSeparator)
        .defaultCatalog(defaultCatalog)
        .catalogs(catalogs)
        .backend(new BackendConfig(backendConf))
        .compatibility(compatibility)
        .federation(federation)
        .transactionalDdlGuard(transactionalDdlGuard)
        .management(management)
        .syntheticReadLockStore(syntheticReadLockStore)
        .rateLimit(rateLimit)
        .latencyRouting(latencyRouting)
        .additionalFrontends(additionalFrontends)
        .build();
  }

  private static String loadCatalogDbSeparator(PropertyReader reader) {
    String sep = reader.getOrNull("routing.catalog-db-separator");
    if (reader.has("routing.catalog-db-separator") && sep == null) {
      throw new IllegalArgumentException("routing.catalog-db-separator must not be blank");
    }
    return sep != null ? sep : ".";
  }
}
