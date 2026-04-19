package io.github.mmalykhin.hmsproxy.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

public final class ProxyConfigLoader {
  private ProxyConfigLoader() {
  }

  public static ProxyConfig load(Path path) throws IOException {
    Properties properties = new Properties();
    try (InputStream input = Files.newInputStream(path)) {
      properties.load(input);
    }
    PropertyReader reader = new PropertyReader(properties);

    ProxyConfig.ServerConfig server = ServerConfigParser.parse(reader);
    String catalogDbSeparator = loadCatalogDbSeparator(reader);
    ProxyConfig.CompatibilityConfig compatibility = CompatibilityConfigParser.parse(reader);
    Map<String, String> backendConf = CatalogConfigParser.parseBackendConf(reader);
    boolean globalImpersonation = reader.getBoolean("security.impersonation-enabled", false);
    Map<String, ProxyConfig.CatalogConfig> catalogs =
        CatalogConfigParser.parse(reader, backendConf, globalImpersonation);
    String defaultCatalog = CatalogConfigParser.resolveDefaultCatalog(reader, catalogs);
    ProxyConfig.SecurityConfig security = SecurityConfigParser.parse(reader, catalogs);
    ProxyConfig.FederationConfig federation =
        FederationConfigParser.parse(reader, catalogs.get(defaultCatalog));
    ProxyConfig.TransactionalDdlGuardConfig transactionalDdlGuard =
        TransactionalDdlGuardConfigParser.parse(reader);
    ProxyConfig.ManagementConfig management = ManagementConfigParser.parse(reader, server);
    ProxyConfig.SyntheticReadLockStoreConfig syntheticReadLockStore =
        SyntheticReadLockStoreConfigParser.parse(reader);
    ProxyConfig.RateLimitConfig rateLimit = RateLimitConfigParser.parse(reader, catalogs);
    ProxyConfig.LatencyRoutingConfig latencyRouting =
        LatencyRoutingConfigParser.parse(reader, catalogs.size());

    return ProxyConfig.builder()
        .server(server)
        .security(security)
        .catalogDbSeparator(catalogDbSeparator)
        .defaultCatalog(defaultCatalog)
        .catalogs(catalogs)
        .backend(new ProxyConfig.BackendConfig(backendConf))
        .compatibility(compatibility)
        .federation(federation)
        .transactionalDdlGuard(transactionalDdlGuard)
        .management(management)
        .syntheticReadLockStore(syntheticReadLockStore)
        .rateLimit(rateLimit)
        .latencyRouting(latencyRouting)
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
