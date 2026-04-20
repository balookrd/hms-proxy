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

    ServerConfig server = ServerConfigParser.parse(reader);
    String catalogDbSeparator = loadCatalogDbSeparator(reader);
    CompatibilityConfig compatibility = CompatibilityConfigParser.parse(reader);
    Map<String, String> backendConf = CatalogConfigParser.parseBackendConf(reader);
    boolean globalImpersonation = reader.getBoolean("security.impersonation-enabled", false);
    Map<String, CatalogConfig> catalogs =
        CatalogConfigParser.parse(reader, backendConf, globalImpersonation);
    String defaultCatalog = CatalogConfigParser.resolveDefaultCatalog(reader, catalogs);
    SecurityConfig security = SecurityConfigParser.parse(reader, catalogs);
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
