package io.github.mmalykhin.hmsproxy.config.security;

import java.util.Map;

public record RangerConfig(
    boolean enabled,
    CatalogRangerConfig defaults,
    Map<String, CatalogRangerConfig> catalogConfigs
) {
  public RangerConfig {
    defaults = defaults == null ? CatalogRangerConfig.disabled() : defaults;
    catalogConfigs = catalogConfigs == null ? Map.of() : Map.copyOf(catalogConfigs);
  }

  public static RangerConfig disabled() {
    return new RangerConfig(false, CatalogRangerConfig.disabled(), Map.of());
  }

  public CatalogRangerConfig forCatalog(String catalogName) {
    if (catalogName != null) {
      CatalogRangerConfig specific = catalogConfigs.get(catalogName);
      if (specific != null) {
        return specific;
      }
      return new CatalogRangerConfig(
          defaults.enabled(),
          defaults.policyRestUrl(),
          defaults.serviceName() != null ? defaults.serviceName() : catalogName,
          defaults.serviceType(),
          defaults.appId(),
          defaults.policyCacheDir(),
          defaults.policyPollIntervalMs(),
          defaults.connectionTimeoutMs(),
          defaults.readTimeoutMs(),
          defaults.sslTruststoreFile(),
          defaults.sslTruststorePassword(),
          defaults.configDir(),
          defaults.auditEnabled());
    }
    return defaults;
  }

  public String policyRestUrl() {
    return defaults.policyRestUrl();
  }

  public String serviceName() {
    return defaults.serviceName();
  }

  public String serviceType() {
    return defaults.serviceType();
  }

  public String appId() {
    return defaults.appId();
  }

  public String policyCacheDir() {
    return defaults.policyCacheDir();
  }

  public long policyPollIntervalMs() {
    return defaults.policyPollIntervalMs();
  }

  public int connectionTimeoutMs() {
    return defaults.connectionTimeoutMs();
  }

  public int readTimeoutMs() {
    return defaults.readTimeoutMs();
  }
}
