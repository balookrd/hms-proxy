package io.github.mmalykhin.hmsproxy.config.restcatalog;

import java.util.List;

public record RestCatalogConfig(
    boolean enabled,
    String bindHost,
    int port,
    int minWorkerThreads,
    int maxWorkerThreads,
    String kerberosPrincipal,
    String kerberosKeytab,
    RestCatalogPurgeMode purgeMode,
    List<String> purgeAllowedPrefixes
) {
  public RestCatalogConfig {
    purgeMode = purgeMode == null ? RestCatalogPurgeMode.ALLOW : purgeMode;
    purgeAllowedPrefixes = purgeAllowedPrefixes == null ? List.of() : List.copyOf(purgeAllowedPrefixes);
  }

  public static RestCatalogConfig disabled() {
    return new RestCatalogConfig(
        false, "0.0.0.0", 8181, 8, 64, null, null, RestCatalogPurgeMode.ALLOW, List.of());
  }

  public boolean kerberosEnabled() {
    return kerberosPrincipal != null && kerberosKeytab != null;
  }
}
