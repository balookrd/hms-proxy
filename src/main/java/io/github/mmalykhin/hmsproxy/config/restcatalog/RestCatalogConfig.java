package io.github.mmalykhin.hmsproxy.config.restcatalog;

public record RestCatalogConfig(
    boolean enabled,
    String bindHost,
    int port,
    int minWorkerThreads,
    int maxWorkerThreads,
    String kerberosPrincipal,
    String kerberosKeytab
) {
  public static RestCatalogConfig disabled() {
    return new RestCatalogConfig(false, "0.0.0.0", 8181, 8, 64, null, null);
  }

  public boolean kerberosEnabled() {
    return kerberosPrincipal != null && kerberosKeytab != null;
  }
}
