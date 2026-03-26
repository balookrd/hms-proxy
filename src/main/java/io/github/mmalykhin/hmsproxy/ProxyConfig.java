package io.github.mmalykhin.hmsproxy;

import java.util.List;
import java.util.Map;

public record ProxyConfig(
    ServerConfig server,
    SecurityConfig security,
    String catalogDbSeparator,
    String defaultCatalog,
    Map<String, CatalogConfig> catalogs
) {
  public ProxyConfig {
    catalogs = Map.copyOf(catalogs);
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
      boolean impersonationEnabled
  ) {
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
