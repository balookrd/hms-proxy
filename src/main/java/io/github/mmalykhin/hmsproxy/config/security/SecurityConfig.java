package io.github.mmalykhin.hmsproxy.config.security;

import java.util.Map;

public record SecurityConfig(
    SecurityMode mode,
    String serverPrincipal,
    String clientPrincipal,
    String keytab,
    String clientKeytab,
    boolean impersonationEnabled,
    Map<String, String> frontDoorConf,
    GroupDiskCacheConfig groupDiskCache
) {
  public SecurityConfig {
    frontDoorConf = Map.copyOf(frontDoorConf);
    groupDiskCache = groupDiskCache != null ? groupDiskCache : GroupDiskCacheConfig.disabled();
  }

  public SecurityConfig(
      SecurityMode mode,
      String serverPrincipal,
      String clientPrincipal,
      String keytab,
      String clientKeytab,
      boolean impersonationEnabled,
      Map<String, String> frontDoorConf
  ) {
    this(mode, serverPrincipal, clientPrincipal, keytab, clientKeytab, impersonationEnabled, frontDoorConf, GroupDiskCacheConfig.disabled());
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
