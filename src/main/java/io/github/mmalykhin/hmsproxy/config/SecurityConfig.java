package io.github.mmalykhin.hmsproxy.config;

import java.util.Map;

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
