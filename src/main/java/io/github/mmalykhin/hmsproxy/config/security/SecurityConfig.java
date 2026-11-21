package io.github.mmalykhin.hmsproxy.config.security;

import java.util.Map;
import java.util.Set;

public record SecurityConfig(
    SecurityMode mode,
    String serverPrincipal,
    String clientPrincipal,
    String keytab,
    String clientKeytab,
    boolean impersonationEnabled,
    Map<String, String> frontDoorConf,
    GroupDiskCacheConfig groupDiskCache,
    Set<String> tokenStoreAdminPrincipals,
    int frontDoorMaxMemoryTokens
) {
  public static final int DEFAULT_FRONT_DOOR_MAX_MEMORY_TOKENS = 10_000;

  public SecurityConfig {
    frontDoorConf = Map.copyOf(frontDoorConf);
    groupDiskCache = groupDiskCache != null ? groupDiskCache : GroupDiskCacheConfig.disabled();
    tokenStoreAdminPrincipals = tokenStoreAdminPrincipals != null
        ? Set.copyOf(tokenStoreAdminPrincipals)
        : Set.of();
    if (frontDoorMaxMemoryTokens < 0) {
      throw new IllegalArgumentException(
          "security.front-door-max-memory-tokens must be >= 0, got: " + frontDoorMaxMemoryTokens);
    }
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
    this(
        mode,
        serverPrincipal,
        clientPrincipal,
        keytab,
        clientKeytab,
        impersonationEnabled,
        frontDoorConf,
        GroupDiskCacheConfig.disabled(),
        Set.of(),
        DEFAULT_FRONT_DOOR_MAX_MEMORY_TOKENS);
  }

  public SecurityConfig(
      SecurityMode mode,
      String serverPrincipal,
      String clientPrincipal,
      String keytab,
      String clientKeytab,
      boolean impersonationEnabled,
      Map<String, String> frontDoorConf,
      GroupDiskCacheConfig groupDiskCache
  ) {
    this(
        mode,
        serverPrincipal,
        clientPrincipal,
        keytab,
        clientKeytab,
        impersonationEnabled,
        frontDoorConf,
        groupDiskCache,
        Set.of(),
        DEFAULT_FRONT_DOOR_MAX_MEMORY_TOKENS);
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
