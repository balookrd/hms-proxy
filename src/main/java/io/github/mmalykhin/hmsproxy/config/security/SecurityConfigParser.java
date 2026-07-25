package io.github.mmalykhin.hmsproxy.config.security;

import java.util.Map;

import io.github.mmalykhin.hmsproxy.config.ConfigParsing;
import io.github.mmalykhin.hmsproxy.config.PropertyReader;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
public final class SecurityConfigParser {
  private SecurityConfigParser() {
  }

  /**
   * {@code impersonationEnabled} is the already-parsed {@code security.impersonation-enabled} flag,
   * passed in rather than re-read here so the global default for
   * {@code catalog.<name>.impersonation-enabled} has a single source of truth.
   */
  public static SecurityConfig parse(
      PropertyReader reader,
      Map<String, CatalogConfig> catalogs,
      boolean impersonationEnabled
  ) {
    SecurityMode securityMode = ConfigParsing.parseEnum(
        SecurityMode.class, reader.getOrNull("security.mode"), "security.mode", SecurityMode.NONE);
    String serverPrincipal = reader.getOrNull("security.server-principal");
    String clientPrincipal = reader.getOrNull("security.client-principal");
    String keytab = reader.getOrNull("security.keytab");
    String clientKeytab = reader.getOrNull("security.client-keytab");
    Map<String, String> frontDoorConf = reader.collectPrefixed("security.front-door-conf.");

    if (clientPrincipal == null && serverPrincipal != null) {
      clientPrincipal = serverPrincipal;
    }
    if (clientKeytab == null && keytab != null) {
      clientKeytab = keytab;
    }
    if (securityMode == SecurityMode.KERBEROS) {
      ConfigParsing.requireNonBlank(serverPrincipal, "security.server-principal");
      ConfigParsing.requireNonBlank(keytab, "security.keytab");
      ConfigParsing.requireReadableFile(keytab, "security.keytab");
    }
    if (catalogs.values().stream().anyMatch(CatalogConfig::impersonationEnabled)
        && securityMode != SecurityMode.KERBEROS) {
      throw new IllegalArgumentException(
          "security.impersonation-enabled and catalog.<name>.impersonation-enabled "
              + "require security.mode=KERBEROS so the proxy can derive the caller identity from SASL");
    }
    if (catalogs.values().stream().anyMatch(catalog -> backendKerberosEnabled(catalog.hiveConf()))) {
      ConfigParsing.requireNonBlank(clientPrincipal, "security.client-principal");
      ConfigParsing.requireNonBlank(clientKeytab, "security.client-keytab");
      ConfigParsing.requireReadableFile(clientKeytab, "security.client-keytab");
    }
    return new SecurityConfig(
        securityMode,
        serverPrincipal,
        clientPrincipal,
        keytab,
        clientKeytab,
        impersonationEnabled,
        frontDoorConf);
  }

  /** Hive-owned key, so it keeps Hive's lenient {@code Boolean.parseBoolean} semantics. */
  private static boolean backendKerberosEnabled(Map<String, String> hiveConf) {
    return Boolean.parseBoolean(PropertyReader.trimToNull(hiveConf.get("hive.metastore.sasl.enabled")));
  }
}
