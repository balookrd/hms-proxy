package io.github.mmalykhin.hmsproxy.config.security;

import java.util.Locale;
import java.util.Map;

import io.github.mmalykhin.hmsproxy.config.ConfigParsing;
import io.github.mmalykhin.hmsproxy.config.PropertyReader;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
public final class SecurityConfigParser {
  private SecurityConfigParser() {
  }

  public static SecurityConfig parse(
      PropertyReader reader,
      Map<String, CatalogConfig> catalogs
  ) {
    SecurityMode securityMode = SecurityMode.valueOf(
        reader.get("security.mode", "NONE").trim().toUpperCase(Locale.ROOT));
    String serverPrincipal = reader.getOrNull("security.server-principal");
    String clientPrincipal = reader.getOrNull("security.client-principal");
    String keytab = reader.getOrNull("security.keytab");
    String clientKeytab = reader.getOrNull("security.client-keytab");
    boolean impersonationEnabled = reader.getBoolean("security.impersonation-enabled", false);
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

  private static boolean backendKerberosEnabled(Map<String, String> hiveConf) {
    return Boolean.parseBoolean(PropertyReader.trimToNull(hiveConf.get("hive.metastore.sasl.enabled")));
  }
}
