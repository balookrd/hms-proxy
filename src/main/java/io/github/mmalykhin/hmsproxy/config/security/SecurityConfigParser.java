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
    if (clientPrincipal == null) {
      clientPrincipal = reader.getOrNull("security.outbound-principal");
    }
    String keytab = reader.getOrNull("security.keytab");
    String clientKeytab = reader.getOrNull("security.client-keytab");
    if (clientKeytab == null) {
      clientKeytab = reader.getOrNull("security.outbound-keytab");
    }
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
    if (catalogs.values().stream().anyMatch(catalog -> backendKerberosEnabled(catalog.hiveConf()))) {
      ConfigParsing.requireNonBlank(clientPrincipal, "security.client-principal");
      ConfigParsing.requireNonBlank(clientKeytab, "security.client-keytab");
      ConfigParsing.requireReadableFile(clientKeytab, "security.client-keytab");
    }

    boolean groupDiskCacheEnabled = reader.getBoolean("security.group-disk-cache.enabled", false);
    String groupDiskCachePath = reader.get("security.group-disk-cache.path", GroupDiskCacheConfig.DEFAULT_PATH);
    long groupDiskCacheEntryTtlSeconds = reader.getNonNegativeLong(
        "security.group-disk-cache.entry-ttl-seconds", GroupDiskCacheConfig.DEFAULT_ENTRY_TTL_SECONDS);
    long groupDiskCachePersistIntervalSeconds = reader.getNonNegativeLong(
        "security.group-disk-cache.persist-interval-seconds", GroupDiskCacheConfig.DEFAULT_PERSIST_INTERVAL_SECONDS);
    boolean groupDiskCachePersistOnShutdown = reader.getBoolean(
        "security.group-disk-cache.persist-on-shutdown", GroupDiskCacheConfig.DEFAULT_PERSIST_ON_SHUTDOWN);

    GroupDiskCacheConfig groupDiskCache = new GroupDiskCacheConfig(
        groupDiskCacheEnabled,
        groupDiskCachePath,
        groupDiskCacheEntryTtlSeconds,
        groupDiskCachePersistIntervalSeconds,
        groupDiskCachePersistOnShutdown);

    return new SecurityConfig(
        securityMode,
        serverPrincipal,
        clientPrincipal,
        keytab,
        clientKeytab,
        impersonationEnabled,
        frontDoorConf,
        groupDiskCache);
  }

  /** Hive-owned key, so it keeps Hive's lenient {@code Boolean.parseBoolean} semantics. */
  private static boolean backendKerberosEnabled(Map<String, String> hiveConf) {
    return Boolean.parseBoolean(PropertyReader.trimToNull(hiveConf.get("hive.metastore.sasl.enabled")));
  }
}
