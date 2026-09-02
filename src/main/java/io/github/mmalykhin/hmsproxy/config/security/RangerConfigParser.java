package io.github.mmalykhin.hmsproxy.config.security;

import io.github.mmalykhin.hmsproxy.config.PropertyReader;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public final class RangerConfigParser {
  private RangerConfigParser() {
  }

  public static RangerConfig parse(PropertyReader reader, Set<String> catalogNames) {
    boolean globalEnabled = reader.getBoolean("ranger.enabled", false);
    String globalPolicyRestUrl = getFirst(reader, "ranger.policy.rest.url", "ranger.policy-rest-url", null);
    String globalServiceName = getFirst(reader, "ranger.service-name", "ranger.service.name", null);
    String globalServiceType = getFirst(reader, "ranger.service-type", "ranger.service.type", CatalogRangerConfig.DEFAULT_SERVICE_TYPE);
    String globalAppId = getFirst(reader, "ranger.app-id", "ranger.app.id", CatalogRangerConfig.DEFAULT_APP_ID);
    String globalPolicyCacheDir = getFirst(reader, "ranger.policy.cache.dir", "ranger.policy-cache-dir", null);
    long globalPolicyPollIntervalMs = getLongFirst(
        reader, "ranger.policy.poll-interval-ms", "ranger.policy-poll-interval-ms", CatalogRangerConfig.DEFAULT_POLL_INTERVAL_MS);
    int globalConnTimeoutMs = getIntFirst(
        reader, "ranger.policy.connection-timeout-ms", "ranger.client.connection-timeout-ms",
        getIntFirst(reader, "ranger.policy-connection-timeout-ms", null, CatalogRangerConfig.DEFAULT_CONNECTION_TIMEOUT_MS));
    int globalReadTimeoutMs = getIntFirst(
        reader, "ranger.policy.read-timeout-ms", "ranger.client.read-timeout-ms",
        getIntFirst(reader, "ranger.policy-read-timeout-ms", null, CatalogRangerConfig.DEFAULT_READ_TIMEOUT_MS));
    String globalSslTruststoreFile = getFirst(reader, "ranger.ssl.truststore.file", "ranger.ssl-truststore-file", null);
    String globalSslTruststorePassword = getFirst(reader, "ranger.ssl.truststore.password", "ranger.ssl-truststore-password", null);
    String globalConfigDir = getFirst(reader, "ranger.config-dir", "ranger.config.dir", null);
    boolean globalAuditEnabled = reader.getBoolean("ranger.audit.enabled", reader.getBoolean("ranger.audit-enabled", false));

    CatalogRangerConfig defaults = new CatalogRangerConfig(
        globalEnabled,
        globalPolicyRestUrl,
        globalServiceName,
        globalServiceType,
        globalAppId,
        globalPolicyCacheDir,
        globalPolicyPollIntervalMs,
        globalConnTimeoutMs,
        globalReadTimeoutMs,
        globalSslTruststoreFile,
        globalSslTruststorePassword,
        globalConfigDir,
        globalAuditEnabled
    );

    Map<String, CatalogRangerConfig> catalogConfigs = new LinkedHashMap<>();
    boolean anyEnabled = globalEnabled;

    if (catalogNames != null) {
      for (String catalogName : catalogNames) {
        String prefix = "catalog." + catalogName + ".ranger.";
        String altPrefix = "catalogs." + catalogName + ".ranger.";

        boolean hasCatalogSpecific = reader.namesWithPrefix(prefix).iterator().hasNext()
            || reader.namesWithPrefix(altPrefix).iterator().hasNext();

        boolean enabled = reader.getBoolean(
            prefix + "enabled",
            reader.getBoolean(altPrefix + "enabled", globalEnabled));

        if (enabled) {
          anyEnabled = true;
        }

        String policyRestUrl = getFirst(reader, prefix + "policy.rest.url", prefix + "policy-rest-url",
            getFirst(reader, altPrefix + "policy.rest.url", altPrefix + "policy-rest-url", globalPolicyRestUrl));
        String serviceName = getFirst(reader, prefix + "service-name", prefix + "service.name",
            getFirst(reader, altPrefix + "service-name", altPrefix + "service.name", globalServiceName));
        String serviceType = getFirst(reader, prefix + "service-type", prefix + "service.type",
            getFirst(reader, altPrefix + "service-type", altPrefix + "service.type", globalServiceType));
        String appId = getFirst(reader, prefix + "app-id", prefix + "app.id",
            getFirst(reader, altPrefix + "app-id", altPrefix + "app.id", globalAppId));
        String policyCacheDir = getFirst(reader, prefix + "policy.cache.dir", prefix + "policy-cache-dir",
            getFirst(reader, altPrefix + "policy.cache.dir", altPrefix + "policy-cache-dir", globalPolicyCacheDir));
        long pollIntervalMs = getLongFirst(reader, prefix + "policy.poll-interval-ms", prefix + "policy-poll-interval-ms",
            getLongFirst(reader, altPrefix + "policy.poll-interval-ms", altPrefix + "policy-poll-interval-ms", globalPolicyPollIntervalMs));
        int connTimeoutMs = getIntFirst(reader, prefix + "policy.connection-timeout-ms", prefix + "client.connection-timeout-ms",
            getIntFirst(reader, altPrefix + "policy.connection-timeout-ms", altPrefix + "client.connection-timeout-ms", globalConnTimeoutMs));
        int readTimeoutMs = getIntFirst(reader, prefix + "policy.read-timeout-ms", prefix + "client.read-timeout-ms",
            getIntFirst(reader, altPrefix + "policy.read-timeout-ms", altPrefix + "client.read-timeout-ms", globalReadTimeoutMs));
        String sslTruststoreFile = getFirst(reader, prefix + "ssl.truststore.file", prefix + "ssl-truststore-file",
            getFirst(reader, altPrefix + "ssl.truststore.file", altPrefix + "ssl-truststore-file", globalSslTruststoreFile));
        String sslTruststorePassword = getFirst(reader, prefix + "ssl.truststore.password", prefix + "ssl-truststore-password",
            getFirst(reader, altPrefix + "ssl.truststore.password", altPrefix + "ssl-truststore-password", globalSslTruststorePassword));
        String configDir = getFirst(reader, prefix + "config-dir", prefix + "config.dir",
            getFirst(reader, altPrefix + "config-dir", altPrefix + "config.dir", globalConfigDir));
        boolean auditEnabled = reader.getBoolean(
            prefix + "audit.enabled",
            reader.getBoolean(prefix + "audit-enabled",
                reader.getBoolean(altPrefix + "audit.enabled",
                    reader.getBoolean(altPrefix + "audit-enabled", globalAuditEnabled))));

        if (enabled || hasCatalogSpecific) {
          catalogConfigs.put(catalogName, new CatalogRangerConfig(
              enabled,
              policyRestUrl,
              serviceName,
              serviceType,
              appId,
              policyCacheDir,
              pollIntervalMs,
              connTimeoutMs,
              readTimeoutMs,
              sslTruststoreFile,
              sslTruststorePassword,
              configDir,
              auditEnabled
          ));
        }
      }
    }

    return new RangerConfig(anyEnabled, defaults, catalogConfigs);
  }

  private static String getFirst(PropertyReader reader, String key1, String key2, String defaultVal) {
    if (key1 != null) {
      String v1 = reader.getOrNull(key1);
      if (v1 != null) {
        return v1;
      }
    }
    if (key2 != null) {
      String v2 = reader.getOrNull(key2);
      if (v2 != null) {
        return v2;
      }
    }
    return defaultVal;
  }

  private static long getLongFirst(PropertyReader reader, String key1, String key2, long defaultVal) {
    if (key1 != null && reader.has(key1)) {
      return reader.getPositiveLong(key1, defaultVal);
    }
    if (key2 != null && reader.has(key2)) {
      return reader.getPositiveLong(key2, defaultVal);
    }
    return defaultVal;
  }

  private static int getIntFirst(PropertyReader reader, String key1, String key2, int defaultVal) {
    if (key1 != null && reader.has(key1)) {
      return reader.getPositiveInt(key1, defaultVal);
    }
    if (key2 != null && reader.has(key2)) {
      return reader.getPositiveInt(key2, defaultVal);
    }
    return defaultVal;
  }
}
