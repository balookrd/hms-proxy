package io.github.mmalykhin.hmsproxy.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfigParser;
import io.github.mmalykhin.hmsproxy.config.compatibility.CompatibilityConfig;
import io.github.mmalykhin.hmsproxy.config.compatibility.CompatibilityConfigParser;
import io.github.mmalykhin.hmsproxy.config.ddlguard.TransactionalDdlGuardConfig;
import io.github.mmalykhin.hmsproxy.config.ddlguard.TransactionalDdlGuardConfigParser;
import io.github.mmalykhin.hmsproxy.config.federation.FederationConfig;
import io.github.mmalykhin.hmsproxy.config.federation.FederationConfigParser;
import io.github.mmalykhin.hmsproxy.config.listener.AdditionalFrontendConfig;
import io.github.mmalykhin.hmsproxy.config.listener.AdditionalFrontendConfigParser;
import io.github.mmalykhin.hmsproxy.config.management.ManagementConfig;
import io.github.mmalykhin.hmsproxy.config.management.ManagementConfigParser;
import io.github.mmalykhin.hmsproxy.config.ratelimit.RateLimitConfig;
import io.github.mmalykhin.hmsproxy.config.ratelimit.RateLimitConfigParser;
import io.github.mmalykhin.hmsproxy.config.restcatalog.RestCatalogConfig;
import io.github.mmalykhin.hmsproxy.config.restcatalog.RestCatalogConfigParser;
import io.github.mmalykhin.hmsproxy.config.routing.BackendConfig;
import io.github.mmalykhin.hmsproxy.config.routing.IcebergPointerGuardConfig;
import io.github.mmalykhin.hmsproxy.config.routing.IcebergPointerGuardConfigParser;
import io.github.mmalykhin.hmsproxy.config.routing.LatencyRoutingConfig;
import io.github.mmalykhin.hmsproxy.config.routing.LatencyRoutingConfigParser;
import io.github.mmalykhin.hmsproxy.config.security.CatalogRangerConfig;
import io.github.mmalykhin.hmsproxy.config.security.RangerConfig;
import io.github.mmalykhin.hmsproxy.config.security.RangerConfigParser;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfigParser;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfigParser;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfigParser;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ProxyConfigLoader {
  private static final Logger LOG = LoggerFactory.getLogger(ProxyConfigLoader.class);
  private ProxyConfigLoader() {
  }

  public static ProxyConfig load(Path path) throws IOException {
    Properties properties = new Properties();
    try (InputStream input = Files.newInputStream(path)) {
      properties.load(input);
    }
    PropertyReader reader = new PropertyReader(properties);

    ServerConfig server = ServerConfigParser.parse(reader);
    String catalogDbSeparator = loadCatalogDbSeparator(reader);
    CompatibilityConfig compatibility = CompatibilityConfigParser.parse(reader);
    Map<String, String> backendConf = CatalogConfigParser.parseBackendConf(reader);
    // Read once: it is both the SecurityConfig flag and the default for per-catalog impersonation.
    boolean globalImpersonation = reader.getBoolean("security.impersonation-enabled", false);
    Map<String, CatalogConfig> catalogs =
        CatalogConfigParser.parse(reader, backendConf, globalImpersonation);
    RangerConfig ranger = RangerConfigParser.parse(reader, catalogs.keySet());
    if (ranger.enabled() || !ranger.catalogConfigs().isEmpty()) {
      Map<String, CatalogConfig> enrichedCatalogs = new java.util.LinkedHashMap<>();
      for (Map.Entry<String, CatalogConfig> entry : catalogs.entrySet()) {
        CatalogConfig orig = entry.getValue();
        CatalogRangerConfig catRanger = ranger.forCatalog(entry.getKey());
        enrichedCatalogs.put(entry.getKey(), new CatalogConfig(
            orig.name(),
            orig.description(),
            orig.locationUri(),
            orig.impersonationEnabled(),
            orig.accessMode(),
            orig.writeDbWhitelist(),
            orig.exposeMode(),
            orig.exposeDbPatterns(),
            orig.exposeTablePatterns(),
            orig.unprefixedDatabases(),
            orig.runtimeProfile(),
            orig.backendStandaloneMetastoreJar(),
            orig.hiveConf(),
            orig.latencyBudgetMs(),
            orig.maxImpersonationClients(),
            orig.impersonationClientIdleTtlMs(),
            orig.sharedSessionPoolSize(),
            orig.impersonationPoolMaxSize(),
            orig.impersonationSessionIdleTtlMs(),
            catRanger,
            orig.startupMode(),
            orig.requiredForReadiness(),
            orig.maxConcurrentCalls(),
            orig.concurrencyTimeoutMs(),
            orig.fallbackCatalog(),
            orig.fallbackOnOutage()
        ));
      }
      catalogs = enrichedCatalogs;
    }
    String defaultCatalog = CatalogConfigParser.resolveDefaultCatalog(reader, catalogs);
    SecurityConfig security = SecurityConfigParser.parse(reader, catalogs, globalImpersonation);
    FederationConfig federation =
        FederationConfigParser.parse(reader, catalogs.get(defaultCatalog));
    TransactionalDdlGuardConfig transactionalDdlGuard =
        TransactionalDdlGuardConfigParser.parse(reader);
    ManagementConfig management = ManagementConfigParser.parse(reader, server);
    RestCatalogConfig restCatalog = RestCatalogConfigParser.parse(reader, server);
    SyntheticReadLockStoreConfig syntheticReadLockStore =
        SyntheticReadLockStoreConfigParser.parse(reader);
    RateLimitConfig rateLimit = RateLimitConfigParser.parse(reader, catalogs);
    LatencyRoutingConfig latencyRouting =
        LatencyRoutingConfigParser.parse(reader, catalogs.size());
    IcebergPointerGuardConfig icebergPointerGuard = IcebergPointerGuardConfigParser.parse(reader);
    List<AdditionalFrontendConfig> additionalFrontends =
        AdditionalFrontendConfigParser.parse(reader, server, management);
    long reloadPollIntervalSeconds = reader.getNonNegativeLong(
        "config.reload.poll-interval-seconds", ProxyConfig.DEFAULT_RELOAD_POLL_INTERVAL_SECONDS);

    boolean strictValidation = reader.getBoolean("config.strict-validation", true);
    validateUnconsumedProperties(reader, strictValidation, catalogs.keySet(), additionalFrontends);

    return ProxyConfig.builder()
        .server(server)
        .security(security)
        .catalogDbSeparator(catalogDbSeparator)
        .defaultCatalog(defaultCatalog)
        .catalogs(catalogs)
        .backend(new BackendConfig(backendConf))
        .compatibility(compatibility)
        .federation(federation)
        .transactionalDdlGuard(transactionalDdlGuard)
        .management(management)
        .restCatalog(restCatalog)
        .syntheticReadLockStore(syntheticReadLockStore)
        .rateLimit(rateLimit)
        .latencyRouting(latencyRouting)
        .icebergPointerGuard(icebergPointerGuard)
        .additionalFrontends(additionalFrontends)
        .ranger(ranger)
        .reloadPollIntervalSeconds(reloadPollIntervalSeconds)
        .build();
  }

  private static String loadCatalogDbSeparator(PropertyReader reader) {
    String sep = reader.getOrNull("routing.catalog-db-separator");
    if (reader.has("routing.catalog-db-separator") && sep == null) {
      throw new IllegalArgumentException("routing.catalog-db-separator must not be blank");
    }
    return sep != null ? sep : ".";
  }

  private static void validateUnconsumedProperties(
      PropertyReader reader,
      boolean strictValidation,
      Set<String> catalogNames,
      List<AdditionalFrontendConfig> additionalFrontends
  ) {
    Set<String> unconsumed = reader.unconsumedKeys();
    if (unconsumed.isEmpty()) {
      return;
    }
    Set<String> knownCandidates = buildKnownCandidates(catalogNames, additionalFrontends);
    List<String> details = new ArrayList<>();
    for (String key : unconsumed) {
      String suggestion = findBestSuggestion(key, knownCandidates);
      if (suggestion != null) {
        details.add("'" + key + "' (did you mean '" + suggestion + "'?)");
      } else {
        details.add("'" + key + "'");
      }
    }
    String message;
    if (details.size() == 1) {
      message = "Unrecognized configuration property: " + details.get(0);
    } else {
      message = "Unrecognized configuration properties (" + details.size() + "):\n  - "
          + String.join("\n  - ", details);
    }
    if (strictValidation) {
      throw new IllegalArgumentException(message);
    } else {
      LOG.warn("{}", message);
    }
  }

  static String findBestSuggestion(String key, Set<String> candidates) {
    String best = null;
    int minDistance = Integer.MAX_VALUE;
    int maxAllowedDistance = Math.max(2, key.length() / 3);
    for (String candidate : candidates) {
      int dist = levenshteinDistance(key, candidate);
      if (dist <= maxAllowedDistance && dist < minDistance) {
        minDistance = dist;
        best = candidate;
      }
    }
    return best;
  }

  static int levenshteinDistance(String a, String b) {
    if (a.equals(b)) {
      return 0;
    }
    int lenA = a.length();
    int lenB = b.length();
    if (lenA == 0) {
      return lenB;
    }
    if (lenB == 0) {
      return lenA;
    }

    int[] prev = new int[lenB + 1];
    int[] curr = new int[lenB + 1];
    for (int j = 0; j <= lenB; j++) {
      prev[j] = j;
    }
    for (int i = 1; i <= lenA; i++) {
      curr[0] = i;
      char charA = a.charAt(i - 1);
      for (int j = 1; j <= lenB; j++) {
        int cost = (charA == b.charAt(j - 1)) ? 0 : 1;
        curr[j] = Math.min(Math.min(curr[j - 1] + 1, prev[j] + 1), prev[j - 1] + cost);
      }
      System.arraycopy(curr, 0, prev, 0, lenB + 1);
    }
    return prev[lenB];
  }

  private static Set<String> buildKnownCandidates(
      Set<String> catalogNames,
      List<AdditionalFrontendConfig> additionalFrontends
  ) {
    Set<String> candidates = new LinkedHashSet<>(STATIC_KNOWN_KEYS);
    for (String cat : catalogNames) {
      for (String suffix : PER_CATALOG_KNOWN_SUFFIXES) {
        candidates.add("catalog." + cat + "." + suffix);
      }
    }
    for (AdditionalFrontendConfig extra : additionalFrontends) {
      for (String suffix : PER_FRONTEND_KNOWN_SUFFIXES) {
        candidates.add("additional-frontends." + extra.name() + "." + suffix);
      }
    }
    return candidates;
  }

  private static final Set<String> STATIC_KNOWN_KEYS = Set.of(
      "config.strict-validation",
      "server.name",
      "server.bind-host",
      "server.port",
      "server.min-worker-threads",
      "server.max-worker-threads",
      "server.client-socket-timeout-ms",
      "server.tcp-keepalive",
      "server.tcp-keepalive-idle-seconds",
      "server.tcp-keepalive-interval-seconds",
      "server.tcp-keepalive-count",
      "server.shutdown-timeout-seconds",
      "catalogs",
      "routing.default-catalog",
      "routing.catalog-db-separator",
      "compatibility.frontend-profile",
      "compatibility.frontend-standalone-metastore-jar",
      "compatibility.hortonworks-standalone-metastore-jar",
      "compatibility.backend-standalone-metastore-jar",
      "federation.preserve-backend-catalog-name",
      "federation.view-text-rewrite.mode",
      "federation.view-text-rewrite.preserve-original-text",
      "federation.external-table-location-rewrite.mode",
      "federation.external-table-location-rewrite.source-default-fs",
      "federation.external-table-drop-purge.mode",
      "guard.transactional-ddl.mode",
      "guard.transactional-ddl.client-addresses",
      "management.enabled",
      "management.bind-host",
      "management.port",
      "management.threads",
      "management.readiness-cache-ms",
      "management.readyz.require-all-catalogs",
      "rest-catalog.enabled",
      "rest-catalog.bind-host",
      "rest-catalog.port",
      "rest-catalog.min-worker-threads",
      "rest-catalog.max-worker-threads",
      "rest-catalog.kerberos.principal",
      "rest-catalog.kerberos.keytab",
      "rest-catalog.purge.mode",
      "rest-catalog.purge.allowed-prefixes",
      "rest-catalog.hive-engine-descriptor",
      "synthetic-read-lock.store.mode",
      "synthetic-read-lock.store.zookeeper.connect-string",
      "synthetic-read-lock.store.zookeeper.znode",
      "synthetic-read-lock.store.zookeeper.connection-timeout-ms",
      "synthetic-read-lock.store.zookeeper.session-timeout-ms",
      "synthetic-read-lock.store.zookeeper.base-sleep-ms",
      "synthetic-read-lock.store.zookeeper.max-retries",
      "rate-limit.principal.requests-per-second",
      "rate-limit.principal.burst",
      "rate-limit.source.requests-per-second",
      "rate-limit.source.burst",
      "routing.iceberg-pointer-guard.enabled",
      "routing.iceberg-pointer-guard.table-cache-ttl-ms",
      "routing.iceberg-pointer-guard.table-cache-max-entries",
      "routing.iceberg-pointer-guard.lock-enabled",
      "routing.iceberg-pointer-guard.lock-acquire-timeout-ms",
      "routing.iceberg-pointer-guard.hive-engine-descriptor",
      "routing.backend-state-polling.enabled",
      "routing.backend-state-polling.interval-ms",
      "routing.backend-state-polling.probe-timeout-ms",
      "routing.backend-state-polling.max-parallelism",
      "routing.adaptive-timeout.enabled",
      "routing.adaptive-timeout.initial-ms",
      "routing.adaptive-timeout.min-ms",
      "routing.adaptive-timeout.max-ms",
      "routing.adaptive-timeout.multiplier",
      "routing.adaptive-timeout.alpha",
      "routing.adaptive-timeout.reconnect-cooldown-ms",
      "routing.circuit-breaker.enabled",
      "routing.circuit-breaker.failure-threshold",
      "routing.circuit-breaker.open-state-ms",
      "routing.hedged-read.enabled",
      "routing.hedged-read.max-parallelism",
      "routing.hedged-read.fanout-timeout-ms",
      "routing.degraded-routing-policy",
      "routing.database-cache.background-refresh.enabled",
      "routing.database-cache.background-refresh.interval-ms",
      "routing.database-cache.background-refresh.activity-window-ms",
      "routing.database-list-cache.ttl-ms",
      "routing.database-list-cache.max-entries",
      "routing.database-list-cache.shared-across-users",
      "routing.database-metadata-cache.ttl-ms",
      "routing.database-metadata-cache.max-entries",
      "routing.database-metadata-cache.shared-across-users",
      "routing.table-metadata-cache.ttl-ms",
      "routing.table-metadata-cache.max-entries",
      "routing.table-metadata-cache.shared-across-users",
      "routing.table-metadata-cache.enabled",
      "routing.cache.table-metadata.ttl-ms",
      "routing.cache.table-metadata.max-entries",
      "routing.cache.table-metadata.shared-across-users",
      "routing.cache.table-metadata.enabled",
      "routing.partition-metadata-cache.ttl-ms",
      "routing.partition-metadata-cache.max-entries",
      "routing.partition-metadata-cache.shared-across-users",
      "routing.partition-metadata-cache.enabled",
      "routing.cache.partition-metadata.ttl-ms",
      "routing.cache.partition-metadata.max-entries",
      "routing.cache.partition-metadata.shared-across-users",
      "routing.cache.partition-metadata.enabled",
      "routing.cache.serve-stale-on-error",
      "routing.cache.stale-grace-period-ms",
      "routing.cache.distributed-invalidation.mode",
      "routing.cache.distributed-invalidation.zookeeper.connect-string",
      "routing.cache.distributed-invalidation.zookeeper.znode",
      "routing.cache.distributed-invalidation.zookeeper.connection-timeout-ms",
      "routing.cache.distributed-invalidation.zookeeper.session-timeout-ms",
      "routing.cache.distributed-invalidation.zookeeper.base-sleep-ms",
      "routing.cache.distributed-invalidation.zookeeper.max-retries",
      "routing.cache.distributed-invalidation.zookeeper.event-retention-ms",
      "routing.cache.distributed-invalidation.zookeeper.event-retention-seconds",
      "routing.config-value-cache.ttl-ms",
      "routing.config-value-cache.max-entries",
      "routing.refresh-privileges.synthetic-success",
      "routing.refresh-privileges.mode",
      "security.mode",
      "security.server-principal",
      "security.client-principal",
      "security.outbound-principal",
      "security.keytab",
      "security.client-keytab",
      "security.outbound-keytab",
      "security.impersonation-enabled",
      "security.group-disk-cache.enabled",
      "security.group-disk-cache.path",
      "security.group-disk-cache.entry-ttl-seconds",
      "security.group-disk-cache.persist-interval-seconds",
      "security.group-disk-cache.persist-on-shutdown",
      "ranger.enabled",
      "ranger.policy.rest.url",
      "ranger.policy-rest-url",
      "ranger.service-name",
      "ranger.service-type",
      "ranger.app-id",
      "ranger.policy.cache.dir",
      "ranger.policy.poll-interval-ms",
      "ranger.policy.connection-timeout-ms",
      "ranger.policy.read-timeout-ms",
      "ranger.ssl.truststore.file",
      "ranger.ssl.truststore.password",
      "ranger.config-dir",
      "ranger.audit.enabled",
      "additional-frontends",
      "config.reload.poll-interval-seconds"
  );

  private static final Set<String> PER_CATALOG_KNOWN_SUFFIXES = Set.of(
      "description",
      "location-uri",
      "impersonation-enabled",
      "access-mode",
      "write-db-whitelist",
      "expose-mode",
      "expose-db-patterns",
      "unprefixed-databases",
      "runtime-profile",
      "backend-standalone-metastore-jar",
      "standalone-metastore-jar",
      "latency-budget-ms",
      "impersonation-max-clients",
      "impersonation-client-idle-ttl-ms",
      "shared-session-pool-size",
      "impersonation-pool-max-size",
      "impersonation-session-idle-ttl-ms",
      "startup-mode",
      "required-for-readiness",
      "max-concurrent-calls",
      "concurrency-timeout-ms",
      "fallback-catalog",
      "fallback-on-outage",
      "ranger.enabled",
      "ranger.policy.rest.url",
      "ranger.policy-rest-url",
      "ranger.service-name",
      "ranger.service-type",
      "ranger.app-id",
      "ranger.policy.cache.dir",
      "ranger.policy.poll-interval-ms",
      "ranger.policy.connection-timeout-ms",
      "ranger.policy.read-timeout-ms",
      "ranger.ssl.truststore.file",
      "ranger.ssl.truststore.password",
      "ranger.config-dir",
      "ranger.audit.enabled"
  );

  private static final Set<String> PER_FRONTEND_KNOWN_SUFFIXES = Set.of(
      "port",
      "bind-host",
      "min-worker-threads",
      "max-worker-threads",
      "frontend-profile",
      "standalone-metastore-jar",
      "client-socket-timeout-ms",
      "tcp-keepalive",
      "tcp-keepalive-idle-seconds",
      "tcp-keepalive-interval-seconds",
      "tcp-keepalive-count"
  );
}
