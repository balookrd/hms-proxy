package io.github.mmalykhin.hmsproxy.config;

import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.listener.AdditionalFrontendConfig;
import io.github.mmalykhin.hmsproxy.config.ratelimit.RateLimitConfig;
import io.github.mmalykhin.hmsproxy.config.ratelimit.RateLimitPolicyConfig;
import io.github.mmalykhin.hmsproxy.config.ratelimit.SourceCidrRateLimitConfig;
import io.github.mmalykhin.hmsproxy.config.routing.AdaptiveTimeoutConfig;
import io.github.mmalykhin.hmsproxy.config.routing.BackendStatePollingConfig;
import io.github.mmalykhin.hmsproxy.config.routing.CircuitBreakerConfig;
import io.github.mmalykhin.hmsproxy.config.routing.HedgedReadConfig;
import io.github.mmalykhin.hmsproxy.config.routing.LatencyRoutingConfig;
import io.github.mmalykhin.hmsproxy.config.security.CatalogRangerConfig;
import io.github.mmalykhin.hmsproxy.config.security.RangerConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfig;
import io.github.mmalykhin.hmsproxy.config.server.ClientSocketConfig;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ProxyConfigLogger {
  private static final Logger LOG = LoggerFactory.getLogger(ProxyConfigLogger.class);

  private ProxyConfigLogger() {
  }

  public static void logConfiguration(ProxyConfig config, Path configPath) {
    String formatted = formatConfiguration(config, configPath);
    LOG.info("{}", formatted);
  }

  public static String formatConfiguration(ProxyConfig config, Path configPath) {
    StringBuilder sb = new StringBuilder();
    sb.append("\n================================================================================\n");
    sb.append("HMS Proxy Configuration Loaded");
    if (configPath != null) {
      sb.append(" (source: ").append(configPath.toAbsolutePath()).append(")");
    }
    sb.append("\n================================================================================\n");

    appendServer(sb, config.server());
    appendSecurity(sb, config.security());
    appendRoutingAndFederation(sb, config);
    appendCatalogs(sb, config);
    appendAdditionalFrontends(sb, config);
    appendRestCatalog(sb, config);
    appendManagement(sb, config);
    appendResilienceAndLatency(sb, config.latencyRouting());
    appendRateLimit(sb, config.rateLimit());
    appendRanger(sb, config.ranger());
    appendDdlGuard(sb, config);
    appendIcebergPointerGuard(sb, config);
    appendSyntheticReadLockStore(sb, config);

    sb.append("================================================================================");
    return sb.toString();
  }

  private static void appendServer(StringBuilder sb, ServerConfig server) {
    sb.append("[Server]\n");
    sb.append("  Name: ").append(server.name()).append("\n");
    sb.append("  Listen: ").append(server.bindHost()).append(":").append(server.port()).append("\n");
    sb.append("  Worker Threads: ").append(server.minWorkerThreads())
        .append(" min / ").append(server.maxWorkerThreads()).append(" max\n");
    ClientSocketConfig socket = server.clientSocket();
    sb.append("  Client Socket Timeout: ").append(socket.clientTimeoutMs()).append(" ms\n");
    sb.append("  TCP Keepalive: enabled=").append(socket.tcpKeepAlive())
        .append(" (idle=").append(socket.keepAliveIdleSeconds()).append("s")
        .append(", interval=").append(socket.keepAliveIntervalSeconds()).append("s")
        .append(", count=").append(socket.keepAliveCount()).append(")\n");
    sb.append("  Shutdown Timeout: ").append(server.shutdownTimeoutSeconds()).append("s\n");
  }

  private static void appendSecurity(StringBuilder sb, SecurityConfig security) {
    sb.append("[Security]\n");
    sb.append("  Mode: ").append(security.mode()).append("\n");
    if (security.serverPrincipal() != null) {
      sb.append("  Server Principal: ").append(security.serverPrincipal()).append("\n");
    }
    if (security.clientPrincipal() != null) {
      sb.append("  Client Principal: ").append(security.clientPrincipal()).append("\n");
    }
    if (security.keytab() != null) {
      sb.append("  Server Keytab: ").append(security.keytab()).append("\n");
    }
    if (security.clientKeytab() != null) {
      sb.append("  Client Keytab: ").append(security.clientKeytab()).append("\n");
    }
    sb.append("  Impersonation: ").append(security.impersonationEnabled()).append("\n");
    if (!security.frontDoorConf().isEmpty()) {
      sb.append("  Front-Door Conf overrides (").append(security.frontDoorConf().size()).append("):\n");
      Map<String, String> sorted = new TreeMap<>(security.frontDoorConf());
      for (Map.Entry<String, String> entry : sorted.entrySet()) {
        sb.append("    * ").append(entry.getKey()).append(" = ")
            .append(maskSensitive(entry.getKey(), entry.getValue())).append("\n");
      }
    }
  }

  private static void appendRoutingAndFederation(StringBuilder sb, ProxyConfig config) {
    sb.append("[Routing & Federation]\n");
    sb.append("  Default Catalog: ").append(config.defaultCatalog()).append("\n");
    sb.append("  Catalog DB Separator: '").append(config.catalogDbSeparator()).append("'\n");
    sb.append("  Frontend Profile: ").append(config.compatibility().frontendProfile())
        .append(" (runtime: ").append(config.compatibility().frontendProfile().runtimeProfile()).append(")\n");
    if (config.compatibility().frontendStandaloneMetastoreJar() != null) {
      sb.append("  Frontend Standalone Metastore Jar: ")
          .append(config.compatibility().frontendStandaloneMetastoreJar()).append("\n");
    }
    if (config.compatibility().backendStandaloneMetastoreJar() != null) {
      sb.append("  Backend Standalone Metastore Jar Override: ")
          .append(config.compatibility().backendStandaloneMetastoreJar()).append("\n");
    }
    sb.append("  Preserve Backend Catalog Name: ").append(config.federation().preserveBackendCatalogName()).append("\n");
    sb.append("  View Text Rewrite: mode=").append(config.federation().viewTextRewriteMode())
        .append(", preserveOriginalText=").append(config.federation().preserveOriginalViewText()).append("\n");
    sb.append("  External Table Location Rewrite: mode=").append(config.federation().externalTableLocationRewriteMode());
    if (config.federation().externalTableLocationRewriteSourceDefaultFs() != null) {
      sb.append(", sourceDefaultFs=").append(config.federation().externalTableLocationRewriteSourceDefaultFs());
    }
    sb.append("\n");
    sb.append("  External Table Drop Purge Mode: ").append(config.federation().externalTableDropPurgeMode()).append("\n");
  }

  private static void appendCatalogs(StringBuilder sb, ProxyConfig config) {
    sb.append("[Catalogs] (total: ").append(config.catalogs().size()).append(")\n");
    for (CatalogConfig cat : config.catalogs().values()) {
      sb.append("  * Catalog '").append(cat.name()).append("':\n");
      sb.append("      Description: ").append(cat.description()).append("\n");
      sb.append("      Location URI: ").append(cat.locationUri()).append("\n");
      sb.append("      Access Mode: ").append(cat.accessMode());
      if (!cat.writeDbWhitelist().isEmpty()) {
        sb.append(" (writeDbWhitelist: ").append(cat.writeDbWhitelist()).append(")");
      }
      sb.append("\n");
      sb.append("      Expose Mode: ").append(cat.exposeMode());
      if (!cat.exposeDbPatterns().isEmpty()) {
        sb.append(" (dbPatterns: ").append(cat.exposeDbPatterns()).append(")");
      }
      sb.append("\n");
      if (cat.runtimeProfile() != null) {
        sb.append("      Runtime Profile Override: ").append(cat.runtimeProfile()).append("\n");
      }
      if (cat.backendStandaloneMetastoreJar() != null) {
        sb.append("      Standalone Metastore Jar: ").append(cat.backendStandaloneMetastoreJar()).append("\n");
      }
      sb.append("      Startup Mode: ").append(cat.startupMode())
          .append(", requiredForReadiness=").append(cat.requiredForReadiness()).append("\n");
      sb.append("      Impersonation: enabled=").append(cat.impersonationEnabled())
          .append(" (maxClients=").append(cat.maxImpersonationClients())
          .append(", clientIdleTtl=").append(cat.impersonationClientIdleTtlMs()).append("ms)\n");
      sb.append("      Session Pools: sharedPoolSize=").append(cat.sharedSessionPoolSize())
          .append(", impersonationMaxSize=").append(cat.impersonationPoolMaxSize())
          .append(", idleTtl=").append(cat.impersonationSessionIdleTtlMs()).append("ms\n");
      sb.append("      Latency Budget: ").append(cat.latencyBudgetMs()).append(" ms\n");
      sb.append("      Concurrency Limit: maxConcurrentCalls=")
          .append(cat.maxConcurrentCalls() > 0 ? cat.maxConcurrentCalls() : "unlimited")
          .append(", timeout=").append(cat.concurrencyTimeoutMs()).append("ms\n");
      if (cat.fallbackCatalog() != null) {
        sb.append("      Fallback Catalog: ").append(cat.fallbackCatalog())
            .append(" (onOutage=").append(cat.fallbackOnOutage()).append(")\n");
      }
      if (!cat.hiveConf().isEmpty()) {
        sb.append("      Hive Conf (").append(cat.hiveConf().size()).append(" properties):\n");
        Map<String, String> sortedConf = new TreeMap<>(cat.hiveConf());
        for (Map.Entry<String, String> entry : sortedConf.entrySet()) {
          sb.append("        - ").append(entry.getKey()).append(" = ")
              .append(maskSensitive(entry.getKey(), entry.getValue())).append("\n");
        }
      }
    }
  }

  private static void appendAdditionalFrontends(StringBuilder sb, ProxyConfig config) {
    if (config.additionalFrontends().isEmpty()) {
      return;
    }
    sb.append("[Additional Frontends] (total: ").append(config.additionalFrontends().size()).append(")\n");
    for (AdditionalFrontendConfig extra : config.additionalFrontends()) {
      sb.append("  * '").append(extra.name()).append("': ")
          .append(extra.bindHost()).append(":").append(extra.port())
          .append(" (profile: ").append(extra.frontendProfile())
          .append(", threads: ").append(extra.minWorkerThreads()).append("-").append(extra.maxWorkerThreads())
          .append(")\n");
    }
  }

  private static void appendRestCatalog(StringBuilder sb, ProxyConfig config) {
    sb.append("[REST Catalog]\n");
    sb.append("  Enabled: ").append(config.restCatalog().enabled()).append("\n");
    if (config.restCatalog().enabled()) {
      sb.append("  Listen: ").append(config.restCatalog().bindHost()).append(":").append(config.restCatalog().port()).append("\n");
      sb.append("  Worker Threads: ").append(config.restCatalog().minWorkerThreads())
          .append(" min / ").append(config.restCatalog().maxWorkerThreads()).append(" max\n");
      sb.append("  Purge Mode: ").append(config.restCatalog().purgeMode()).append("\n");
      if (!config.restCatalog().purgeAllowedPrefixes().isEmpty()) {
        sb.append("  Purge Allowed Prefixes: ").append(config.restCatalog().purgeAllowedPrefixes()).append("\n");
      }
      sb.append("  Hive Engine Descriptor: ").append(config.restCatalog().hiveEngineDescriptor()).append("\n");
    }
  }

  private static void appendManagement(StringBuilder sb, ProxyConfig config) {
    sb.append("[Management HTTP]\n");
    sb.append("  Enabled: ").append(config.management().enabled()).append("\n");
    if (config.management().enabled()) {
      sb.append("  Listen: ").append(config.management().bindHost()).append(":").append(config.management().port()).append("\n");
      sb.append("  Handler Threads: ").append(config.management().threads()).append("\n");
      sb.append("  Readiness Cache: ").append(config.management().readinessCacheMs()).append(" ms\n");
      sb.append("  Require All Catalogs: ").append(config.management().requireAllCatalogs()).append("\n");
    }
  }

  private static void appendResilienceAndLatency(StringBuilder sb, LatencyRoutingConfig lr) {
    sb.append("[Resilience & Latency Routing]\n");
    BackendStatePollingConfig bsp = lr.backendStatePolling();
    sb.append("  Backend State Polling: enabled=").append(bsp.enabled());
    if (bsp.enabled()) {
      sb.append(" (interval=").append(bsp.intervalMs()).append("ms")
          .append(", probeTimeout=").append(bsp.probeTimeoutMs()).append("ms")
          .append(", maxParallelism=").append(bsp.maxParallelism()).append(")");
    }
    sb.append("\n");

    AdaptiveTimeoutConfig at = lr.adaptiveTimeout();
    sb.append("  Adaptive Timeout: enabled=").append(at.enabled());
    if (at.enabled()) {
      sb.append(" (initial=").append(at.initialTimeoutMs()).append("ms")
          .append(", min=").append(at.minTimeoutMs()).append("ms")
          .append(", max=").append(at.maxTimeoutMs()).append("ms")
          .append(", multiplier=").append(at.multiplier())
          .append(", alpha=").append(at.alpha())
          .append(", cooldown=").append(at.reconnectCooldownMs()).append("ms)");
    }
    sb.append("\n");

    CircuitBreakerConfig cb = lr.circuitBreaker();
    sb.append("  Circuit Breaker: enabled=").append(cb.enabled());
    if (cb.enabled()) {
      sb.append(" (failureThreshold=").append(cb.failureThreshold())
          .append(", openState=").append(cb.openStateMs()).append("ms)");
    }
    sb.append("\n");

    HedgedReadConfig hr = lr.hedgedRead();
    sb.append("  Hedged Read: enabled=").append(hr.enabled());
    if (hr.enabled()) {
      sb.append(" (maxParallelism=").append(hr.maxParallelism())
          .append(", timeout=").append(hr.fanoutTimeoutMs()).append("ms)");
    }
    sb.append("\n");

    sb.append("  Degraded Routing Policy: ").append(lr.degradedRoutingPolicy()).append("\n");
    sb.append("  Database List Cache: ttl=").append(lr.databaseListCache().ttlMs()).append("ms")
        .append(", maxEntries=").append(lr.databaseListCache().maxEntries())
        .append(", shared=").append(lr.databaseListCache().sharedAcrossUsers()).append("\n");
    sb.append("  Database Metadata Cache: ttl=").append(lr.databaseMetadataCache().ttlMs()).append("ms")
        .append(", maxEntries=").append(lr.databaseMetadataCache().maxEntries())
        .append(", shared=").append(lr.databaseMetadataCache().sharedAcrossUsers()).append("\n");
    sb.append("  Table Metadata Cache: ttl=").append(lr.tableMetadataCache().ttlMs()).append("ms")
        .append(", maxEntries=").append(lr.tableMetadataCache().maxEntries())
        .append(", shared=").append(lr.tableMetadataCache().sharedAcrossUsers()).append("\n");
    sb.append("  Partition Metadata Cache: ttl=").append(lr.partitionMetadataCache().ttlMs()).append("ms")
        .append(", maxEntries=").append(lr.partitionMetadataCache().maxEntries())
        .append(", shared=").append(lr.partitionMetadataCache().sharedAcrossUsers()).append("\n");
    sb.append("  Config Value Cache: ttl=").append(lr.configValueCache().ttlMs()).append("ms")
        .append(", maxEntries=").append(lr.configValueCache().maxEntries()).append("\n");
    sb.append("  Serve Stale On Error: enabled=").append(lr.cacheServeStaleOnError())
        .append(", gracePeriod=").append(lr.cacheStaleGracePeriodMs()).append("ms\n");
  }

  private static void appendRateLimit(StringBuilder sb, RateLimitConfig rl) {
    sb.append("[Rate Limiting]\n");
    sb.append("  Principal: ").append(formatPolicy(rl.principal())).append("\n");
    sb.append("  Source IP: ").append(formatPolicy(rl.source())).append("\n");
    if (!rl.sourceCidrs().isEmpty()) {
      sb.append("  Source CIDRs (").append(rl.sourceCidrs().size()).append("):\n");
      for (Map.Entry<String, SourceCidrRateLimitConfig> entry : rl.sourceCidrs().entrySet()) {
        sb.append("    * ").append(entry.getKey()).append(": cidrs=").append(entry.getValue().cidrRules())
            .append(", ").append(formatPolicy(entry.getValue().policy())).append("\n");
      }
    }
    if (!rl.methodFamilies().isEmpty()) {
      sb.append("  Method Families: ").append(rl.methodFamilies().keySet()).append("\n");
    }
    if (!rl.catalogs().isEmpty()) {
      sb.append("  Catalog Policies: ").append(rl.catalogs().keySet()).append("\n");
    }
    if (!rl.rpcClasses().isEmpty()) {
      sb.append("  RPC Classes: ").append(rl.rpcClasses().keySet()).append("\n");
    }
  }

  private static String formatPolicy(RateLimitPolicyConfig policy) {
    if (policy == null || !policy.enabled()) {
      return "disabled";
    }
    return policy.requestsPerSecond() + " rps (burst=" + policy.burst() + ")";
  }

  private static void appendRanger(StringBuilder sb, RangerConfig ranger) {
    sb.append("[Ranger]\n");
    sb.append("  Enabled: ").append(ranger.enabled()).append("\n");
    if (ranger.enabled() || !ranger.catalogConfigs().isEmpty()) {
      CatalogRangerConfig def = ranger.defaults();
      sb.append("  Defaults: serviceName=").append(def.serviceName())
          .append(", restUrl=").append(def.policyRestUrl())
          .append(", auditEnabled=").append(def.auditEnabled()).append("\n");
      if (!ranger.catalogConfigs().isEmpty()) {
        sb.append("  Per-Catalog Configurations (").append(ranger.catalogConfigs().size()).append("):\n");
        for (Map.Entry<String, CatalogRangerConfig> entry : ranger.catalogConfigs().entrySet()) {
          sb.append("    * ").append(entry.getKey()).append(": enabled=").append(entry.getValue().enabled())
              .append(", service=").append(entry.getValue().serviceName()).append("\n");
        }
      }
    }
  }

  private static void appendDdlGuard(StringBuilder sb, ProxyConfig config) {
    sb.append("[DDL Guard]\n");
    sb.append("  Mode: ").append(config.transactionalDdlGuard().mode()).append("\n");
    if (!config.transactionalDdlGuard().clientAddressRules().isEmpty()) {
      sb.append("  Client Addresses: ").append(config.transactionalDdlGuard().clientAddressRules()).append("\n");
    }
  }

  private static void appendIcebergPointerGuard(StringBuilder sb, ProxyConfig config) {
    sb.append("[Iceberg Pointer Guard]\n");
    sb.append("  Enabled: ").append(config.icebergPointerGuard().enabled())
        .append(", cacheTtl=").append(config.icebergPointerGuard().tableCacheTtlMs()).append("ms")
        .append(", maxEntries=").append(config.icebergPointerGuard().tableCacheMaxEntries())
        .append(", lockEnabled=").append(config.icebergPointerGuard().lockEnabled())
        .append(", lockTimeout=").append(config.icebergPointerGuard().lockAcquireTimeoutMs()).append("ms")
        .append(", hiveEngineDescriptor=").append(config.icebergPointerGuard().hiveEngineDescriptor()).append("\n");
  }

  private static void appendSyntheticReadLockStore(StringBuilder sb, ProxyConfig config) {
    sb.append("[Synthetic Read Lock Store]\n");
    sb.append("  Mode: ").append(config.syntheticReadLockStore().mode()).append("\n");
    if (config.syntheticReadLockStore().zooKeeper() != null
        && config.syntheticReadLockStore().zooKeeper().connectString() != null) {
      sb.append("  ZooKeeper Connect: ").append(config.syntheticReadLockStore().zooKeeper().connectString())
          .append(", znode=").append(config.syntheticReadLockStore().zooKeeper().znode()).append("\n");
    }
  }

  public static String maskSensitive(String key, String value) {
    if (value == null) {
      return null;
    }
    String lower = key.toLowerCase(Locale.ROOT);
    if (lower.contains("password")
        || lower.contains("secret")
        || lower.contains("credential")
        || lower.contains("token")
        || lower.contains("private")
        || lower.contains("keytab-password")) {
      return "******";
    }
    return value;
  }
}
