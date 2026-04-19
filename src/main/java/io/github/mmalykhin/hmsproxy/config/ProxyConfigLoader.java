package io.github.mmalykhin.hmsproxy.config;

import io.github.mmalykhin.hmsproxy.util.ClientAddressMatcher;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public final class ProxyConfigLoader {
  private static final Set<String> SUPPORTED_RATE_LIMIT_METHOD_FAMILIES =
      Arrays.stream(HmsOperationClass.values())
          .map(HmsOperationClass::wireName)
          .collect(Collectors.toUnmodifiableSet());
  private static final Set<String> SUPPORTED_RATE_LIMIT_RPC_CLASSES = Set.of("write", "ddl", "txn", "lock");

  private ProxyConfigLoader() {
  }

  public static ProxyConfig load(Path path) throws IOException {
    Properties p = new Properties();
    try (InputStream input = Files.newInputStream(path)) {
      p.load(input);
    }
    ProxyConfig.ServerConfig server = loadServerConfig(p);
    String catalogDbSeparator = loadCatalogDbSeparator(p);
    ProxyConfig.CompatibilityConfig compatibility = loadCompatibilityConfig(p);
    Map<String, String> backendConf = loadBackendConf(p);
    boolean globalImpersonation = Boolean.parseBoolean(
        get(p, "security.impersonation-enabled", "false"));
    Map<String, ProxyConfig.CatalogConfig> catalogs =
        loadCatalogConfigs(p, backendConf, globalImpersonation);
    String defaultCatalog = resolveDefaultCatalog(p, catalogs);
    ProxyConfig.SecurityConfig security = loadSecurityConfig(p, catalogs);
    ProxyConfig.FederationConfig federation = loadFederationConfig(p, catalogs.get(defaultCatalog));
    ProxyConfig.TransactionalDdlGuardConfig transactionalDdlGuard = loadTransactionalDdlGuardConfig(p);
    ProxyConfig.ManagementConfig management = loadManagementConfig(p, server);
    ProxyConfig.SyntheticReadLockStoreConfig syntheticReadLockStore = loadSyntheticReadLockStoreConfig(p);
    ProxyConfig.RateLimitConfig rateLimit = loadRateLimitConfig(p, catalogs);
    ProxyConfig.LatencyRoutingConfig latencyRouting = loadLatencyRoutingConfig(p, catalogs.size());
    return ProxyConfig.builder()
        .server(server)
        .security(security)
        .catalogDbSeparator(catalogDbSeparator)
        .defaultCatalog(defaultCatalog)
        .catalogs(catalogs)
        .backend(new ProxyConfig.BackendConfig(backendConf))
        .compatibility(compatibility)
        .federation(federation)
        .transactionalDdlGuard(transactionalDdlGuard)
        .management(management)
        .syntheticReadLockStore(syntheticReadLockStore)
        .rateLimit(rateLimit)
        .latencyRouting(latencyRouting)
        .build();
  }

  private static ProxyConfig.ServerConfig loadServerConfig(Properties p) {
    int port = getInt(p, "server.port", 9083);
    if (port < 1 || port > 65535) {
      throw new IllegalArgumentException("server.port must be between 1 and 65535, got: " + port);
    }
    int minWorkerThreads = getInt(p, "server.min-worker-threads", 16);
    if (minWorkerThreads < 1) {
      throw new IllegalArgumentException("server.min-worker-threads must be >= 1, got: " + minWorkerThreads);
    }
    int maxWorkerThreads = getInt(p, "server.max-worker-threads", 256);
    if (maxWorkerThreads < minWorkerThreads) {
      throw new IllegalArgumentException(
          "server.max-worker-threads (" + maxWorkerThreads
              + ") must be >= server.min-worker-threads (" + minWorkerThreads + ")");
    }
    return new ProxyConfig.ServerConfig(
        get(p, "server.name", "hms-proxy"),
        get(p, "server.bind-host", "0.0.0.0"),
        port,
        minWorkerThreads,
        maxWorkerThreads);
  }

  private static String loadCatalogDbSeparator(Properties p) {
    String sep = trimToNull(p.getProperty("routing.catalog-db-separator"));
    if (p.containsKey("routing.catalog-db-separator") && sep == null) {
      throw new IllegalArgumentException("routing.catalog-db-separator must not be blank");
    }
    return sep != null ? sep : ".";
  }

  private static ProxyConfig.CompatibilityConfig loadCompatibilityConfig(Properties p) {
    ProxyConfig.FrontendProfile frontendProfile = ProxyConfig.FrontendProfile.valueOf(
        get(p, "compatibility.frontend-profile", "APACHE_3_1_3").trim().toUpperCase());
    String frontendStandaloneMetastoreJar =
        trimToNull(p.getProperty("compatibility.frontend-standalone-metastore-jar"));
    if (frontendStandaloneMetastoreJar == null) {
      frontendStandaloneMetastoreJar =
          trimToNull(p.getProperty("compatibility.hortonworks-standalone-metastore-jar"));
    }
    String backendStandaloneMetastoreJar =
        trimToNull(p.getProperty("compatibility.backend-standalone-metastore-jar"));
    boolean preserveBackendCatalogName = Boolean.parseBoolean(
        get(p, "federation.preserve-backend-catalog-name", "false"));
    return new ProxyConfig.CompatibilityConfig(
        frontendProfile,
        frontendStandaloneMetastoreJar,
        backendStandaloneMetastoreJar,
        preserveBackendCatalogName);
  }

  private static Map<String, String> loadBackendConf(Properties p) {
    return p.stringPropertyNames().stream()
        .filter(name -> name.startsWith("backend.conf."))
        .sorted()
        .collect(Collectors.toMap(
            name -> name.substring("backend.conf.".length()),
            p::getProperty,
            (left, right) -> right,
            LinkedHashMap::new));
  }

  private static Map<String, ProxyConfig.CatalogConfig> loadCatalogConfigs(
      Properties p,
      Map<String, String> backendConf,
      boolean globalImpersonation
  ) {
    String catalogsValue = require(p, "catalogs");
    Map<String, ProxyConfig.CatalogConfig> catalogs = new LinkedHashMap<>();
    for (String catalogName : splitCsv(catalogsValue)) {
      String prefix = "catalog." + catalogName + ".";
      boolean catalogImpersonationEnabled = Boolean.parseBoolean(
          get(p, prefix + "impersonation-enabled", Boolean.toString(globalImpersonation)));
      ProxyConfig.CatalogAccessMode catalogAccessMode = parseCatalogAccessMode(
          trimToNull(p.getProperty(prefix + "access-mode")));
      String[] catalogWriteDbWhitelist = splitCsv(get(p, prefix + "write-db-whitelist", ""));
      ProxyConfig.CatalogExposureMode catalogExposureMode = parseCatalogExposureMode(
          trimToNull(p.getProperty(prefix + "expose-mode")));
      String[] catalogExposeDbPatterns = splitCsv(get(p, prefix + "expose-db-patterns", ""));
      validateRegexList(prefix + "expose-db-patterns", catalogExposeDbPatterns);
      Map<String, List<String>> catalogExposeTablePatterns = parseExposeTablePatterns(p, prefix);
      MetastoreRuntimeProfile catalogRuntimeProfile = parseRuntimeProfile(
          trimToNull(p.getProperty(prefix + "runtime-profile")));
      String catalogBackendStandaloneMetastoreJar =
          trimToNull(p.getProperty(prefix + "backend-standalone-metastore-jar"));
      long latencyBudgetMs = getNonNegativeLong(p, prefix + "latency-budget-ms", 0L);
      int maxImpersonationClients = getPositiveInt(p, prefix + "impersonation-max-clients", 128);
      long impersonationClientIdleTtlMs = getNonNegativeLong(p, prefix + "impersonation-client-idle-ttl-ms", 0L);
      Map<String, String> hiveConfOverrides = p.stringPropertyNames().stream()
          .filter(name -> name.startsWith(prefix + "conf."))
          .sorted()
          .collect(Collectors.toMap(
              name -> name.substring((prefix + "conf.").length()),
              p::getProperty,
              (left, right) -> right,
              LinkedHashMap::new));
      Map<String, String> hiveConf = new LinkedHashMap<>(backendConf);
      hiveConf.putAll(hiveConfOverrides);
      if (!hiveConf.containsKey("hive.metastore.uris")) {
        throw new IllegalArgumentException(
            "Missing backend.conf.hive.metastore.uris or " + prefix + "conf.hive.metastore.uris for catalog "
                + catalogName);
      }
      catalogs.put(catalogName, new ProxyConfig.CatalogConfig(
          catalogName,
          get(p, prefix + "description", catalogName),
          get(p, prefix + "location-uri", "file:///warehouse/" + catalogName),
          catalogImpersonationEnabled,
          catalogAccessMode,
          Arrays.asList(catalogWriteDbWhitelist),
          catalogExposureMode,
          Arrays.asList(catalogExposeDbPatterns),
          catalogExposeTablePatterns,
          catalogRuntimeProfile,
          catalogBackendStandaloneMetastoreJar,
          hiveConf,
          latencyBudgetMs,
          maxImpersonationClients,
          impersonationClientIdleTtlMs));
    }
    return catalogs;
  }

  private static String resolveDefaultCatalog(
      Properties p,
      Map<String, ProxyConfig.CatalogConfig> catalogs
  ) {
    String defaultCatalog = trimToNull(p.getProperty("routing.default-catalog"));
    if (defaultCatalog == null) {
      if (catalogs.size() == 1) {
        defaultCatalog = catalogs.keySet().iterator().next();
      } else {
        throw new IllegalArgumentException(
            "routing.default-catalog is required when more than one catalog is configured");
      }
    }
    if (!catalogs.containsKey(defaultCatalog)) {
      throw new IllegalArgumentException("Unknown routing.default-catalog: " + defaultCatalog);
    }
    return defaultCatalog;
  }

  private static ProxyConfig.SecurityConfig loadSecurityConfig(
      Properties p,
      Map<String, ProxyConfig.CatalogConfig> catalogs
  ) {
    ProxyConfig.SecurityMode securityMode = ProxyConfig.SecurityMode.valueOf(
        get(p, "security.mode", "NONE").trim().toUpperCase());
    String serverPrincipal = trimToNull(p.getProperty("security.server-principal"));
    String clientPrincipal = trimToNull(p.getProperty("security.client-principal"));
    String keytab = trimToNull(p.getProperty("security.keytab"));
    String clientKeytab = trimToNull(p.getProperty("security.client-keytab"));
    boolean impersonationEnabled = Boolean.parseBoolean(
        get(p, "security.impersonation-enabled", "false"));
    Map<String, String> frontDoorConf = p.stringPropertyNames().stream()
        .filter(name -> name.startsWith("security.front-door-conf."))
        .sorted()
        .collect(Collectors.toMap(
            name -> name.substring("security.front-door-conf.".length()),
            p::getProperty,
            (left, right) -> right,
            LinkedHashMap::new));

    if (clientPrincipal == null && serverPrincipal != null) {
      clientPrincipal = serverPrincipal;
    }
    if (clientKeytab == null && keytab != null) {
      clientKeytab = keytab;
    }
    if (securityMode == ProxyConfig.SecurityMode.KERBEROS) {
      requireNonBlank(serverPrincipal, "security.server-principal");
      requireNonBlank(keytab, "security.keytab");
      requireReadableFile(keytab, "security.keytab");
    }
    if (catalogs.values().stream().anyMatch(ProxyConfig.CatalogConfig::impersonationEnabled)
        && securityMode != ProxyConfig.SecurityMode.KERBEROS) {
      throw new IllegalArgumentException(
          "security.impersonation-enabled and catalog.<name>.impersonation-enabled "
              + "require security.mode=KERBEROS so the proxy can derive the caller identity from SASL");
    }
    if (catalogs.values().stream().anyMatch(catalog -> backendKerberosEnabled(catalog.hiveConf()))) {
      requireNonBlank(clientPrincipal, "security.client-principal");
      requireNonBlank(clientKeytab, "security.client-keytab");
      requireReadableFile(clientKeytab, "security.client-keytab");
    }
    return new ProxyConfig.SecurityConfig(
        securityMode,
        serverPrincipal,
        clientPrincipal,
        keytab,
        clientKeytab,
        impersonationEnabled,
        frontDoorConf);
  }

  private static ProxyConfig.FederationConfig loadFederationConfig(
      Properties p,
      ProxyConfig.CatalogConfig defaultCatalogConfig
  ) {
    boolean preserveBackendCatalogName = Boolean.parseBoolean(
        get(p, "federation.preserve-backend-catalog-name", "false"));
    ProxyConfig.ViewTextRewriteMode viewTextRewriteMode = parseViewTextRewriteMode(
        trimToNull(p.getProperty("federation.view-text-rewrite.mode")));
    boolean preserveOriginalViewText = Boolean.parseBoolean(
        get(p, "federation.view-text-rewrite.preserve-original-text", "false"));
    ProxyConfig.ExternalTableLocationRewriteMode externalTableLocationRewriteMode =
        parseExternalTableLocationRewriteMode(
            trimToNull(p.getProperty("federation.external-table-location-rewrite.mode")));
    String externalTableLocationRewriteSourceDefaultFs =
        trimToNull(p.getProperty("federation.external-table-location-rewrite.source-default-fs"));
    if (externalTableLocationRewriteSourceDefaultFs == null && defaultCatalogConfig != null) {
      externalTableLocationRewriteSourceDefaultFs =
          trimToNull(defaultCatalogConfig.hiveConf().get("fs.defaultFS"));
    }
    ProxyConfig.ExternalTableDropPurgeMode externalTableDropPurgeMode =
        parseExternalTableDropPurgeMode(
            trimToNull(p.getProperty("federation.external-table-drop-purge.mode")));
    if (externalTableLocationRewriteMode
        == ProxyConfig.ExternalTableLocationRewriteMode.REWRITE_IF_SOURCE_DEFAULT_FS
        && externalTableLocationRewriteSourceDefaultFs == null) {
      throw new IllegalArgumentException(
          "Missing required property: federation.external-table-location-rewrite.source-default-fs"
              + " (or catalog." + (defaultCatalogConfig != null ? defaultCatalogConfig.name() : "<default>")
              + ".conf.fs.defaultFS)");
    }
    return new ProxyConfig.FederationConfig(
        preserveBackendCatalogName,
        viewTextRewriteMode,
        preserveOriginalViewText,
        externalTableLocationRewriteMode,
        externalTableLocationRewriteSourceDefaultFs,
        externalTableDropPurgeMode);
  }

  private static ProxyConfig.TransactionalDdlGuardConfig loadTransactionalDdlGuardConfig(Properties p) {
    ProxyConfig.TransactionalDdlGuardMode mode = parseTransactionalDdlGuardMode(
        trimToNull(p.getProperty("guard.transactional-ddl.mode")));
    String[] clientAddresses = splitCsv(get(p, "guard.transactional-ddl.client-addresses", ""));
    ClientAddressMatcher.parseAll(Arrays.asList(clientAddresses));
    return new ProxyConfig.TransactionalDdlGuardConfig(mode, Arrays.asList(clientAddresses));
  }

  private static ProxyConfig.ManagementConfig loadManagementConfig(
      Properties p,
      ProxyConfig.ServerConfig server
  ) {
    boolean managementPortConfigured = p.containsKey("management.port");
    boolean managementEnabled = Boolean.parseBoolean(
        get(p, "management.enabled", Boolean.toString(managementPortConfigured)));
    int managementPort = getInt(p, "management.port", server.port() + 1000);
    if (managementEnabled && (managementPort < 1 || managementPort > 65535)) {
      throw new IllegalArgumentException(
          "management.port must be between 1 and 65535, got: " + managementPort);
    }
    String managementBindHost = get(p, "management.bind-host", server.bindHost());
    return new ProxyConfig.ManagementConfig(managementEnabled, managementBindHost, managementPort);
  }

  private static ProxyConfig.SyntheticReadLockStoreConfig loadSyntheticReadLockStoreConfig(Properties p) {
    boolean zkConfigured = hasConfiguredPrefix(p, "synthetic-read-lock.store.zookeeper.");
    ProxyConfig.SyntheticReadLockStoreMode mode = parseSyntheticReadLockStoreMode(
        trimToNull(p.getProperty("synthetic-read-lock.store.mode")), zkConfigured);
    String znode = trimToNull(p.getProperty("synthetic-read-lock.store.zookeeper.znode"));
    if (p.containsKey("synthetic-read-lock.store.zookeeper.znode") && znode == null) {
      throw new IllegalArgumentException("synthetic-read-lock.store.zookeeper.znode must not be blank");
    }
    int connectionTimeoutMs = getPositiveInt(p, "synthetic-read-lock.store.zookeeper.connection-timeout-ms", 15_000);
    int sessionTimeoutMs = getPositiveInt(p, "synthetic-read-lock.store.zookeeper.session-timeout-ms", 60_000);
    int baseSleepMs = getPositiveInt(p, "synthetic-read-lock.store.zookeeper.base-sleep-ms", 1_000);
    int maxRetries = getPositiveInt(p, "synthetic-read-lock.store.zookeeper.max-retries", 3);
    ProxyConfig.SyntheticReadLockStoreZooKeeperConfig zk =
        new ProxyConfig.SyntheticReadLockStoreZooKeeperConfig(
            trimToNull(p.getProperty("synthetic-read-lock.store.zookeeper.connect-string")),
            znode,
            connectionTimeoutMs,
            sessionTimeoutMs,
            baseSleepMs,
            maxRetries);
    if (mode == ProxyConfig.SyntheticReadLockStoreMode.ZOOKEEPER) {
      requireNonBlank(zk.connectString(), "synthetic-read-lock.store.zookeeper.connect-string");
    }
    return new ProxyConfig.SyntheticReadLockStoreConfig(mode, zk);
  }

  private static ProxyConfig.RateLimitConfig loadRateLimitConfig(
      Properties p,
      Map<String, ProxyConfig.CatalogConfig> catalogs
  ) {
    ProxyConfig.RateLimitPolicyConfig principalRateLimit = parseRateLimitPolicy(p, "rate-limit.principal");
    ProxyConfig.RateLimitPolicyConfig sourceRateLimit = parseRateLimitPolicy(p, "rate-limit.source");
    Map<String, ProxyConfig.SourceCidrRateLimitConfig> sourceCidrRateLimits = parseSourceCidrRateLimits(p);
    Map<String, ProxyConfig.RateLimitPolicyConfig> methodFamilyRateLimits =
        parseRateLimitPolicies(p, "rate-limit.method-family.", SUPPORTED_RATE_LIMIT_METHOD_FAMILIES, true);
    Map<String, ProxyConfig.RateLimitPolicyConfig> catalogRateLimits =
        parseRateLimitPolicies(p, "rate-limit.catalog.", null, false);
    for (String catalogName : catalogRateLimits.keySet()) {
      if (!catalogs.containsKey(catalogName)) {
        throw new IllegalArgumentException("Unknown rate-limit.catalog entry: " + catalogName);
      }
    }
    Map<String, ProxyConfig.RateLimitPolicyConfig> rpcClassRateLimits =
        parseRateLimitPolicies(p, "rate-limit.rpc-class.", SUPPORTED_RATE_LIMIT_RPC_CLASSES, true);
    return new ProxyConfig.RateLimitConfig(
        principalRateLimit,
        sourceRateLimit,
        sourceCidrRateLimits,
        methodFamilyRateLimits,
        catalogRateLimits,
        rpcClassRateLimits);
  }

  private static ProxyConfig.LatencyRoutingConfig loadLatencyRoutingConfig(
      Properties p,
      int catalogCount
  ) {
    boolean backendStatePollingEnabled =
        Boolean.parseBoolean(get(p, "routing.backend-state-polling.enabled", "false"));
    int backendStatePollingIntervalMs =
        getPositiveInt(p, "routing.backend-state-polling.interval-ms", 10_000);
    long backendStatePollingProbeTimeoutMs =
        getPositiveLong(p, "routing.backend-state-polling.probe-timeout-ms", 5_000L);
    boolean adaptiveTimeoutEnabled =
        Boolean.parseBoolean(get(p, "routing.adaptive-timeout.enabled", "false"));
    long adaptiveTimeoutInitialMs = getPositiveLong(p, "routing.adaptive-timeout.initial-ms", 5_000L);
    long adaptiveTimeoutMinMs = getPositiveLong(p, "routing.adaptive-timeout.min-ms", 1_000L);
    long adaptiveTimeoutMaxMs = getPositiveLong(p, "routing.adaptive-timeout.max-ms", 60_000L);
    double adaptiveTimeoutMultiplier = getPositiveDouble(p, "routing.adaptive-timeout.multiplier", 4.0d);
    double adaptiveTimeoutAlpha = getBoundedDouble(p, "routing.adaptive-timeout.alpha", 0.2d, 0.0d, 1.0d);
    boolean circuitBreakerEnabled =
        Boolean.parseBoolean(get(p, "routing.circuit-breaker.enabled", "false"));
    int circuitBreakerFailureThreshold = getPositiveInt(p, "routing.circuit-breaker.failure-threshold", 3);
    long circuitBreakerOpenStateMs = getPositiveLong(p, "routing.circuit-breaker.open-state-ms", 30_000L);
    boolean hedgedReadEnabled =
        Boolean.parseBoolean(get(p, "routing.hedged-read.enabled", "false"));
    long hedgedReadFanoutTimeoutMs = getPositiveLong(p, "routing.hedged-read.fanout-timeout-ms", 30_000L);
    ProxyConfig.DegradedRoutingPolicy degradedRoutingPolicy = parseDegradedRoutingPolicy(
        trimToNull(p.getProperty("routing.degraded-routing-policy")));
    return new ProxyConfig.LatencyRoutingConfig(
        new ProxyConfig.BackendStatePollingConfig(
            backendStatePollingEnabled, backendStatePollingIntervalMs, backendStatePollingProbeTimeoutMs),
        new ProxyConfig.AdaptiveTimeoutConfig(
            adaptiveTimeoutEnabled,
            adaptiveTimeoutInitialMs,
            adaptiveTimeoutMinMs,
            adaptiveTimeoutMaxMs,
            adaptiveTimeoutMultiplier,
            adaptiveTimeoutAlpha),
        new ProxyConfig.CircuitBreakerConfig(
            circuitBreakerEnabled,
            circuitBreakerFailureThreshold,
            circuitBreakerOpenStateMs),
        new ProxyConfig.HedgedReadConfig(
            hedgedReadEnabled,
            Math.max(1, Math.min(catalogCount, getPositiveInt(
                p,
                "routing.hedged-read.max-parallelism",
                Math.max(1, catalogCount)))),
            hedgedReadFanoutTimeoutMs),
        degradedRoutingPolicy);
  }

  private static Map<String, ProxyConfig.SourceCidrRateLimitConfig> parseSourceCidrRateLimits(Properties properties) {
    String prefix = "rate-limit.source-cidr.";
    Map<String, ProxyConfig.SourceCidrRateLimitConfig> parsed = new LinkedHashMap<>();
    for (String ruleName : scopedNames(properties, prefix)) {
      String baseKey = prefix + ruleName;
      List<String> cidrRules = Arrays.asList(splitCsv(get(properties, baseKey + ".cidrs", "")));
      ProxyConfig.RateLimitPolicyConfig policy = parseRateLimitPolicy(properties, baseKey);
      if (cidrRules.isEmpty() && !policy.enabled()) {
        continue;
      }
      if (cidrRules.isEmpty()) {
        throw new IllegalArgumentException("Missing required property: " + baseKey + ".cidrs");
      }
      ClientAddressMatcher.parseAll(cidrRules);
      if (!policy.enabled()) {
        throw new IllegalArgumentException(
            baseKey + ".requests-per-second must be >= 1 when " + baseKey + " is configured");
      }
      parsed.put(ruleName, new ProxyConfig.SourceCidrRateLimitConfig(cidrRules, policy));
    }
    return parsed;
  }

  private static Map<String, ProxyConfig.RateLimitPolicyConfig> parseRateLimitPolicies(
      Properties properties,
      String prefix,
      Set<String> allowedNames,
      boolean normalizeToLowerCase
  ) {
    Map<String, ProxyConfig.RateLimitPolicyConfig> parsed = new LinkedHashMap<>();
    for (String rawName : scopedNames(properties, prefix)) {
      String normalizedName = normalizeToLowerCase ? rawName.toLowerCase(Locale.ROOT) : rawName;
      if (allowedNames != null && !allowedNames.contains(normalizedName)) {
        throw new IllegalArgumentException("Unsupported rate-limit scope '" + rawName + "' under " + prefix);
      }
      ProxyConfig.RateLimitPolicyConfig policy = parseRateLimitPolicy(properties, prefix + rawName);
      if (policy.enabled()) {
        parsed.put(normalizedName, policy);
      }
    }
    return parsed;
  }

  private static ProxyConfig.RateLimitPolicyConfig parseRateLimitPolicy(Properties properties, String baseKey) {
    boolean rateConfigured = properties.containsKey(baseKey + ".requests-per-second");
    boolean burstConfigured = properties.containsKey(baseKey + ".burst");
    int requestsPerSecond = getNonNegativeInt(properties, baseKey + ".requests-per-second", 0);
    int burst = getNonNegativeInt(properties, baseKey + ".burst", 0);
    if (!rateConfigured && !burstConfigured) {
      return ProxyConfig.RateLimitPolicyConfig.disabled();
    }
    if (requestsPerSecond < 1) {
      throw new IllegalArgumentException(baseKey + ".requests-per-second must be >= 1");
    }
    if (burstConfigured && burst < 1) {
      throw new IllegalArgumentException(baseKey + ".burst must be >= 1");
    }
    return new ProxyConfig.RateLimitPolicyConfig(requestsPerSecond, burst);
  }

  private static List<String> scopedNames(Properties properties, String prefix) {
    return properties.stringPropertyNames().stream()
        .filter(name -> name.startsWith(prefix))
        .map(name -> extractScopedName(name, prefix))
        .filter(Objects::nonNull)
        .collect(Collectors.toCollection(LinkedHashSet::new))
        .stream()
        .sorted()
        .toList();
  }

  private static String extractScopedName(String propertyName, String prefix) {
    String suffix = propertyName.substring(prefix.length());
    int separatorIndex = suffix.lastIndexOf('.');
    if (separatorIndex <= 0) {
      return null;
    }
    return suffix.substring(0, separatorIndex);
  }

  private static String[] splitCsv(String value) {
    return Arrays.stream(value.split(","))
        .map(String::trim)
        .filter(token -> !token.isEmpty())
        .toArray(String[]::new);
  }

  private static String get(Properties properties, String key, String defaultValue) {
    return Objects.requireNonNullElse(trimToNull(properties.getProperty(key)), defaultValue);
  }

  private static String require(Properties properties, String key) {
    String value = trimToNull(properties.getProperty(key));
    if (value == null) {
      throw new IllegalArgumentException("Missing required property: " + key);
    }
    return value;
  }

  private static int getInt(Properties properties, String key, int defaultValue) {
    String value = trimToNull(properties.getProperty(key));
    if (value == null) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid integer value for property " + key + ": " + value, e);
    }
  }

  private static int getNonNegativeInt(Properties properties, String key, int defaultValue) {
    int value = getInt(properties, key, defaultValue);
    if (value < 0) {
      throw new IllegalArgumentException(key + " must be >= 0, got: " + value);
    }
    return value;
  }

  private static int getPositiveInt(Properties properties, String key, int defaultValue) {
    int value = getInt(properties, key, defaultValue);
    if (value < 1) {
      throw new IllegalArgumentException(key + " must be >= 1, got: " + value);
    }
    return value;
  }

  private static long getLong(Properties properties, String key, long defaultValue) {
    String value = trimToNull(properties.getProperty(key));
    if (value == null) {
      return defaultValue;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid long value for property " + key + ": " + value, e);
    }
  }

  private static long getNonNegativeLong(Properties properties, String key, long defaultValue) {
    long value = getLong(properties, key, defaultValue);
    if (value < 0L) {
      throw new IllegalArgumentException(key + " must be >= 0, got: " + value);
    }
    return value;
  }

  private static long getPositiveLong(Properties properties, String key, long defaultValue) {
    long value = getLong(properties, key, defaultValue);
    if (value < 1L) {
      throw new IllegalArgumentException(key + " must be >= 1, got: " + value);
    }
    return value;
  }

  private static double getDouble(Properties properties, String key, double defaultValue) {
    String value = trimToNull(properties.getProperty(key));
    if (value == null) {
      return defaultValue;
    }
    try {
      return Double.parseDouble(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid floating point value for property " + key + ": " + value, e);
    }
  }

  private static double getPositiveDouble(Properties properties, String key, double defaultValue) {
    double value = getDouble(properties, key, defaultValue);
    if (value <= 0.0d) {
      throw new IllegalArgumentException(key + " must be > 0, got: " + value);
    }
    return value;
  }

  private static double getBoundedDouble(
      Properties properties,
      String key,
      double defaultValue,
      double minExclusive,
      double maxInclusive
  ) {
    double value = getDouble(properties, key, defaultValue);
    if (value <= minExclusive || value > maxInclusive) {
      throw new IllegalArgumentException(
          key + " must be > " + minExclusive + " and <= " + maxInclusive + ", got: " + value);
    }
    return value;
  }

  private static boolean hasConfiguredPrefix(Properties properties, String prefix) {
    return properties.stringPropertyNames().stream().anyMatch(name -> name.startsWith(prefix));
  }

  private static String trimToNull(String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }

  private static void requireNonBlank(String value, String name) {
    if (trimToNull(value) == null) {
      throw new IllegalArgumentException("Missing required property: " + name);
    }
  }

  private static void requireReadableFile(String path, String propertyName) {
    if (!Files.isReadable(Path.of(path))) {
      throw new IllegalArgumentException(
          "File not found or not readable for " + propertyName + ": " + path);
    }
  }

  private static boolean backendKerberosEnabled(Map<String, String> hiveConf) {
    return Boolean.parseBoolean(trimToNull(hiveConf.get("hive.metastore.sasl.enabled")));
  }

  private static MetastoreRuntimeProfile parseRuntimeProfile(String value) {
    if (value == null) {
      return null;
    }
    return MetastoreRuntimeProfile.valueOf(value.trim().toUpperCase());
  }

  private static ProxyConfig.TransactionalDdlGuardMode parseTransactionalDdlGuardMode(String value) {
    if (value == null) {
      return ProxyConfig.TransactionalDdlGuardMode.DISABLED;
    }
    try {
      return ProxyConfig.TransactionalDdlGuardMode.valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid value for guard.transactional-ddl.mode: " + value
              + ". Expected one of: REJECT_TRANSACTIONAL, REWRITE_TRANSACTIONAL_TO_EXTERNAL,"
              + " REWRITE_TO_NON_TRANSACTIONAL, REWRITE_MANAGED_TO_EXTERNAL", e);
    }
  }

  private static ProxyConfig.SyntheticReadLockStoreMode parseSyntheticReadLockStoreMode(
      String value,
      boolean zooKeeperConfigured
  ) {
    if (value == null) {
      return zooKeeperConfigured
          ? ProxyConfig.SyntheticReadLockStoreMode.ZOOKEEPER
          : ProxyConfig.SyntheticReadLockStoreMode.IN_MEMORY;
    }
    try {
      return ProxyConfig.SyntheticReadLockStoreMode.valueOf(value.trim().toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid value for synthetic-read-lock.store.mode: " + value
              + ". Expected one of: IN_MEMORY, ZOOKEEPER",
          e);
    }
  }

  private static ProxyConfig.CatalogAccessMode parseCatalogAccessMode(String value) {
    if (value == null) {
      return ProxyConfig.CatalogAccessMode.READ_WRITE;
    }
    try {
      return ProxyConfig.CatalogAccessMode.valueOf(value.trim().toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid value for catalog.<name>.access-mode: " + value
              + ". Expected one of: READ_ONLY, READ_WRITE, READ_WRITE_DB_WHITELIST",
          e);
    }
  }

  private static ProxyConfig.CatalogExposureMode parseCatalogExposureMode(String value) {
    if (value == null) {
      return ProxyConfig.CatalogExposureMode.ALLOW_ALL;
    }
    try {
      return ProxyConfig.CatalogExposureMode.valueOf(value.trim().toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid value for catalog.<name>.expose-mode: " + value
              + ". Expected one of: ALLOW_ALL, DENY_BY_DEFAULT",
          e);
    }
  }

  private static ProxyConfig.ViewTextRewriteMode parseViewTextRewriteMode(String value) {
    if (value == null) {
      return ProxyConfig.ViewTextRewriteMode.DISABLED;
    }
    try {
      return ProxyConfig.ViewTextRewriteMode.valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid value for federation.view-text-rewrite.mode: " + value
              + ". Expected one of: DISABLED, REWRITE",
          e);
    }
  }

  private static ProxyConfig.ExternalTableLocationRewriteMode parseExternalTableLocationRewriteMode(String value) {
    if (value == null) {
      return ProxyConfig.ExternalTableLocationRewriteMode.DISABLED;
    }
    try {
      return ProxyConfig.ExternalTableLocationRewriteMode.valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid value for federation.external-table-location-rewrite.mode: " + value
              + ". Expected one of: DISABLED, QUALIFY_UNQUALIFIED, REWRITE_IF_SOURCE_DEFAULT_FS",
          e);
    }
  }

  private static ProxyConfig.ExternalTableDropPurgeMode parseExternalTableDropPurgeMode(String value) {
    if (value == null) {
      return ProxyConfig.ExternalTableDropPurgeMode.DISABLED;
    }
    try {
      return ProxyConfig.ExternalTableDropPurgeMode.valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid value for federation.external-table-drop-purge.mode: " + value
              + ". Expected one of: DISABLED, BEST_EFFORT",
          e);
    }
  }

  private static ProxyConfig.DegradedRoutingPolicy parseDegradedRoutingPolicy(String value) {
    if (value == null) {
      return ProxyConfig.DegradedRoutingPolicy.STRICT;
    }
    try {
      return ProxyConfig.DegradedRoutingPolicy.valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid value for routing.degraded-routing-policy: " + value
              + ". Expected one of: STRICT, SAFE_FANOUT_READS",
          e);
    }
  }

  private static Map<String, List<String>> parseExposeTablePatterns(Properties properties, String prefix) {
    String propertyPrefix = prefix + "expose-table-patterns.";
    Map<String, List<String>> patterns = new LinkedHashMap<>();
    for (String propertyName : properties.stringPropertyNames().stream()
        .filter(name -> name.startsWith(propertyPrefix))
        .sorted()
        .toList()) {
      String dbPattern = trimToNull(propertyName.substring(propertyPrefix.length()));
      if (dbPattern == null) {
        throw new IllegalArgumentException(propertyName + " must not have a blank database pattern suffix");
      }
      String rawValue = properties.getProperty(propertyName);
      String[] tablePatterns = splitCsv(get(properties, propertyName, ""));
      if (rawValue != null && tablePatterns.length == 0) {
        throw new IllegalArgumentException(propertyName + " must define at least one table regex");
      }
      validateRegex(propertyName + " (db pattern)", dbPattern);
      validateRegexList(propertyName, tablePatterns);
      patterns.put(dbPattern, Arrays.asList(tablePatterns));
    }
    return patterns;
  }

  private static void validateRegexList(String propertyName, String[] patterns) {
    for (String pattern : patterns) {
      validateRegex(propertyName, pattern);
    }
  }

  private static void validateRegex(String propertyName, String pattern) {
    try {
      Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
    } catch (PatternSyntaxException e) {
      throw new IllegalArgumentException(
          "Invalid regex for " + propertyName + ": " + pattern + " - " + e.getMessage(),
          e);
    }
  }
}
