package io.github.mmalykhin.hmsproxy.config;

import io.github.mmalykhin.hmsproxy.compatibility.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.routing.HmsOperationRegistry;
import io.github.mmalykhin.hmsproxy.security.ClientAddressMatcher;
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
      Arrays.stream(HmsOperationRegistry.OperationClass.values())
          .map(HmsOperationRegistry.OperationClass::wireName)
          .collect(Collectors.toUnmodifiableSet());
  private static final Set<String> SUPPORTED_RATE_LIMIT_RPC_CLASSES = Set.of("write", "ddl", "txn", "lock");

  private ProxyConfigLoader() {
  }

  public static ProxyConfig load(Path path) throws IOException {
    Properties properties = new Properties();
    try (InputStream input = Files.newInputStream(path)) {
      properties.load(input);
    }

    int port = getInt(properties, "server.port", 9083);
    if (port < 1 || port > 65535) {
      throw new IllegalArgumentException("server.port must be between 1 and 65535, got: " + port);
    }
    int minWorkerThreads = getInt(properties, "server.min-worker-threads", 16);
    if (minWorkerThreads < 1) {
      throw new IllegalArgumentException("server.min-worker-threads must be >= 1, got: " + minWorkerThreads);
    }
    int maxWorkerThreads = getInt(properties, "server.max-worker-threads", 256);
    if (maxWorkerThreads < minWorkerThreads) {
      throw new IllegalArgumentException(
          "server.max-worker-threads (" + maxWorkerThreads
              + ") must be >= server.min-worker-threads (" + minWorkerThreads + ")");
    }
    ProxyConfig.ServerConfig server = new ProxyConfig.ServerConfig(
        get(properties, "server.name", "hms-proxy"),
        get(properties, "server.bind-host", "0.0.0.0"),
        port,
        minWorkerThreads,
        maxWorkerThreads);
    String catalogDbSeparator = trimToNull(properties.getProperty("routing.catalog-db-separator"));
    if (properties.containsKey("routing.catalog-db-separator") && catalogDbSeparator == null) {
      throw new IllegalArgumentException("routing.catalog-db-separator must not be blank");
    }
    if (catalogDbSeparator == null) {
      catalogDbSeparator = ".";
    }
    ProxyConfig.FrontendProfile frontendProfile = ProxyConfig.FrontendProfile.valueOf(
        get(properties, "compatibility.frontend-profile", "APACHE_3_1_3").trim().toUpperCase());
    String frontendStandaloneMetastoreJar =
        trimToNull(properties.getProperty("compatibility.frontend-standalone-metastore-jar"));
    if (frontendStandaloneMetastoreJar == null) {
      frontendStandaloneMetastoreJar =
          trimToNull(properties.getProperty("compatibility.hortonworks-standalone-metastore-jar"));
    }
    String backendStandaloneMetastoreJar =
        trimToNull(properties.getProperty("compatibility.backend-standalone-metastore-jar"));
    boolean preserveBackendCatalogName = Boolean.parseBoolean(get(
        properties,
        "federation.preserve-backend-catalog-name",
        "false"));
    ProxyConfig.ViewTextRewriteMode viewTextRewriteMode = parseViewTextRewriteMode(
        trimToNull(properties.getProperty("federation.view-text-rewrite.mode")));
    boolean preserveOriginalViewText = Boolean.parseBoolean(get(
        properties,
        "federation.view-text-rewrite.preserve-original-text",
        "false"));
    ProxyConfig.TransactionalDdlGuardMode transactionalDdlGuardMode = parseTransactionalDdlGuardMode(
        trimToNull(properties.getProperty("guard.transactional-ddl.mode")));
    String[] transactionalDdlClientAddresses =
        splitCsv(get(properties, "guard.transactional-ddl.client-addresses", ""));
    ClientAddressMatcher.parseAll(Arrays.asList(transactionalDdlClientAddresses));
    boolean managementPortConfigured = properties.containsKey("management.port");
    boolean managementEnabled = Boolean.parseBoolean(
        get(properties, "management.enabled", Boolean.toString(managementPortConfigured)));
    int managementPort = getInt(properties, "management.port", port + 1000);
    if (managementEnabled && (managementPort < 1 || managementPort > 65535)) {
      throw new IllegalArgumentException(
          "management.port must be between 1 and 65535, got: " + managementPort);
    }
    String managementBindHost = get(properties, "management.bind-host", server.bindHost());
    boolean syntheticReadLockZooKeeperConfigured =
        hasConfiguredPrefix(properties, "synthetic-read-lock.store.zookeeper.");
    ProxyConfig.SyntheticReadLockStoreMode syntheticReadLockStoreMode = parseSyntheticReadLockStoreMode(
        trimToNull(properties.getProperty("synthetic-read-lock.store.mode")),
        syntheticReadLockZooKeeperConfigured);
    String syntheticReadLockZooKeeperZnode =
        trimToNull(properties.getProperty("synthetic-read-lock.store.zookeeper.znode"));
    if (properties.containsKey("synthetic-read-lock.store.zookeeper.znode")
        && syntheticReadLockZooKeeperZnode == null) {
      throw new IllegalArgumentException("synthetic-read-lock.store.zookeeper.znode must not be blank");
    }
    int syntheticReadLockConnectionTimeoutMs =
        getPositiveInt(properties, "synthetic-read-lock.store.zookeeper.connection-timeout-ms", 15_000);
    int syntheticReadLockSessionTimeoutMs =
        getPositiveInt(properties, "synthetic-read-lock.store.zookeeper.session-timeout-ms", 60_000);
    int syntheticReadLockBaseSleepMs =
        getPositiveInt(properties, "synthetic-read-lock.store.zookeeper.base-sleep-ms", 1_000);
    int syntheticReadLockMaxRetries =
        getPositiveInt(properties, "synthetic-read-lock.store.zookeeper.max-retries", 3);
    boolean backendStatePollingEnabled =
        Boolean.parseBoolean(get(properties, "routing.backend-state-polling.enabled", "false"));
    int backendStatePollingIntervalMs =
        getPositiveInt(properties, "routing.backend-state-polling.interval-ms", 10_000);
    boolean adaptiveTimeoutEnabled =
        Boolean.parseBoolean(get(properties, "routing.adaptive-timeout.enabled", "false"));
    long adaptiveTimeoutInitialMs =
        getPositiveLong(properties, "routing.adaptive-timeout.initial-ms", 5_000L);
    long adaptiveTimeoutMinMs =
        getPositiveLong(properties, "routing.adaptive-timeout.min-ms", 1_000L);
    long adaptiveTimeoutMaxMs =
        getPositiveLong(properties, "routing.adaptive-timeout.max-ms", 60_000L);
    double adaptiveTimeoutMultiplier =
        getPositiveDouble(properties, "routing.adaptive-timeout.multiplier", 4.0d);
    double adaptiveTimeoutAlpha =
        getBoundedDouble(properties, "routing.adaptive-timeout.alpha", 0.2d, 0.0d, 1.0d);
    boolean circuitBreakerEnabled =
        Boolean.parseBoolean(get(properties, "routing.circuit-breaker.enabled", "false"));
    int circuitBreakerFailureThreshold =
        getPositiveInt(properties, "routing.circuit-breaker.failure-threshold", 3);
    long circuitBreakerOpenStateMs =
        getPositiveLong(properties, "routing.circuit-breaker.open-state-ms", 30_000L);
    boolean hedgedReadEnabled =
        Boolean.parseBoolean(get(properties, "routing.hedged-read.enabled", "false"));
    ProxyConfig.DegradedRoutingPolicy degradedRoutingPolicy = parseDegradedRoutingPolicy(
        trimToNull(properties.getProperty("routing.degraded-routing-policy")));

    ProxyConfig.SecurityMode securityMode = ProxyConfig.SecurityMode.valueOf(
        get(properties, "security.mode", "NONE").trim().toUpperCase());
    String serverPrincipal = trimToNull(properties.getProperty("security.server-principal"));
    String clientPrincipal = trimToNull(properties.getProperty("security.client-principal"));
    String keytab = trimToNull(properties.getProperty("security.keytab"));
    String clientKeytab = trimToNull(properties.getProperty("security.client-keytab"));
    boolean impersonationEnabled =
        Boolean.parseBoolean(get(properties, "security.impersonation-enabled", "false"));
    Map<String, String> frontDoorConf = properties.stringPropertyNames().stream()
        .filter(name -> name.startsWith("security.front-door-conf."))
        .sorted()
        .collect(Collectors.toMap(
            name -> name.substring("security.front-door-conf.".length()),
            properties::getProperty,
            (left, right) -> right,
            LinkedHashMap::new));
    Map<String, String> backendConf = properties.stringPropertyNames().stream()
        .filter(name -> name.startsWith("backend.conf."))
        .sorted()
        .collect(Collectors.toMap(
            name -> name.substring("backend.conf.".length()),
            properties::getProperty,
            (left, right) -> right,
            LinkedHashMap::new));

    String catalogsValue = require(properties, "catalogs");
    Map<String, ProxyConfig.CatalogConfig> catalogs = new LinkedHashMap<>();
    for (String catalogName : splitCsv(catalogsValue)) {
      String prefix = "catalog." + catalogName + ".";
      boolean catalogImpersonationEnabled =
          Boolean.parseBoolean(get(properties, prefix + "impersonation-enabled", Boolean.toString(impersonationEnabled)));
      ProxyConfig.CatalogAccessMode catalogAccessMode = parseCatalogAccessMode(
          trimToNull(properties.getProperty(prefix + "access-mode")));
      String[] catalogWriteDbWhitelist = splitCsv(get(properties, prefix + "write-db-whitelist", ""));
      ProxyConfig.CatalogExposureMode catalogExposureMode = parseCatalogExposureMode(
          trimToNull(properties.getProperty(prefix + "expose-mode")));
      String[] catalogExposeDbPatterns = splitCsv(get(properties, prefix + "expose-db-patterns", ""));
      validateRegexList(prefix + "expose-db-patterns", catalogExposeDbPatterns);
      Map<String, List<String>> catalogExposeTablePatterns = parseExposeTablePatterns(properties, prefix);
      MetastoreRuntimeProfile catalogRuntimeProfile = parseRuntimeProfile(
          trimToNull(properties.getProperty(prefix + "runtime-profile")));
      String catalogBackendStandaloneMetastoreJar =
          trimToNull(properties.getProperty(prefix + "backend-standalone-metastore-jar"));
      long latencyBudgetMs = getNonNegativeLong(properties, prefix + "latency-budget-ms", 0L);
      Map<String, String> hiveConfOverrides = properties.stringPropertyNames().stream()
          .filter(name -> name.startsWith(prefix + "conf."))
          .sorted()
          .collect(Collectors.toMap(
              name -> name.substring((prefix + "conf.").length()),
              properties::getProperty,
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
          get(properties, prefix + "description", catalogName),
          get(properties, prefix + "location-uri", "file:///warehouse/" + catalogName),
          catalogImpersonationEnabled,
          catalogAccessMode,
          Arrays.asList(catalogWriteDbWhitelist),
          catalogExposureMode,
          Arrays.asList(catalogExposeDbPatterns),
          catalogExposeTablePatterns,
          catalogRuntimeProfile,
          catalogBackendStandaloneMetastoreJar,
          hiveConf,
          latencyBudgetMs));
    }

    String defaultCatalog = trimToNull(properties.getProperty("routing.default-catalog"));
    if (defaultCatalog == null) {
      if (catalogs.size() == 1) {
        defaultCatalog = catalogs.keySet().iterator().next();
      } else {
        throw new IllegalArgumentException(
            "routing.default-catalog is required when more than one catalog is configured");
      }
    }
    if (!catalogs.containsKey(defaultCatalog)) {
      throw new IllegalArgumentException(
          "Unknown routing.default-catalog: " + defaultCatalog);
    }

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

    ProxyConfig.SecurityConfig security = new ProxyConfig.SecurityConfig(
        securityMode,
        serverPrincipal,
        clientPrincipal,
        keytab,
        clientKeytab,
        impersonationEnabled,
        frontDoorConf);
    ProxyConfig.BackendConfig backend = new ProxyConfig.BackendConfig(backendConf);
    ProxyConfig.CompatibilityConfig compatibility =
        new ProxyConfig.CompatibilityConfig(
            frontendProfile,
            frontendStandaloneMetastoreJar,
            backendStandaloneMetastoreJar,
            preserveBackendCatalogName);
    ProxyConfig.FederationConfig federation = new ProxyConfig.FederationConfig(
        preserveBackendCatalogName,
        viewTextRewriteMode,
        preserveOriginalViewText);
    ProxyConfig.TransactionalDdlGuardConfig transactionalDdlGuard =
        new ProxyConfig.TransactionalDdlGuardConfig(
            transactionalDdlGuardMode,
            Arrays.asList(transactionalDdlClientAddresses));
    ProxyConfig.ManagementConfig management =
        new ProxyConfig.ManagementConfig(managementEnabled, managementBindHost, managementPort);
    ProxyConfig.SyntheticReadLockStoreZooKeeperConfig syntheticReadLockZooKeeper =
        new ProxyConfig.SyntheticReadLockStoreZooKeeperConfig(
            trimToNull(properties.getProperty("synthetic-read-lock.store.zookeeper.connect-string")),
            syntheticReadLockZooKeeperZnode,
            syntheticReadLockConnectionTimeoutMs,
            syntheticReadLockSessionTimeoutMs,
            syntheticReadLockBaseSleepMs,
            syntheticReadLockMaxRetries);
    if (syntheticReadLockStoreMode == ProxyConfig.SyntheticReadLockStoreMode.ZOOKEEPER) {
      requireNonBlank(
          syntheticReadLockZooKeeper.connectString(),
          "synthetic-read-lock.store.zookeeper.connect-string");
    }
    ProxyConfig.SyntheticReadLockStoreConfig syntheticReadLockStore =
        new ProxyConfig.SyntheticReadLockStoreConfig(syntheticReadLockStoreMode, syntheticReadLockZooKeeper);
    ProxyConfig.RateLimitPolicyConfig principalRateLimit =
        parseRateLimitPolicy(properties, "rate-limit.principal");
    ProxyConfig.RateLimitPolicyConfig sourceRateLimit =
        parseRateLimitPolicy(properties, "rate-limit.source");
    Map<String, ProxyConfig.SourceCidrRateLimitConfig> sourceCidrRateLimits =
        parseSourceCidrRateLimits(properties);
    Map<String, ProxyConfig.RateLimitPolicyConfig> methodFamilyRateLimits =
        parseRateLimitPolicies(properties, "rate-limit.method-family.", SUPPORTED_RATE_LIMIT_METHOD_FAMILIES, true);
    Map<String, ProxyConfig.RateLimitPolicyConfig> catalogRateLimits =
        parseRateLimitPolicies(properties, "rate-limit.catalog.", null, false);
    for (String catalogName : catalogRateLimits.keySet()) {
      if (!catalogs.containsKey(catalogName)) {
        throw new IllegalArgumentException("Unknown rate-limit.catalog entry: " + catalogName);
      }
    }
    Map<String, ProxyConfig.RateLimitPolicyConfig> rpcClassRateLimits =
        parseRateLimitPolicies(properties, "rate-limit.rpc-class.", SUPPORTED_RATE_LIMIT_RPC_CLASSES, true);
    ProxyConfig.RateLimitConfig rateLimit = new ProxyConfig.RateLimitConfig(
        principalRateLimit,
        sourceRateLimit,
        sourceCidrRateLimits,
        methodFamilyRateLimits,
        catalogRateLimits,
        rpcClassRateLimits);
    ProxyConfig.LatencyRoutingConfig latencyRouting = new ProxyConfig.LatencyRoutingConfig(
        new ProxyConfig.BackendStatePollingConfig(backendStatePollingEnabled, backendStatePollingIntervalMs),
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
            Math.max(1, Math.min(catalogs.size(), getPositiveInt(
                properties,
                "routing.hedged-read.max-parallelism",
                Math.max(1, catalogs.size()))))),
        degradedRoutingPolicy);
    return new ProxyConfig(
        server,
        security,
        catalogDbSeparator,
        defaultCatalog,
        catalogs,
        backend,
        compatibility,
        federation,
        transactionalDdlGuard,
        management,
        syntheticReadLockStore,
        rateLimit,
        latencyRouting);
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
      return ProxyConfig.TransactionalDdlGuardMode.valueOf(value.trim().toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid value for guard.transactional-ddl.mode: " + value + ". Expected one of: reject, rewrite", e);
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
      return ProxyConfig.ViewTextRewriteMode.valueOf(value.trim().toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid value for federation.view-text-rewrite.mode: " + value
              + ". Expected one of: DISABLED, REWRITE",
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
