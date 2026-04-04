package io.github.mmalykhin.hmsproxy.config;

import io.github.mmalykhin.hmsproxy.compatibility.MetastoreRuntimeProfile;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public record ProxyConfig(
    ServerConfig server,
    SecurityConfig security,
    String catalogDbSeparator,
    String defaultCatalog,
    Map<String, CatalogConfig> catalogs,
    BackendConfig backend,
    CompatibilityConfig compatibility,
    FederationConfig federation,
    TransactionalDdlGuardConfig transactionalDdlGuard,
    ManagementConfig management,
    SyntheticReadLockStoreConfig syntheticReadLockStore,
    RateLimitConfig rateLimit,
    LatencyRoutingConfig latencyRouting
) {
  public ProxyConfig {
    catalogs = Map.copyOf(catalogs);
    backend = backend == null ? new BackendConfig(Map.of()) : backend;
    compatibility = compatibility == null
        ? new CompatibilityConfig(FrontendProfile.APACHE_3_1_3, null, null, false)
        : compatibility;
    federation = federation == null
        ? new FederationConfig(compatibility.preserveBackendCatalogName(), ViewTextRewriteMode.DISABLED, false)
        : federation;
    transactionalDdlGuard = transactionalDdlGuard == null
        ? new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.DISABLED, List.of())
        : transactionalDdlGuard;
    management = management == null
        ? new ManagementConfig(false, server.bindHost(), server.port() + 1000)
        : management;
    syntheticReadLockStore = syntheticReadLockStore == null
        ? new SyntheticReadLockStoreConfig(SyntheticReadLockStoreMode.IN_MEMORY, null)
        : syntheticReadLockStore;
    rateLimit = rateLimit == null ? RateLimitConfig.disabled() : rateLimit;
    latencyRouting = latencyRouting == null ? LatencyRoutingConfig.disabled() : latencyRouting;
  }

  public ProxyConfig(
      ServerConfig server,
      SecurityConfig security,
      String catalogDbSeparator,
      String defaultCatalog,
      Map<String, CatalogConfig> catalogs
  ) {
    this(server, security, catalogDbSeparator, defaultCatalog, catalogs, new BackendConfig(Map.of()),
        new CompatibilityConfig(FrontendProfile.APACHE_3_1_3, null, null, false),
        new FederationConfig(false, ViewTextRewriteMode.DISABLED, false),
        new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.DISABLED, List.of()),
        new ManagementConfig(false, server.bindHost(), server.port() + 1000),
        new SyntheticReadLockStoreConfig(SyntheticReadLockStoreMode.IN_MEMORY, null),
        RateLimitConfig.disabled(),
        LatencyRoutingConfig.disabled());
  }

  public ProxyConfig(
      ServerConfig server,
      SecurityConfig security,
      String catalogDbSeparator,
      String defaultCatalog,
      Map<String, CatalogConfig> catalogs,
      BackendConfig backend,
      CompatibilityConfig compatibility,
      FederationConfig federation,
      TransactionalDdlGuardConfig transactionalDdlGuard,
      ManagementConfig management
  ) {
    this(
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
        new SyntheticReadLockStoreConfig(SyntheticReadLockStoreMode.IN_MEMORY, null),
        RateLimitConfig.disabled(),
        LatencyRoutingConfig.disabled());
  }

  public ProxyConfig(
      ServerConfig server,
      SecurityConfig security,
      String catalogDbSeparator,
      String defaultCatalog,
      Map<String, CatalogConfig> catalogs,
      CompatibilityConfig compatibility
  ) {
    this(server, security, catalogDbSeparator, defaultCatalog, catalogs, new BackendConfig(Map.of()), compatibility,
        new FederationConfig(false, ViewTextRewriteMode.DISABLED, false),
        new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.DISABLED, List.of()),
        new ManagementConfig(false, server.bindHost(), server.port() + 1000),
        new SyntheticReadLockStoreConfig(SyntheticReadLockStoreMode.IN_MEMORY, null),
        RateLimitConfig.disabled(),
        LatencyRoutingConfig.disabled());
  }

  public ProxyConfig(
      ServerConfig server,
      SecurityConfig security,
      String catalogDbSeparator,
      String defaultCatalog,
      Map<String, CatalogConfig> catalogs,
      CompatibilityConfig compatibility,
      TransactionalDdlGuardConfig transactionalDdlGuard
  ) {
    this(server, security, catalogDbSeparator, defaultCatalog, catalogs, new BackendConfig(Map.of()), compatibility,
        new FederationConfig(
            compatibility != null && compatibility.preserveBackendCatalogName(),
            ViewTextRewriteMode.DISABLED,
            false),
        transactionalDdlGuard,
        new ManagementConfig(false, server.bindHost(), server.port() + 1000),
        new SyntheticReadLockStoreConfig(SyntheticReadLockStoreMode.IN_MEMORY, null),
        RateLimitConfig.disabled(),
        LatencyRoutingConfig.disabled());
  }

  public ProxyConfig(
      ServerConfig server,
      SecurityConfig security,
      String catalogDbSeparator,
      String defaultCatalog,
      Map<String, CatalogConfig> catalogs,
      BackendConfig backend,
      CompatibilityConfig compatibility
  ) {
    this(server, security, catalogDbSeparator, defaultCatalog, catalogs, backend, compatibility,
        new FederationConfig(false, ViewTextRewriteMode.DISABLED, false),
        new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.DISABLED, List.of()),
        new ManagementConfig(false, server.bindHost(), server.port() + 1000),
        new SyntheticReadLockStoreConfig(SyntheticReadLockStoreMode.IN_MEMORY, null),
        RateLimitConfig.disabled(),
        LatencyRoutingConfig.disabled());
  }

  public ProxyConfig(
      ServerConfig server,
      SecurityConfig security,
      String catalogDbSeparator,
      String defaultCatalog,
      Map<String, CatalogConfig> catalogs,
      CompatibilityConfig compatibility,
      FederationConfig federation,
      TransactionalDdlGuardConfig transactionalDdlGuard
  ) {
    this(server, security, catalogDbSeparator, defaultCatalog, catalogs, new BackendConfig(Map.of()), compatibility,
        federation,
        transactionalDdlGuard,
        new ManagementConfig(false, server.bindHost(), server.port() + 1000),
        new SyntheticReadLockStoreConfig(SyntheticReadLockStoreMode.IN_MEMORY, null),
        RateLimitConfig.disabled(),
        LatencyRoutingConfig.disabled());
  }

  public ProxyConfig(
      ServerConfig server,
      SecurityConfig security,
      String catalogDbSeparator,
      String defaultCatalog,
      Map<String, CatalogConfig> catalogs,
      BackendConfig backend,
      CompatibilityConfig compatibility,
      FederationConfig federation,
      TransactionalDdlGuardConfig transactionalDdlGuard,
      ManagementConfig management,
      SyntheticReadLockStoreConfig syntheticReadLockStore
  ) {
    this(
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
        RateLimitConfig.disabled(),
        LatencyRoutingConfig.disabled());
  }

  public ProxyConfig(
      ServerConfig server,
      SecurityConfig security,
      String catalogDbSeparator,
      String defaultCatalog,
      Map<String, CatalogConfig> catalogs,
      BackendConfig backend,
      CompatibilityConfig compatibility,
      FederationConfig federation,
      TransactionalDdlGuardConfig transactionalDdlGuard,
      ManagementConfig management,
      SyntheticReadLockStoreConfig syntheticReadLockStore,
      RateLimitConfig rateLimit
  ) {
    this(
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
        LatencyRoutingConfig.disabled());
  }

  public record ServerConfig(
      String name,
      String bindHost,
      int port,
      int minWorkerThreads,
      int maxWorkerThreads
  ) {
  }

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

  public record CatalogConfig(
      String name,
      String description,
      String locationUri,
      boolean impersonationEnabled,
      CatalogAccessMode accessMode,
      List<String> writeDbWhitelist,
      CatalogExposureMode exposeMode,
      List<String> exposeDbPatterns,
      Map<String, List<String>> exposeTablePatterns,
      MetastoreRuntimeProfile runtimeProfile,
      String backendStandaloneMetastoreJar,
      Map<String, String> hiveConf,
      long latencyBudgetMs
  ) {
    public CatalogConfig {
      accessMode = accessMode == null ? CatalogAccessMode.READ_WRITE : accessMode;
      exposeMode = exposeMode == null ? CatalogExposureMode.ALLOW_ALL : exposeMode;
      writeDbWhitelist = writeDbWhitelist == null ? List.of() : List.copyOf(writeDbWhitelist);
      exposeDbPatterns = exposeDbPatterns == null ? List.of() : List.copyOf(exposeDbPatterns);
      latencyBudgetMs = Math.max(latencyBudgetMs, 0L);
      Map<String, List<String>> copiedExposeTablePatterns = new LinkedHashMap<>();
      for (Map.Entry<String, List<String>> entry : (exposeTablePatterns == null ? Map.<String, List<String>>of() : exposeTablePatterns).entrySet()) {
        copiedExposeTablePatterns.put(entry.getKey(), List.copyOf(entry.getValue()));
      }
      exposeTablePatterns = Collections.unmodifiableMap(copiedExposeTablePatterns);
      hiveConf = Map.copyOf(hiveConf);
    }

    public CatalogConfig(
        String name,
        String description,
        String locationUri,
        boolean impersonationEnabled,
        CatalogAccessMode accessMode,
        List<String> writeDbWhitelist,
        MetastoreRuntimeProfile runtimeProfile,
        String backendStandaloneMetastoreJar,
        Map<String, String> hiveConf
    ) {
      this(
          name,
          description,
          locationUri,
          impersonationEnabled,
          accessMode,
          writeDbWhitelist,
          CatalogExposureMode.ALLOW_ALL,
          List.of(),
          Map.of(),
          runtimeProfile,
          backendStandaloneMetastoreJar,
          hiveConf,
          0L);
    }

    public CatalogConfig(
        String name,
        String description,
        String locationUri,
        boolean impersonationEnabled,
        CatalogAccessMode accessMode,
        List<String> writeDbWhitelist,
        CatalogExposureMode exposeMode,
        List<String> exposeDbPatterns,
        Map<String, List<String>> exposeTablePatterns,
        MetastoreRuntimeProfile runtimeProfile,
        String backendStandaloneMetastoreJar,
        Map<String, String> hiveConf
    ) {
      this(
          name,
          description,
          locationUri,
          impersonationEnabled,
          accessMode,
          writeDbWhitelist,
          exposeMode,
          exposeDbPatterns,
          exposeTablePatterns,
          runtimeProfile,
          backendStandaloneMetastoreJar,
          hiveConf,
          0L);
    }
  }

  public record BackendConfig(
      Map<String, String> hiveConf
  ) {
    public BackendConfig {
      hiveConf = Map.copyOf(hiveConf);
    }
  }

  public record CompatibilityConfig(
      FrontendProfile frontendProfile,
      String frontendStandaloneMetastoreJar,
      String backendStandaloneMetastoreJar,
      boolean preserveBackendCatalogName
  ) {
    public CompatibilityConfig {
      frontendProfile = frontendProfile == null ? FrontendProfile.APACHE_3_1_3 : frontendProfile;
    }

    public CompatibilityConfig(FrontendProfile frontendProfile) {
      this(frontendProfile, null, null, false);
    }

    public CompatibilityConfig(FrontendProfile frontendProfile, boolean preserveBackendCatalogName) {
      this(frontendProfile, null, null, preserveBackendCatalogName);
    }

    public CompatibilityConfig(boolean preserveBackendCatalogName) {
      this(FrontendProfile.APACHE_3_1_3, null, null, preserveBackendCatalogName);
    }

  }

  public record FederationConfig(
      boolean preserveBackendCatalogName,
      ViewTextRewriteMode viewTextRewriteMode,
      boolean preserveOriginalViewText
  ) {
    public FederationConfig {
      viewTextRewriteMode = viewTextRewriteMode == null ? ViewTextRewriteMode.DISABLED : viewTextRewriteMode;
    }

    public boolean viewTextRewriteEnabled() {
      return viewTextRewriteMode == ViewTextRewriteMode.REWRITE;
    }
  }

  public enum ViewTextRewriteMode {
    DISABLED,
    REWRITE
  }

  public record TransactionalDdlGuardConfig(
      TransactionalDdlGuardMode mode,
      List<String> clientAddressRules
  ) {
    public TransactionalDdlGuardConfig {
      mode = mode == null ? TransactionalDdlGuardMode.DISABLED : mode;
      clientAddressRules = List.copyOf(clientAddressRules);
    }

    public boolean enabled() {
      return mode != TransactionalDdlGuardMode.DISABLED;
    }

    public boolean rewriteEnabled() {
      return mode == TransactionalDdlGuardMode.REWRITE;
    }

    public boolean rejectEnabled() {
      return mode == TransactionalDdlGuardMode.REJECT;
    }
  }

  public record ManagementConfig(
      boolean enabled,
      String bindHost,
      int port
  ) {
  }

  public record RateLimitConfig(
      RateLimitPolicyConfig principal,
      RateLimitPolicyConfig source,
      Map<String, SourceCidrRateLimitConfig> sourceCidrs,
      Map<String, RateLimitPolicyConfig> methodFamilies,
      Map<String, RateLimitPolicyConfig> catalogs,
      Map<String, RateLimitPolicyConfig> rpcClasses
  ) {
    public RateLimitConfig {
      principal = principal == null ? RateLimitPolicyConfig.disabled() : principal;
      source = source == null ? RateLimitPolicyConfig.disabled() : source;
      sourceCidrs = copySourceCidrs(sourceCidrs);
      methodFamilies = copyPolicies(methodFamilies);
      catalogs = copyPolicies(catalogs);
      rpcClasses = copyPolicies(rpcClasses);
    }

    public static RateLimitConfig disabled() {
      return new RateLimitConfig(
          RateLimitPolicyConfig.disabled(),
          RateLimitPolicyConfig.disabled(),
          Map.of(),
          Map.of(),
          Map.of(),
          Map.of());
    }

    public boolean enabled() {
      return principal.enabled()
          || source.enabled()
          || hasEnabledPolicies(sourceCidrs.values().stream().map(SourceCidrRateLimitConfig::policy).toList())
          || hasEnabledPolicies(methodFamilies.values())
          || hasEnabledPolicies(catalogs.values())
          || hasEnabledPolicies(rpcClasses.values());
    }

    private static Map<String, SourceCidrRateLimitConfig> copySourceCidrs(
        Map<String, SourceCidrRateLimitConfig> sourceCidrs
    ) {
      if (sourceCidrs == null || sourceCidrs.isEmpty()) {
        return Map.of();
      }
      Map<String, SourceCidrRateLimitConfig> copied = new LinkedHashMap<>();
      sourceCidrs.forEach((name, config) -> copied.put(name, config));
      return Collections.unmodifiableMap(copied);
    }

    private static Map<String, RateLimitPolicyConfig> copyPolicies(Map<String, RateLimitPolicyConfig> policies) {
      if (policies == null || policies.isEmpty()) {
        return Map.of();
      }
      Map<String, RateLimitPolicyConfig> copied = new LinkedHashMap<>();
      policies.forEach((name, config) -> copied.put(name, config == null ? RateLimitPolicyConfig.disabled() : config));
      return Collections.unmodifiableMap(copied);
    }

    private static boolean hasEnabledPolicies(Iterable<RateLimitPolicyConfig> policies) {
      for (RateLimitPolicyConfig policy : policies) {
        if (policy != null && policy.enabled()) {
          return true;
        }
      }
      return false;
    }
  }

  public record SourceCidrRateLimitConfig(
      List<String> cidrRules,
      RateLimitPolicyConfig policy
  ) {
    public SourceCidrRateLimitConfig {
      cidrRules = cidrRules == null ? List.of() : List.copyOf(cidrRules);
      policy = policy == null ? RateLimitPolicyConfig.disabled() : policy;
    }

    public boolean enabled() {
      return !cidrRules.isEmpty() && policy.enabled();
    }
  }

  public record RateLimitPolicyConfig(
      int requestsPerSecond,
      int burst
  ) {
    public RateLimitPolicyConfig {
      requestsPerSecond = Math.max(requestsPerSecond, 0);
      burst = burst <= 0 ? requestsPerSecond : burst;
    }

    public static RateLimitPolicyConfig disabled() {
      return new RateLimitPolicyConfig(0, 0);
    }

    public boolean enabled() {
      return requestsPerSecond > 0 && burst > 0;
    }
  }

  public record LatencyRoutingConfig(
      BackendStatePollingConfig backendStatePolling,
      AdaptiveTimeoutConfig adaptiveTimeout,
      CircuitBreakerConfig circuitBreaker,
      HedgedReadConfig hedgedRead,
      DegradedRoutingPolicy degradedRoutingPolicy
  ) {
    public LatencyRoutingConfig {
      backendStatePolling =
          backendStatePolling == null ? new BackendStatePollingConfig(false, 10_000) : backendStatePolling;
      adaptiveTimeout = adaptiveTimeout == null
          ? new AdaptiveTimeoutConfig(false, 5_000L, 1_000L, 60_000L, 4.0d, 0.2d)
          : adaptiveTimeout;
      circuitBreaker = circuitBreaker == null ? new CircuitBreakerConfig(false, 3, 30_000L) : circuitBreaker;
      hedgedRead = hedgedRead == null ? new HedgedReadConfig(false, 8) : hedgedRead;
      degradedRoutingPolicy =
          degradedRoutingPolicy == null ? DegradedRoutingPolicy.STRICT : degradedRoutingPolicy;
    }

    public static LatencyRoutingConfig disabled() {
      return new LatencyRoutingConfig(
          new BackendStatePollingConfig(false, 10_000),
          new AdaptiveTimeoutConfig(false, 5_000L, 1_000L, 60_000L, 4.0d, 0.2d),
          new CircuitBreakerConfig(false, 3, 30_000L),
          new HedgedReadConfig(false, 8),
          DegradedRoutingPolicy.STRICT);
    }
  }

  public record BackendStatePollingConfig(
      boolean enabled,
      int intervalMs
  ) {
    public BackendStatePollingConfig {
      intervalMs = intervalMs <= 0 ? 10_000 : intervalMs;
    }
  }

  public record AdaptiveTimeoutConfig(
      boolean enabled,
      long initialTimeoutMs,
      long minTimeoutMs,
      long maxTimeoutMs,
      double multiplier,
      double alpha
  ) {
    public AdaptiveTimeoutConfig {
      initialTimeoutMs = initialTimeoutMs <= 0 ? 5_000L : initialTimeoutMs;
      minTimeoutMs = minTimeoutMs <= 0 ? 1_000L : minTimeoutMs;
      maxTimeoutMs = maxTimeoutMs <= 0 ? 60_000L : maxTimeoutMs;
      if (maxTimeoutMs < minTimeoutMs) {
        maxTimeoutMs = minTimeoutMs;
      }
      if (initialTimeoutMs < minTimeoutMs) {
        initialTimeoutMs = minTimeoutMs;
      }
      if (initialTimeoutMs > maxTimeoutMs) {
        initialTimeoutMs = maxTimeoutMs;
      }
      multiplier = multiplier <= 1.0d ? 4.0d : multiplier;
      alpha = alpha <= 0.0d || alpha > 1.0d ? 0.2d : alpha;
    }
  }

  public record CircuitBreakerConfig(
      boolean enabled,
      int failureThreshold,
      long openStateMs
  ) {
    public CircuitBreakerConfig {
      failureThreshold = failureThreshold <= 0 ? 3 : failureThreshold;
      openStateMs = openStateMs <= 0 ? 30_000L : openStateMs;
    }
  }

  public record HedgedReadConfig(
      boolean enabled,
      int maxParallelism
  ) {
    public HedgedReadConfig {
      maxParallelism = maxParallelism <= 0 ? 8 : maxParallelism;
    }
  }

  public enum DegradedRoutingPolicy {
    STRICT,
    SAFE_FANOUT_READS
  }

  public record SyntheticReadLockStoreConfig(
      SyntheticReadLockStoreMode mode,
      SyntheticReadLockStoreZooKeeperConfig zooKeeper
  ) {
    public SyntheticReadLockStoreConfig {
      mode = mode == null ? SyntheticReadLockStoreMode.IN_MEMORY : mode;
      zooKeeper = zooKeeper == null
          ? new SyntheticReadLockStoreZooKeeperConfig(null, "/hms-proxy-synthetic-read-locks", 15_000, 60_000, 1_000, 3)
          : zooKeeper;
    }

    public boolean zooKeeperEnabled() {
      return mode == SyntheticReadLockStoreMode.ZOOKEEPER;
    }
  }

  public record SyntheticReadLockStoreZooKeeperConfig(
      String connectString,
      String znode,
      int connectionTimeoutMs,
      int sessionTimeoutMs,
      int baseSleepMs,
      int maxRetries
  ) {
    public SyntheticReadLockStoreZooKeeperConfig {
      znode = (znode == null || znode.isBlank()) ? "/hms-proxy-synthetic-read-locks" : znode;
      connectionTimeoutMs = connectionTimeoutMs <= 0 ? 15_000 : connectionTimeoutMs;
      sessionTimeoutMs = sessionTimeoutMs <= 0 ? 60_000 : sessionTimeoutMs;
      baseSleepMs = baseSleepMs <= 0 ? 1_000 : baseSleepMs;
      maxRetries = maxRetries <= 0 ? 3 : maxRetries;
    }
  }

  public enum TransactionalDdlGuardMode {
    DISABLED,
    REJECT,
    REWRITE
  }

  public enum SyntheticReadLockStoreMode {
    IN_MEMORY,
    ZOOKEEPER
  }

  public enum CatalogAccessMode {
    READ_ONLY,
    READ_WRITE,
    READ_WRITE_DB_WHITELIST
  }

  public enum CatalogExposureMode {
    ALLOW_ALL,
    DENY_BY_DEFAULT
  }

  public enum FrontendProfile {
    APACHE_3_1_3(MetastoreRuntimeProfile.APACHE_3_1_3),
    HORTONWORKS_3_1_0_3_1_0_78(MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78),
    HORTONWORKS_3_1_0_3_1_5_6150_1(MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_5_6150_1);

    private final MetastoreRuntimeProfile runtimeProfile;

    FrontendProfile(MetastoreRuntimeProfile runtimeProfile) {
      this.runtimeProfile = runtimeProfile;
    }

    public MetastoreRuntimeProfile runtimeProfile() {
      return runtimeProfile;
    }

    public String metastoreVersion() {
      return runtimeProfile.metastoreVersion();
    }

    public String defaultStandaloneMetastoreJar() {
      return runtimeProfile.defaultStandaloneMetastoreJar();
    }
  }

  public enum SecurityMode {
    NONE,
    KERBEROS;

    public String hadoopAuthValue() {
      return this == KERBEROS ? "kerberos" : "simple";
    }
  }

  public List<String> catalogNames() {
    return catalogs.keySet().stream().sorted().toList();
  }
}
