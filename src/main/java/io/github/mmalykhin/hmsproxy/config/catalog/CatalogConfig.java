package io.github.mmalykhin.hmsproxy.config.catalog;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.github.mmalykhin.hmsproxy.config.server.MetastoreRuntimeProfile;
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
    long latencyBudgetMs,
    int maxImpersonationClients,
    long impersonationClientIdleTtlMs,
    int sharedSessionPoolSize,
    int impersonationPoolMaxSize,
    long impersonationSessionIdleTtlMs,
    io.github.mmalykhin.hmsproxy.config.security.CatalogRangerConfig ranger
) {
  public static final CatalogAccessMode DEFAULT_ACCESS_MODE = CatalogAccessMode.READ_WRITE;
  public static final CatalogExposureMode DEFAULT_EXPOSE_MODE = CatalogExposureMode.ALLOW_ALL;
  public static final long DEFAULT_LATENCY_BUDGET_MS = 0L;
  public static final int DEFAULT_MAX_IMPERSONATION_CLIENTS = 128;
  public static final long DEFAULT_IMPERSONATION_CLIENT_IDLE_TTL_MS = 0L;
  public static final int DEFAULT_SHARED_SESSION_POOL_SIZE = 1;
  public static final int DEFAULT_IMPERSONATION_POOL_MAX_SIZE = 4;
  public static final long DEFAULT_IMPERSONATION_SESSION_IDLE_TTL_MS = 0L;

  // The property loader validates sizing keys strictly and fails startup on a non-positive value;
  // these substitutions only cover the in-process builder path, where an unset field reads as 0.
  public CatalogConfig {
    accessMode = accessMode == null ? DEFAULT_ACCESS_MODE : accessMode;
    exposeMode = exposeMode == null ? DEFAULT_EXPOSE_MODE : exposeMode;
    writeDbWhitelist = writeDbWhitelist == null ? List.of() : List.copyOf(writeDbWhitelist);
    exposeDbPatterns = exposeDbPatterns == null ? List.of() : List.copyOf(exposeDbPatterns);
    latencyBudgetMs = Math.max(latencyBudgetMs, DEFAULT_LATENCY_BUDGET_MS);
    maxImpersonationClients =
        maxImpersonationClients <= 0 ? DEFAULT_MAX_IMPERSONATION_CLIENTS : maxImpersonationClients;
    impersonationClientIdleTtlMs =
        Math.max(impersonationClientIdleTtlMs, DEFAULT_IMPERSONATION_CLIENT_IDLE_TTL_MS);
    sharedSessionPoolSize =
        sharedSessionPoolSize <= 0 ? DEFAULT_SHARED_SESSION_POOL_SIZE : sharedSessionPoolSize;
    impersonationPoolMaxSize =
        impersonationPoolMaxSize <= 0 ? DEFAULT_IMPERSONATION_POOL_MAX_SIZE : impersonationPoolMaxSize;
    impersonationSessionIdleTtlMs =
        Math.max(impersonationSessionIdleTtlMs, DEFAULT_IMPERSONATION_SESSION_IDLE_TTL_MS);
    ranger = ranger == null ? io.github.mmalykhin.hmsproxy.config.security.CatalogRangerConfig.disabled() : ranger;
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
        DEFAULT_EXPOSE_MODE,
        List.of(),
        Map.of(),
        runtimeProfile,
        backendStandaloneMetastoreJar,
        hiveConf,
        DEFAULT_LATENCY_BUDGET_MS,
        DEFAULT_MAX_IMPERSONATION_CLIENTS,
        DEFAULT_IMPERSONATION_CLIENT_IDLE_TTL_MS,
        DEFAULT_SHARED_SESSION_POOL_SIZE,
        DEFAULT_IMPERSONATION_POOL_MAX_SIZE,
        DEFAULT_IMPERSONATION_SESSION_IDLE_TTL_MS,
        io.github.mmalykhin.hmsproxy.config.security.CatalogRangerConfig.disabled());
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
        DEFAULT_LATENCY_BUDGET_MS,
        DEFAULT_MAX_IMPERSONATION_CLIENTS,
        DEFAULT_IMPERSONATION_CLIENT_IDLE_TTL_MS,
        DEFAULT_SHARED_SESSION_POOL_SIZE,
        DEFAULT_IMPERSONATION_POOL_MAX_SIZE,
        DEFAULT_IMPERSONATION_SESSION_IDLE_TTL_MS,
        io.github.mmalykhin.hmsproxy.config.security.CatalogRangerConfig.disabled());
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
      Map<String, String> hiveConf,
      long latencyBudgetMs,
      int maxImpersonationClients,
      long impersonationClientIdleTtlMs,
      int sharedSessionPoolSize,
      int impersonationPoolMaxSize,
      long impersonationSessionIdleTtlMs
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
        latencyBudgetMs,
        maxImpersonationClients,
        impersonationClientIdleTtlMs,
        sharedSessionPoolSize,
        impersonationPoolMaxSize,
        impersonationSessionIdleTtlMs,
        io.github.mmalykhin.hmsproxy.config.security.CatalogRangerConfig.disabled());
  }
}
