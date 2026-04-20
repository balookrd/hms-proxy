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
    int sharedSessionPoolSize
) {
  public CatalogConfig {
    accessMode = accessMode == null ? CatalogAccessMode.READ_WRITE : accessMode;
    exposeMode = exposeMode == null ? CatalogExposureMode.ALLOW_ALL : exposeMode;
    writeDbWhitelist = writeDbWhitelist == null ? List.of() : List.copyOf(writeDbWhitelist);
    exposeDbPatterns = exposeDbPatterns == null ? List.of() : List.copyOf(exposeDbPatterns);
    latencyBudgetMs = Math.max(latencyBudgetMs, 0L);
    maxImpersonationClients = maxImpersonationClients <= 0 ? 128 : maxImpersonationClients;
    impersonationClientIdleTtlMs = Math.max(impersonationClientIdleTtlMs, 0L);
    sharedSessionPoolSize = sharedSessionPoolSize <= 0 ? 1 : sharedSessionPoolSize;
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
        0L,
        128,
        0L,
        1);
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
        0L,
        128,
        0L,
        1);
  }
}
