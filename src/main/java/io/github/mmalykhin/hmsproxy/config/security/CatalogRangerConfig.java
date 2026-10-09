package io.github.mmalykhin.hmsproxy.config.security;

public record CatalogRangerConfig(
    boolean enabled,
    String policyRestUrl,
    String serviceName,
    String serviceType,
    String appId,
    String policyCacheDir,
    long policyPollIntervalMs,
    int connectionTimeoutMs,
    int readTimeoutMs,
    String sslTruststoreFile,
    String sslTruststorePassword,
    String configDir,
    boolean auditEnabled,
    boolean rolesEnabled,
    boolean maskUnauthorizedAsNotFound
) {
  public static final String DEFAULT_SERVICE_TYPE = "hive";
  public static final String DEFAULT_APP_ID = "hms-proxy";
  public static final long DEFAULT_POLL_INTERVAL_MS = 30_000L;
  public static final int DEFAULT_CONNECTION_TIMEOUT_MS = 5_000;
  public static final int DEFAULT_READ_TIMEOUT_MS = 10_000;
  public static final boolean DEFAULT_ROLES_ENABLED = true;
  public static final boolean DEFAULT_MASK_UNAUTHORIZED_AS_NOT_FOUND = false;

  public CatalogRangerConfig(
      boolean enabled,
      String policyRestUrl,
      String serviceName,
      String serviceType,
      String appId,
      String policyCacheDir,
      long policyPollIntervalMs,
      int connectionTimeoutMs,
      int readTimeoutMs,
      String sslTruststoreFile,
      String sslTruststorePassword,
      String configDir,
      boolean auditEnabled
  ) {
    this(
        enabled, policyRestUrl, serviceName, serviceType, appId, policyCacheDir,
        policyPollIntervalMs, connectionTimeoutMs, readTimeoutMs, sslTruststoreFile,
        sslTruststorePassword, configDir, auditEnabled, DEFAULT_ROLES_ENABLED,
        DEFAULT_MASK_UNAUTHORIZED_AS_NOT_FOUND);
  }

  public CatalogRangerConfig(
      boolean enabled,
      String policyRestUrl,
      String serviceName,
      String serviceType,
      String appId,
      String policyCacheDir,
      long policyPollIntervalMs,
      int connectionTimeoutMs,
      int readTimeoutMs,
      String sslTruststoreFile,
      String sslTruststorePassword,
      String configDir,
      boolean auditEnabled,
      boolean rolesEnabled
  ) {
    this(
        enabled, policyRestUrl, serviceName, serviceType, appId, policyCacheDir,
        policyPollIntervalMs, connectionTimeoutMs, readTimeoutMs, sslTruststoreFile,
        sslTruststorePassword, configDir, auditEnabled, rolesEnabled,
        DEFAULT_MASK_UNAUTHORIZED_AS_NOT_FOUND);
  }

  public CatalogRangerConfig {
    serviceType = serviceType == null || serviceType.isBlank() ? DEFAULT_SERVICE_TYPE : serviceType.trim();
    appId = appId == null || appId.isBlank() ? DEFAULT_APP_ID : appId.trim();
    policyPollIntervalMs = policyPollIntervalMs <= 0 ? DEFAULT_POLL_INTERVAL_MS : policyPollIntervalMs;
    connectionTimeoutMs = connectionTimeoutMs <= 0 ? DEFAULT_CONNECTION_TIMEOUT_MS : connectionTimeoutMs;
    readTimeoutMs = readTimeoutMs <= 0 ? DEFAULT_READ_TIMEOUT_MS : readTimeoutMs;
  }

  public static CatalogRangerConfig disabled() {
    return new CatalogRangerConfig(
        false, null, null, DEFAULT_SERVICE_TYPE, DEFAULT_APP_ID, null,
        DEFAULT_POLL_INTERVAL_MS, DEFAULT_CONNECTION_TIMEOUT_MS, DEFAULT_READ_TIMEOUT_MS,
        null, null, null, false, false, DEFAULT_MASK_UNAUTHORIZED_AS_NOT_FOUND);
  }
}
