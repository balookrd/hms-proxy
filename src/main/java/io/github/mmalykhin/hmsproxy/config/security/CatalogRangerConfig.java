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
    boolean auditEnabled
) {
  public static final String DEFAULT_SERVICE_TYPE = "hive";
  public static final String DEFAULT_APP_ID = "hms-proxy";
  public static final long DEFAULT_POLL_INTERVAL_MS = 30_000L;
  public static final int DEFAULT_CONNECTION_TIMEOUT_MS = 5_000;
  public static final int DEFAULT_READ_TIMEOUT_MS = 10_000;

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
        null, null, null, false);
  }
}
