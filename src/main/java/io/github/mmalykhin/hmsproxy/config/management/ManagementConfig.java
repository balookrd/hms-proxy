package io.github.mmalykhin.hmsproxy.config.management;

public record ManagementConfig(
    boolean enabled,
    String bindHost,
    int port,
    int threads,
    long readinessCacheMs
) {
  public static final int DEFAULT_THREADS = 4;
  public static final long DEFAULT_READINESS_CACHE_MS = 2_000L;

  public ManagementConfig {
    if (threads < 1) {
      throw new IllegalArgumentException("management.threads must be >= 1, got: " + threads);
    }
    if (readinessCacheMs < 0L) {
      throw new IllegalArgumentException(
          "management.readiness-cache-ms must be >= 0, got: " + readinessCacheMs);
    }
  }

  public ManagementConfig(boolean enabled, String bindHost, int port) {
    this(enabled, bindHost, port, DEFAULT_THREADS, DEFAULT_READINESS_CACHE_MS);
  }
}
