package io.github.mmalykhin.hmsproxy.config.management;

import io.github.mmalykhin.hmsproxy.util.ClientAddressMatcher;
import java.util.List;

public record ManagementConfig(
    boolean enabled,
    String bindHost,
    int port,
    int threads,
    long readinessCacheMs,
    boolean requireAllCatalogs,
    List<String> allowedClientIps,
    boolean readyzIncludeDetails
) {
  public static final String DEFAULT_BIND_HOST = "127.0.0.1";
  public static final int DEFAULT_THREADS = 4;
  public static final long DEFAULT_READINESS_CACHE_MS = 2_000L;
  public static final boolean DEFAULT_REQUIRE_ALL_CATALOGS = true;
  public static final boolean DEFAULT_READYZ_INCLUDE_DETAILS = false;

  public ManagementConfig {
    if (threads < 1) {
      throw new IllegalArgumentException("management.threads must be >= 1, got: " + threads);
    }
    if (readinessCacheMs < 0L) {
      throw new IllegalArgumentException(
          "management.readiness-cache-ms must be >= 0, got: " + readinessCacheMs);
    }
    allowedClientIps = allowedClientIps == null ? List.of() : List.copyOf(allowedClientIps);
    ClientAddressMatcher.parseAll(allowedClientIps);
  }

  public ManagementConfig(
      boolean enabled,
      String bindHost,
      int port,
      int threads,
      long readinessCacheMs,
      boolean requireAllCatalogs
  ) {
    this(
        enabled,
        bindHost,
        port,
        threads,
        readinessCacheMs,
        requireAllCatalogs,
        List.of(),
        DEFAULT_READYZ_INCLUDE_DETAILS);
  }

  public ManagementConfig(boolean enabled, String bindHost, int port, int threads, long readinessCacheMs) {
    this(
        enabled,
        bindHost,
        port,
        threads,
        readinessCacheMs,
        DEFAULT_REQUIRE_ALL_CATALOGS,
        List.of(),
        DEFAULT_READYZ_INCLUDE_DETAILS);
  }

  public ManagementConfig(boolean enabled, String bindHost, int port) {
    this(
        enabled,
        bindHost,
        port,
        DEFAULT_THREADS,
        DEFAULT_READINESS_CACHE_MS,
        DEFAULT_REQUIRE_ALL_CATALOGS,
        List.of(),
        DEFAULT_READYZ_INCLUDE_DETAILS);
  }
}
