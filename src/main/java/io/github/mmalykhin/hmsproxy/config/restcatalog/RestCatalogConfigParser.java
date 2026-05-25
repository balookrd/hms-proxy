package io.github.mmalykhin.hmsproxy.config.restcatalog;

import io.github.mmalykhin.hmsproxy.config.PropertyReader;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;

public final class RestCatalogConfigParser {
  private static final int DEFAULT_PORT_OFFSET = 100;
  private static final int DEFAULT_MIN_THREADS = 8;
  private static final int DEFAULT_MAX_THREADS = 64;

  private RestCatalogConfigParser() {
  }

  public static RestCatalogConfig parse(PropertyReader reader, ServerConfig server) {
    boolean portConfigured = reader.has("rest-catalog.port");
    boolean enabled = reader.getBoolean("rest-catalog.enabled", portConfigured);
    int port = reader.getInt("rest-catalog.port", server.port() + DEFAULT_PORT_OFFSET);
    if (enabled && (port < 1 || port > 65535)) {
      throw new IllegalArgumentException(
          "rest-catalog.port must be between 1 and 65535, got: " + port);
    }
    String bindHost = reader.get("rest-catalog.bind-host", server.bindHost());
    int minWorkerThreads = reader.getPositiveInt("rest-catalog.min-worker-threads", DEFAULT_MIN_THREADS);
    int maxWorkerThreads = reader.getInt("rest-catalog.max-worker-threads", DEFAULT_MAX_THREADS);
    if (maxWorkerThreads < minWorkerThreads) {
      throw new IllegalArgumentException(
          "rest-catalog.max-worker-threads (" + maxWorkerThreads
              + ") must be >= rest-catalog.min-worker-threads (" + minWorkerThreads + ")");
    }
    return new RestCatalogConfig(enabled, bindHost, port, minWorkerThreads, maxWorkerThreads);
  }
}
