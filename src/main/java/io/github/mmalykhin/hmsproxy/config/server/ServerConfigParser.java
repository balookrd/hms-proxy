package io.github.mmalykhin.hmsproxy.config.server;


import io.github.mmalykhin.hmsproxy.config.PropertyReader;
public final class ServerConfigParser {
  private ServerConfigParser() {
  }

  public static ServerConfig parse(PropertyReader reader) {
    int port = reader.getInt("server.port", 9083);
    if (port < 1 || port > 65535) {
      throw new IllegalArgumentException("server.port must be between 1 and 65535, got: " + port);
    }
    int minWorkerThreads = reader.getInt("server.min-worker-threads", 16);
    if (minWorkerThreads < 1) {
      throw new IllegalArgumentException("server.min-worker-threads must be >= 1, got: " + minWorkerThreads);
    }
    int maxWorkerThreads = reader.getInt("server.max-worker-threads", 256);
    if (maxWorkerThreads < minWorkerThreads) {
      throw new IllegalArgumentException(
          "server.max-worker-threads (" + maxWorkerThreads
              + ") must be >= server.min-worker-threads (" + minWorkerThreads + ")");
    }
    return new ServerConfig(
        reader.get("server.name", "hms-proxy"),
        reader.get("server.bind-host", "0.0.0.0"),
        port,
        minWorkerThreads,
        maxWorkerThreads,
        parseClientSocket(reader, "server.", ClientSocketConfig.defaults()),
        reader.getPositiveInt("server.shutdown-timeout-seconds",
            ServerConfig.DEFAULT_SHUTDOWN_TIMEOUT_SECONDS));
  }

  /**
   * Parses the front-door socket keys under {@code scope}, falling back to {@code fallback} so
   * additional listeners inherit the primary {@code server.*} values unless they override them.
   */
  public static ClientSocketConfig parseClientSocket(
      PropertyReader reader,
      String scope,
      ClientSocketConfig fallback
  ) {
    return new ClientSocketConfig(
        reader.getNonNegativeInt(scope + "client-socket-timeout-ms", fallback.clientTimeoutMs()),
        reader.getBoolean(scope + "tcp-keepalive", fallback.tcpKeepAlive()),
        reader.getPositiveInt(scope + "tcp-keepalive-idle-seconds", fallback.keepAliveIdleSeconds()),
        reader.getPositiveInt(scope + "tcp-keepalive-interval-seconds", fallback.keepAliveIntervalSeconds()),
        reader.getPositiveInt(scope + "tcp-keepalive-count", fallback.keepAliveCount()));
  }
}
