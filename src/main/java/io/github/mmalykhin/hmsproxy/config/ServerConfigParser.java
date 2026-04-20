package io.github.mmalykhin.hmsproxy.config;

final class ServerConfigParser {
  private ServerConfigParser() {
  }

  static ServerConfig parse(PropertyReader reader) {
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
        maxWorkerThreads);
  }
}
