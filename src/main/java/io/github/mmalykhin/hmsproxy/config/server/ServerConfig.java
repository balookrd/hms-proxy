package io.github.mmalykhin.hmsproxy.config.server;

public record ServerConfig(
    String name,
    String bindHost,
    int port,
    int minWorkerThreads,
    int maxWorkerThreads,
    ClientSocketConfig clientSocket,
    int shutdownTimeoutSeconds
) {
  public static final int DEFAULT_SHUTDOWN_TIMEOUT_SECONDS = 30;

  public ServerConfig {
    clientSocket = clientSocket == null ? ClientSocketConfig.defaults() : clientSocket;
    shutdownTimeoutSeconds = shutdownTimeoutSeconds < 1
        ? DEFAULT_SHUTDOWN_TIMEOUT_SECONDS : shutdownTimeoutSeconds;
  }

  /** Convenience form for callers that do not tune front-door socket lifetime. */
  public ServerConfig(
      String name,
      String bindHost,
      int port,
      int minWorkerThreads,
      int maxWorkerThreads
  ) {
    this(name, bindHost, port, minWorkerThreads, maxWorkerThreads,
        ClientSocketConfig.defaults(), DEFAULT_SHUTDOWN_TIMEOUT_SECONDS);
  }
}
