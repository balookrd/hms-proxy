package io.github.mmalykhin.hmsproxy.config.server;

/**
 * Lifetime settings applied to every accepted front-door client socket.
 *
 * <p>libthrift 0.9.3 accepts client sockets with an infinite SO_TIMEOUT and no TCP keepalive,
 * so a client that dies without FIN/RST (network partition, kill behind NAT) pins a
 * {@code TThreadPoolServer} worker thread in read forever and slowly drains the pool. These
 * settings bound both the idle-read case and the dead-peer case.
 */
public record ClientSocketConfig(
    int clientTimeoutMs,
    boolean tcpKeepAlive,
    int keepAliveIdleSeconds,
    int keepAliveIntervalSeconds,
    int keepAliveCount,
    long thriftMaxStringLengthBytes,
    long thriftMaxContainerLength,
    int saslHandshakeTimeoutMs,
    int saslMaxFrameBytes
) {
  /** Same order of magnitude as Hive's own {@code hive.metastore.client.socket.timeout} (600s). */
  public static final int DEFAULT_CLIENT_TIMEOUT_MS = 600_000;
  /** Mirrors vanilla HMS {@code hive.metastore.server.tcp.keepalive}. */
  public static final boolean DEFAULT_TCP_KEEP_ALIVE = true;
  public static final int DEFAULT_KEEP_ALIVE_IDLE_SECONDS = 120;
  public static final int DEFAULT_KEEP_ALIVE_INTERVAL_SECONDS = 30;
  public static final int DEFAULT_KEEP_ALIVE_COUNT = 4;
  /** Default cap on a single serialized Thrift string/binary field (64 MiB; 0 disables). */
  public static final long DEFAULT_THRIFT_MAX_STRING_LENGTH_BYTES = 64L * 1024L * 1024L;
  /** Default cap on elements in a single serialized Thrift list/set/map (0 disables). */
  public static final long DEFAULT_THRIFT_MAX_CONTAINER_LENGTH = 1_000_000L;
  /** Short SO_TIMEOUT applied only during the pre-auth SASL handshake (0 reuses clientTimeoutMs). */
  public static final int DEFAULT_SASL_HANDSHAKE_TIMEOUT_MS = 10_000;
  /** Max size of a single SASL negotiation frame before rejection (1 MiB; 0 disables extra cap). */
  public static final int DEFAULT_SASL_MAX_FRAME_BYTES = 1024 * 1024;

  private static final ClientSocketConfig DEFAULTS = new ClientSocketConfig(
      DEFAULT_CLIENT_TIMEOUT_MS,
      DEFAULT_TCP_KEEP_ALIVE,
      DEFAULT_KEEP_ALIVE_IDLE_SECONDS,
      DEFAULT_KEEP_ALIVE_INTERVAL_SECONDS,
      DEFAULT_KEEP_ALIVE_COUNT,
      DEFAULT_THRIFT_MAX_STRING_LENGTH_BYTES,
      DEFAULT_THRIFT_MAX_CONTAINER_LENGTH,
      DEFAULT_SASL_HANDSHAKE_TIMEOUT_MS,
      DEFAULT_SASL_MAX_FRAME_BYTES);

  public ClientSocketConfig(
      int clientTimeoutMs,
      boolean tcpKeepAlive,
      int keepAliveIdleSeconds,
      int keepAliveIntervalSeconds,
      int keepAliveCount
  ) {
    this(
        clientTimeoutMs,
        tcpKeepAlive,
        keepAliveIdleSeconds,
        keepAliveIntervalSeconds,
        keepAliveCount,
        DEFAULT_THRIFT_MAX_STRING_LENGTH_BYTES,
        DEFAULT_THRIFT_MAX_CONTAINER_LENGTH,
        DEFAULT_SASL_HANDSHAKE_TIMEOUT_MS,
        DEFAULT_SASL_MAX_FRAME_BYTES);
  }

  public ClientSocketConfig {
    clientTimeoutMs = Math.max(clientTimeoutMs, 0);
    keepAliveIdleSeconds = keepAliveIdleSeconds < 1
        ? DEFAULT_KEEP_ALIVE_IDLE_SECONDS : keepAliveIdleSeconds;
    keepAliveIntervalSeconds = keepAliveIntervalSeconds < 1
        ? DEFAULT_KEEP_ALIVE_INTERVAL_SECONDS : keepAliveIntervalSeconds;
    keepAliveCount = keepAliveCount < 1 ? DEFAULT_KEEP_ALIVE_COUNT : keepAliveCount;
    if (thriftMaxStringLengthBytes < 0L) {
      throw new IllegalArgumentException(
          "thriftMaxStringLengthBytes must be >= 0, got: " + thriftMaxStringLengthBytes);
    }
    if (thriftMaxContainerLength < 0L) {
      throw new IllegalArgumentException(
          "thriftMaxContainerLength must be >= 0, got: " + thriftMaxContainerLength);
    }
    if (saslHandshakeTimeoutMs < 0) {
      throw new IllegalArgumentException(
          "saslHandshakeTimeoutMs must be >= 0, got: " + saslHandshakeTimeoutMs);
    }
    if (saslMaxFrameBytes < 0) {
      throw new IllegalArgumentException(
          "saslMaxFrameBytes must be >= 0, got: " + saslMaxFrameBytes);
    }
  }

  public static ClientSocketConfig defaults() {
    return DEFAULTS;
  }

  /** A zero timeout keeps the libthrift default of blocking in read forever. */
  public boolean clientTimeoutEnabled() {
    return clientTimeoutMs > 0;
  }

  /** Upper bound on how long a dead peer stays undetected once keepalive tuning is applied. */
  public int keepAliveDetectionSeconds() {
    return keepAliveIdleSeconds + keepAliveIntervalSeconds * keepAliveCount;
  }
}
