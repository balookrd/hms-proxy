package io.github.mmalykhin.hmsproxy.config.server;

public record ServerConfig(
    String name,
    String bindHost,
    int port,
    int minWorkerThreads,
    int maxWorkerThreads
) {
}
