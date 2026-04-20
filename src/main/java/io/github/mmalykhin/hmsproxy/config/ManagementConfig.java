package io.github.mmalykhin.hmsproxy.config;

public record ManagementConfig(
    boolean enabled,
    String bindHost,
    int port
) {
}
