package io.github.mmalykhin.hmsproxy.config.management;

public record ManagementConfig(
    boolean enabled,
    String bindHost,
    int port
) {
}
