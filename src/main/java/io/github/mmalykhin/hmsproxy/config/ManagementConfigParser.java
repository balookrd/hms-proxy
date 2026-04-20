package io.github.mmalykhin.hmsproxy.config;

final class ManagementConfigParser {
  private ManagementConfigParser() {
  }

  static ManagementConfig parse(PropertyReader reader, ServerConfig server) {
    boolean managementPortConfigured = reader.has("management.port");
    boolean managementEnabled = reader.getBoolean("management.enabled", managementPortConfigured);
    int managementPort = reader.getInt("management.port", server.port() + 1000);
    if (managementEnabled && (managementPort < 1 || managementPort > 65535)) {
      throw new IllegalArgumentException(
          "management.port must be between 1 and 65535, got: " + managementPort);
    }
    String managementBindHost = reader.get("management.bind-host", server.bindHost());
    return new ManagementConfig(managementEnabled, managementBindHost, managementPort);
  }
}
