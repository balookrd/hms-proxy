package io.github.mmalykhin.hmsproxy.config.listener;

import io.github.mmalykhin.hmsproxy.config.server.ClientSocketConfig;
import io.github.mmalykhin.hmsproxy.config.server.FrontendProfile;

/**
 * Configuration for a single additional Thrift listener that runs in parallel
 * with the primary {@code server.*} listener. Each additional listener has its
 * own port and may advertise a different FrontendProfile (e.g. APACHE_3_1_3 on
 * 9083 and APACHE_4_1_0 on 9084 for the same proxy). Routing/federation/audit
 * pipeline is shared; only the wire-level Thrift API differs.
 */
public record AdditionalFrontendConfig(
    String name,
    String bindHost,
    int port,
    int minWorkerThreads,
    int maxWorkerThreads,
    FrontendProfile frontendProfile,
    String standaloneMetastoreJar,
    ClientSocketConfig clientSocket
) {
  public AdditionalFrontendConfig {
    clientSocket = clientSocket == null ? ClientSocketConfig.defaults() : clientSocket;
  }

  /** Convenience form for callers that do not tune front-door socket lifetime. */
  public AdditionalFrontendConfig(
      String name,
      String bindHost,
      int port,
      int minWorkerThreads,
      int maxWorkerThreads,
      FrontendProfile frontendProfile,
      String standaloneMetastoreJar
  ) {
    this(name, bindHost, port, minWorkerThreads, maxWorkerThreads, frontendProfile,
        standaloneMetastoreJar, ClientSocketConfig.defaults());
  }
}
