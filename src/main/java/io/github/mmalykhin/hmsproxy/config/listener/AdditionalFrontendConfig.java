package io.github.mmalykhin.hmsproxy.config.listener;

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
    String standaloneMetastoreJar
) {
}
