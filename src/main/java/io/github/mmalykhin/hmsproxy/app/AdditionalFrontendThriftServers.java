package io.github.mmalykhin.hmsproxy.app;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.compatibility.CompatibilityConfig;
import io.github.mmalykhin.hmsproxy.config.listener.AdditionalFrontendConfig;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.security.FrontDoorSecurity;
import io.github.mmalykhin.hmsproxy.security.MetastoreThriftServer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns the additional Thrift listeners declared via {@code additional-frontends.*}.
 * Each listener gets its own thread and shadow {@link ProxyConfig} where the
 * {@code server.*} and {@code compatibility.frontend-*} sections are overridden
 * by the per-listener config; the rest of the proxy (routing, federation,
 * security, observability) is shared with the primary listener.
 */
public final class AdditionalFrontendThriftServers implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(AdditionalFrontendThriftServers.class);
  private static final long STOP_TIMEOUT_SECONDS = 5L;

  private final List<RunningListener> running;

  private AdditionalFrontendThriftServers(List<RunningListener> running) {
    this.running = running;
  }

  public static AdditionalFrontendThriftServers open(
      ProxyConfig config,
      ThriftHiveMetastore.Iface handler,
      FrontDoorSecurity frontDoorSecurity
  ) throws Exception {
    List<AdditionalFrontendConfig> extras = config.additionalFrontends();
    if (extras.isEmpty()) {
      return new AdditionalFrontendThriftServers(List.of());
    }
    List<RunningListener> started = new ArrayList<>();
    try {
      for (AdditionalFrontendConfig extra : extras) {
        ProxyConfig shadow = shadowConfig(config, extra);
        MetastoreThriftServer server = new MetastoreThriftServer(shadow, handler, frontDoorSecurity);
        Thread thread = new Thread(server::serve, "hms-proxy-fe-" + extra.name());
        thread.setDaemon(false);
        thread.start();
        LOG.info("Additional frontend listener '{}' started on {}:{} (profile={}, threads={}..{})",
            extra.name(), extra.bindHost(), extra.port(), extra.frontendProfile(),
            extra.minWorkerThreads(), extra.maxWorkerThreads());
        started.add(new RunningListener(server, thread, extra));
      }
      return new AdditionalFrontendThriftServers(List.copyOf(started));
    } catch (Throwable t) {
      // Cleanup partial state so a failed listener does not leak threads/ports.
      for (RunningListener listener : started) {
        safeStop(listener);
      }
      throw t;
    }
  }

  public List<AdditionalFrontendConfig> running() {
    List<AdditionalFrontendConfig> result = new ArrayList<>();
    for (RunningListener listener : running) {
      result.add(listener.extra());
    }
    return List.copyOf(result);
  }

  @Override
  public void close() {
    if (running.isEmpty()) {
      return;
    }
    for (RunningListener listener : running) {
      safeStop(listener);
    }
    for (RunningListener listener : running) {
      try {
        listener.thread().join(TimeUnit.SECONDS.toMillis(STOP_TIMEOUT_SECONDS));
        if (listener.thread().isAlive()) {
          LOG.warn("Additional frontend listener '{}' did not stop within {}s",
              listener.extra().name(), STOP_TIMEOUT_SECONDS);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.warn("Interrupted while waiting for additional frontend listener '{}' to stop",
            listener.extra().name());
        return;
      }
    }
  }

  private static void safeStop(RunningListener listener) {
    try {
      listener.server().stop();
    } catch (Exception e) {
      LOG.warn("Failed to stop additional frontend listener '{}'", listener.extra().name(), e);
    }
  }

  private static ProxyConfig shadowConfig(ProxyConfig base, AdditionalFrontendConfig extra) {
    ServerConfig shadowServer = new ServerConfig(
        base.server().name() + "-" + extra.name(),
        extra.bindHost(),
        extra.port(),
        extra.minWorkerThreads(),
        extra.maxWorkerThreads());
    CompatibilityConfig shadowCompat = new CompatibilityConfig(
        extra.frontendProfile(),
        extra.standaloneMetastoreJar(),
        base.compatibility().backendStandaloneMetastoreJar(),
        base.compatibility().preserveBackendCatalogName());
    return ProxyConfig.builder()
        .server(shadowServer)
        .security(base.security())
        .catalogDbSeparator(base.catalogDbSeparator())
        .defaultCatalog(base.defaultCatalog())
        .catalogs(base.catalogs())
        .backend(base.backend())
        .compatibility(shadowCompat)
        .federation(base.federation())
        .transactionalDdlGuard(base.transactionalDdlGuard())
        .management(base.management())
        .syntheticReadLockStore(base.syntheticReadLockStore())
        .rateLimit(base.rateLimit())
        .latencyRouting(base.latencyRouting())
        // Do NOT carry additionalFrontends into the shadow config — would recurse.
        .additionalFrontends(List.of())
        .build();
  }

  private record RunningListener(
      MetastoreThriftServer server,
      Thread thread,
      AdditionalFrontendConfig extra
  ) {
  }
}
