package io.github.mmalykhin.hmsproxy.app;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.observability.KerberosHealthProbe;
import io.github.mmalykhin.hmsproxy.observability.ProxyObservability;
import io.github.mmalykhin.hmsproxy.observability.ProxyRuntimeState;
import io.github.mmalykhin.hmsproxy.routing.CatalogRouter;
import java.io.IOException;
import java.io.OutputStream;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ManagementHttpServer implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ManagementHttpServer.class);

  private final HttpServer server;
  private final ExecutorService httpExecutor;
  private final ExecutorService readinessProbeExecutor;

  private ManagementHttpServer(
      HttpServer server,
      ExecutorService httpExecutor,
      ExecutorService readinessProbeExecutor) {
    this.server = server;
    this.httpExecutor = httpExecutor;
    this.readinessProbeExecutor = readinessProbeExecutor;
  }

  public static ManagementHttpServer open(
      ProxyConfig config,
      CatalogRouter router,
      ProxyObservability observability
  ) throws IOException {
    if (!config.management().enabled()) {
      return null;
    }

    HttpServer server;
    try {
      server = HttpServer.create(
          new InetSocketAddress(config.management().bindHost(), config.management().port()), 0);
    } catch (BindException e) {
      LOG.error("Failed to bind management HTTP listener on {}:{} - {}",
          config.management().bindHost(), config.management().port(), e.getMessage());
      throw e;
    }
    server.createContext("/healthz", exchange -> {
      String body = "{\"status\":\"ok\",\"alive\":true,\"uptimeSeconds\":"
          + (System.currentTimeMillis() / 1000L - observability.runtimeState().startedAtEpochSecond())
          + "}\n";
      respond(exchange, 200, "application/json; charset=utf-8", body);
    });
    ExecutorService readinessProbeExecutor = null;
    if (!config.latencyRouting().backendStatePolling().enabled() && !router.backends().isEmpty()) {
      int probePoolSize = Math.max(1, Math.min(
          config.latencyRouting().backendStatePolling().maxParallelism(),
          router.backends().size()));
      readinessProbeExecutor = new ThreadPoolExecutor(
          probePoolSize,
          probePoolSize,
          0L,
          TimeUnit.MILLISECONDS,
          new ArrayBlockingQueue<>(router.backends().size()),
          namedThreadFactory("hms-proxy-readyz-probe"),
          new ThreadPoolExecutor.CallerRunsPolicy());
    }
    server.createContext("/readyz", new ReadinessHandler(config, router, observability, readinessProbeExecutor));
    server.createContext("/metrics", exchange -> respond(
        exchange,
        200,
        "text/plain; version=0.0.4; charset=utf-8",
        observability.metrics().render()));
    // Without an explicit executor the built-in HttpServer serves every context from its single
    // dispatcher thread, so one /readyz call blocked on an unreachable backend would also stall
    // liveness checks and metric scrapes. Readiness probing is single-flight (see ReadinessHandler),
    // so at most one of these threads can be parked on backend I/O at a time.
    int managementThreads = config.management().threads();
    ExecutorService httpExecutor = new ThreadPoolExecutor(
        managementThreads,
        managementThreads,
        0L,
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(),
        namedThreadFactory("hms-proxy-management"));
    server.setExecutor(httpExecutor);
    server.start();
    LOG.info("Management HTTP listener started on {}:{} with {} handler thread(s), "
            + "readiness probe cache {}ms",
        config.management().bindHost(), config.management().port(), managementThreads,
        config.management().readinessCacheMs());
    return new ManagementHttpServer(server, httpExecutor, readinessProbeExecutor);
  }

  ExecutorService httpExecutor() {
    return httpExecutor;
  }

  @Override
  public void close() {
    server.stop(0);
    shutdown(httpExecutor, "Management HTTP executor");
    shutdown(readinessProbeExecutor, "Readiness probe executor");
  }

  private static void shutdown(ExecutorService executor, String description) {
    if (executor == null) {
      return;
    }
    executor.shutdownNow();
    try {
      if (!executor.awaitTermination(5L, TimeUnit.SECONDS)) {
        LOG.warn("{} did not terminate within 5s after shutdown", description);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static ThreadFactory namedThreadFactory(String prefix) {
    return runnable -> {
      Thread thread = new Thread(runnable);
      thread.setName(prefix + "-" + thread.getId());
      thread.setDaemon(true);
      return thread;
    };
  }

  private static void respond(HttpExchange exchange, int status, String contentType, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", contentType);
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream output = exchange.getResponseBody()) {
      output.write(bytes);
    }
  }

  private static final class ReadinessHandler implements HttpHandler {
    private final ProxyConfig config;
    private final CatalogRouter router;
    private final ProxyObservability observability;
    private final ExecutorService probeExecutor;
    private final SingleFlightCache<ProbeSnapshot> probeCache;

    private ReadinessHandler(
        ProxyConfig config,
        CatalogRouter router,
        ProxyObservability observability,
        ExecutorService probeExecutor) {
      this.config = config;
      this.router = router;
      this.observability = observability;
      this.probeExecutor = probeExecutor;
      this.probeCache = new SingleFlightCache<>(config.management().readinessCacheMs());
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
      // Backend and Kerberos probes are the expensive part of readiness; they are refreshed at most
      // once per readiness-cache-ms so that frequent scrapes do not fan out network checks. The
      // backend status fields below are read fresh from in-memory runtime state on every request.
      ProbeSnapshot probes = probeCache.get(this::refreshProbes);
      long probeAgeMs = Math.max(0L, System.currentTimeMillis() - probes.probedAtEpochMs());
      KerberosHealthProbe.KerberosStatus frontDoorKerberos = probes.frontDoor();
      KerberosHealthProbe.KerberosStatus backendKerberos = probes.backend();
      List<ProxyRuntimeState.BackendRuntimeStatus> statuses = observability.runtimeState().backendStatuses();
      boolean backendConnectivity = statuses.stream().allMatch(ProxyRuntimeState.BackendRuntimeStatus::connected);
      boolean ready = statuses.stream().allMatch(status ->
          status.connected()
              && !status.degraded()
              && status.circuitState() == ProxyRuntimeState.CircuitState.CLOSED)
          && frontDoorKerberos.healthy()
          && backendKerberos.healthy();
      StringBuilder body = new StringBuilder(512);
      body.append("{\"status\":\"").append(ready ? "ready" : "degraded").append("\",")
          .append("\"alive\":true,")
          .append("\"probeAgeMs\":").append(probeAgeMs).append(',')
          .append("\"backendConnectivity\":").append(backendConnectivity).append(',')
          .append("\"kerberos\":{")
          .append("\"frontDoor\":").append(renderKerberos(frontDoorKerberos)).append(',')
          .append("\"backend\":").append(renderKerberos(backendKerberos)).append("},")
          .append("\"backends\":[");
      for (int index = 0; index < statuses.size(); index++) {
        ProxyRuntimeState.BackendRuntimeStatus status = statuses.get(index);
        if (index > 0) {
          body.append(',');
        }
        body.append('{')
            .append("\"backend\":\"").append(escape(status.backend())).append("\",")
            .append("\"connected\":").append(status.connected()).append(',')
            .append("\"degraded\":").append(status.degraded()).append(',')
            .append("\"lastSuccessEpochSecond\":").append(status.lastSuccessEpochSecond()).append(',')
            .append("\"lastFailureEpochSecond\":").append(status.lastFailureEpochSecond()).append(',')
            .append("\"lastProbeEpochSecond\":").append(status.lastProbeEpochSecond()).append(',')
            .append("\"lastLatencyMs\":").append(status.lastLatencyMs()).append(',')
            .append("\"latencyEwmaMs\":").append(status.latencyEwmaMs()).append(',')
            .append("\"baselineTimeoutMs\":").append(status.baselineTimeoutMs()).append(',')
            .append("\"adaptiveTimeoutMs\":").append(status.adaptiveTimeoutMs()).append(',')
            .append("\"latencyBudgetMs\":").append(status.latencyBudgetMs()).append(',')
            .append("\"circuitState\":\"").append(status.circuitState()).append("\",")
            .append("\"consecutiveFailures\":").append(status.consecutiveFailures()).append(',')
            .append("\"circuitRetryAtEpochMs\":").append(status.circuitRetryAtEpochMs()).append(',')
            .append("\"lastError\":");
        if (status.lastError() == null) {
          body.append("null");
        } else {
          body.append('"').append(escape(status.lastError())).append('"');
        }
        body.append('}');
      }
      body.append("]}\n");
      respond(exchange, ready ? 200 : 503, "application/json; charset=utf-8", body.toString());
    }

    private ProbeSnapshot refreshProbes() {
      // probeExecutor is only created when background backend-state polling is disabled; with
      // polling on, readiness reuses the poller's results and only refreshes Kerberos status.
      if (probeExecutor != null) {
        runReadinessProbes();
      }
      return new ProbeSnapshot(
          configKerberosStatus(), backendKerberosStatus(), System.currentTimeMillis());
    }

    private record ProbeSnapshot(
        KerberosHealthProbe.KerberosStatus frontDoor,
        KerberosHealthProbe.KerberosStatus backend,
        long probedAtEpochMs) {
    }

    private void runReadinessProbes() {
      long probeTimeoutMs = config.latencyRouting().backendStatePolling().probeTimeoutMs();
      List<CatalogBackend> backends = new ArrayList<>(router.backends());
      List<ProbeInFlight> inFlight = new ArrayList<>(backends.size());
      for (CatalogBackend backend : backends) {
        long startedAt = System.nanoTime();
        Future<?> probe = probeExecutor.submit((Callable<Void>) () -> {
          try {
            backend.probeConnectivity(probeTimeoutMs);
          } catch (Exception e) {
            throw e;
          } catch (Throwable t) {
            throw new RuntimeException(t);
          }
          return null;
        });
        inFlight.add(new ProbeInFlight(backend, startedAt, probe));
      }
      long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(probeTimeoutMs);
      for (ProbeInFlight entry : inFlight) {
        try {
          long remainingNanos = deadlineNanos - System.nanoTime();
          if (remainingNanos <= 0L) {
            entry.probe.cancel(true);
            throw new TimeoutException("readiness probe deadline exceeded");
          }
          entry.probe.get(remainingNanos, TimeUnit.NANOSECONDS);
          observability.runtimeState().recordBackendProbeSuccess(
              entry.backend.name(),
              (System.nanoTime() - entry.startedAt) / 1_000_000L,
              config.latencyRouting());
        } catch (TimeoutException e) {
          entry.probe.cancel(true);
          observability.runtimeState().recordBackendProbeFailure(
              entry.backend.name(), e, config.latencyRouting());
        } catch (ExecutionException e) {
          observability.runtimeState().recordBackendProbeFailure(
              entry.backend.name(),
              e.getCause() != null ? e.getCause() : e,
              config.latencyRouting());
        } catch (InterruptedException e) {
          entry.probe.cancel(true);
          Thread.currentThread().interrupt();
          observability.runtimeState().recordBackendProbeFailure(
              entry.backend.name(), e, config.latencyRouting());
          return;
        }
      }
    }

    private record ProbeInFlight(CatalogBackend backend, long startedAt, Future<?> probe) {
    }

    private KerberosHealthProbe.KerberosStatus configKerberosStatus() {
      if (!config.security().kerberosEnabled()) {
        return KerberosHealthProbe.disabled("frontDoor");
      }
      return KerberosHealthProbe.probeLoginUser("frontDoor", config.security().serverPrincipal());
    }

    private KerberosHealthProbe.KerberosStatus backendKerberosStatus() {
      boolean backendKerberosEnabled = config.catalogs().values().stream()
          .anyMatch(catalog -> Boolean.parseBoolean(catalog.hiveConf().getOrDefault("hive.metastore.sasl.enabled", "false")));
      if (!backendKerberosEnabled) {
        return KerberosHealthProbe.disabled("backend");
      }
      return KerberosHealthProbe.probeBackendLogin("backend", config.security().outboundPrincipal());
    }

  }

  static String escape(String value) {
    StringBuilder sb = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      switch (c) {
        case '"':  sb.append("\\\""); break;
        case '\\': sb.append("\\\\"); break;
        case '\n': sb.append("\\n");  break;
        case '\r': sb.append("\\r");  break;
        case '\t': sb.append("\\t");  break;
        case '\b': sb.append("\\b");  break;
        case '\f': sb.append("\\f");  break;
        default:
          if (c < 0x20) {
            sb.append(String.format("\\u%04x", (int) c));
          } else {
            sb.append(c);
          }
      }
    }
    return sb.toString();
  }

  static String renderKerberos(KerberosHealthProbe.KerberosStatus status) {
    StringBuilder builder = new StringBuilder(160);
    builder.append('{')
        .append("\"component\":\"").append(escape(status.component())).append("\",")
        .append("\"enabled\":").append(status.enabled()).append(',')
        .append("\"state\":\"").append(status.state()).append("\",")
        .append("\"loggedIn\":").append(status.loggedIn()).append(',')
        .append("\"healthy\":").append(status.healthy()).append(',')
        .append("\"principal\":");
    if (status.principal() == null) {
      builder.append("null");
    } else {
      builder.append('"').append(escape(status.principal())).append('"');
    }
    builder.append(",\"checkedAtEpochSecond\":").append(status.checkedAtEpochSecond() == null ? "null" : status.checkedAtEpochSecond())
        .append(",\"tgtExpiresAtEpochSecond\":").append(status.tgtExpiresAtEpochSecond() == null ? "null" : status.tgtExpiresAtEpochSecond())
        .append(",\"secondsUntilExpiry\":").append(status.secondsUntilExpiry() == null ? "null" : status.secondsUntilExpiry())
        .append(",\"detail\":");
    if (status.detail() == null) {
      builder.append("null");
    } else {
      builder.append('"').append(escape(status.detail())).append('"');
    }
    builder.append('}');
    return builder.toString();
  }
}
