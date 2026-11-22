package io.github.mmalykhin.hmsproxy.app;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.observability.ProxyObservability;
import io.github.mmalykhin.hmsproxy.observability.ProxyRuntimeState;
import io.github.mmalykhin.hmsproxy.routing.BackendProbeRunner;
import io.github.mmalykhin.hmsproxy.routing.CatalogRouter;
import io.github.mmalykhin.hmsproxy.security.KerberosHealthProbe;
import io.github.mmalykhin.hmsproxy.util.ClientAddressMatcher;
import io.github.mmalykhin.hmsproxy.util.HttpResponseWriter;
import io.github.mmalykhin.hmsproxy.util.HttpServerTimeouts;
import io.github.mmalykhin.hmsproxy.util.JsonEscapeUtil;
import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ManagementHttpServer implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ManagementHttpServer.class);
  private static final int HTTP_QUEUE_CAPACITY = 32;
  private static final long COLD_START_PROBE_WAIT_MS = 200L;
  private static final long REJECTION_LOG_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(60L);

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
    return open(config, router, observability, CatalogBackend::probeConnectivity);
  }

  static ManagementHttpServer open(
      ProxyConfig config,
      CatalogRouter router,
      ProxyObservability observability,
      BackendProbeRunner.BackendProbe backendProbe
  ) throws IOException {
    if (!config.management().enabled()) {
      return null;
    }
    HttpServerTimeouts.installDefaultsIfUnset();
    List<ClientAddressMatcher> allowedClients =
        ClientAddressMatcher.parseAll(config.management().allowedClientIps());

    HttpServer server;
    try {
      server = HttpServer.create(
          new InetSocketAddress(config.management().bindHost(), config.management().port()), 0);
    } catch (BindException e) {
      LOG.error("Failed to bind management HTTP listener on {}:{} - {}",
          config.management().bindHost(), config.management().port(), e.getMessage());
      throw e;
    }
    server.createContext("/healthz", wrapHandler(allowedClients, exchange -> {
      String body = "{\"status\":\"ok\",\"alive\":true,\"uptimeSeconds\":"
          + (System.currentTimeMillis() / 1000L - observability.runtimeState().startedAtEpochSecond())
          + "}\n";
      respond(exchange, 200, "application/json; charset=utf-8", body);
    }));
    ExecutorService readinessProbeExecutor = null;
    BackendProbeRunner readinessProbeRunner = null;
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
      readinessProbeRunner = new BackendProbeRunner(readinessProbeExecutor, backendProbe);
    }
    server.createContext("/readyz", wrapHandler(
        allowedClients, new ReadinessHandler(config, router, observability, readinessProbeRunner)));
    server.createContext("/metrics", wrapHandler(allowedClients, exchange -> respond(
        exchange,
        200,
        "text/plain; version=0.0.4; charset=utf-8",
        observability.metrics().render())));
    // Without an explicit executor the built-in HttpServer serves every context from its single
    // dispatcher thread, so one /readyz call blocked on an unreachable backend would also stall
    // liveness checks and metric scrapes. Readiness probing is single-flight (see ReadinessHandler),
    // so at most one of these threads can be parked on backend I/O at a time.
    int managementThreads = config.management().threads();
    AtomicLong lastRejectedWarnNanos = new AtomicLong();
    ExecutorService httpExecutor = new ThreadPoolExecutor(
        managementThreads,
        managementThreads,
        0L,
        TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(HTTP_QUEUE_CAPACITY),
        namedThreadFactory("hms-proxy-management"),
        (runnable, executor) -> {
          long now = System.nanoTime();
          long prev = lastRejectedWarnNanos.get();
          if (now - prev >= REJECTION_LOG_INTERVAL_NANOS
              && lastRejectedWarnNanos.compareAndSet(prev, now)) {
            LOG.warn("Management HTTP request queue full (capacity={}); rejecting request",
                HTTP_QUEUE_CAPACITY);
          }
          throw new RejectedExecutionException("Management HTTP request queue full");
        });
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

  private static HttpHandler wrapHandler(
      List<ClientAddressMatcher> allowedClients,
      HttpHandler delegate
  ) {
    return exchange -> {
      if (!isClientAllowed(exchange, allowedClients)) {
        respond(exchange, 403, "text/plain; charset=utf-8", "Forbidden\n");
        return;
      }
      String method = exchange.getRequestMethod();
      if (!"GET".equalsIgnoreCase(method) && !"HEAD".equalsIgnoreCase(method)) {
        exchange.getResponseHeaders().set("Allow", "GET, HEAD");
        respond(exchange, 405, "text/plain; charset=utf-8", "Method Not Allowed\n");
        return;
      }
      delegate.handle(exchange);
    };
  }

  private static boolean isClientAllowed(
      HttpExchange exchange,
      List<ClientAddressMatcher> allowedClients
  ) {
    if (allowedClients.isEmpty()) {
      return true;
    }
    InetSocketAddress remote = exchange.getRemoteAddress();
    byte[] address = remote != null && remote.getAddress() != null
        ? remote.getAddress().getAddress()
        : null;
    if (address == null) {
      return false;
    }
    for (ClientAddressMatcher matcher : allowedClients) {
      if (matcher.matches(address)) {
        return true;
      }
    }
    return false;
  }

  private static void respond(HttpExchange exchange, int status, String contentType, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", contentType);
    HttpResponseWriter.sendBody(exchange, status, bytes);
  }

  private static final class ReadinessHandler implements HttpHandler {
    private final ProxyConfig config;
    private final CatalogRouter router;
    private final ProxyObservability observability;
    private final BackendProbeRunner probeRunner;
    private final SingleFlightCache<ProbeSnapshot> probeCache;

    private ReadinessHandler(
        ProxyConfig config,
        CatalogRouter router,
        ProxyObservability observability,
        BackendProbeRunner probeRunner) {
      this.config = config;
      this.router = router;
      this.observability = observability;
      this.probeRunner = probeRunner;
      this.probeCache = new SingleFlightCache<>(config.management().readinessCacheMs());
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
      // Backend and Kerberos probes are the expensive part of readiness; they are refreshed at most
      // once per readiness-cache-ms so that frequent scrapes do not fan out network checks. The
      // backend status fields below are read fresh from in-memory runtime state on every request.
      Optional<ProbeSnapshot> probesOpt = probeCache.tryGet(this::refreshProbes, COLD_START_PROBE_WAIT_MS);
      if (probesOpt.isEmpty()) {
        respond(
            exchange,
            503,
            "application/json; charset=utf-8",
            "{\"status\":\"NOT_READY\",\"alive\":true,\"reason\":\"initializing\"}\n");
        return;
      }
      ProbeSnapshot probes = probesOpt.get();
      long probeAgeMs = Math.max(0L, System.currentTimeMillis() - probes.probedAtEpochMs());
      KerberosHealthProbe.KerberosStatus frontDoorKerberos = probes.frontDoor();
      KerberosHealthProbe.KerberosStatus backendKerberos = probes.backend();
      boolean includeDetails = config.management().readyzIncludeDetails();
      List<ProxyRuntimeState.BackendRuntimeStatus> statuses = observability.runtimeState().backendStatuses();
      boolean requireAll = config.management().requireAllCatalogs();
      boolean backendConnectivity = statuses.stream()
          .filter(status -> isCatalogRequiredForReadiness(status.backend(), requireAll))
          .allMatch(ProxyRuntimeState.BackendRuntimeStatus::connected);
      boolean ready = statuses.stream()
          .filter(status -> isCatalogRequiredForReadiness(status.backend(), requireAll))
          .allMatch(status ->
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
          .append("\"frontDoor\":").append(renderKerberos(frontDoorKerberos, includeDetails)).append(',')
          .append("\"backend\":").append(renderKerberos(backendKerberos, includeDetails)).append("},")
          .append("\"backends\":[");
      for (int index = 0; index < statuses.size(); index++) {
        ProxyRuntimeState.BackendRuntimeStatus status = statuses.get(index);
        boolean required = isCatalogRequiredForReadiness(status.backend(), requireAll);
        if (index > 0) {
          body.append(',');
        }
        body.append('{')
            .append("\"backend\":\"").append(escape(status.backend())).append("\",")
            .append("\"requiredForReadiness\":").append(required).append(',')
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
        if (!includeDetails || status.lastError() == null) {
          body.append("null");
        } else {
          body.append('"').append(escape(status.lastError())).append('"');
        }
        body.append('}');
      }
      body.append("]}\n");
      respond(exchange, ready ? 200 : 503, "application/json; charset=utf-8", body.toString());
    }

    private boolean isCatalogRequiredForReadiness(String catalogName, boolean requireAll) {
      if (catalogName.equals(config.defaultCatalog())) {
        return true;
      }
      if (!requireAll) {
        return false;
      }
      CatalogConfig catalogConfig = config.catalogs().get(catalogName);
      return catalogConfig == null || catalogConfig.requiredForReadiness();
    }

    private ProbeSnapshot refreshProbes() {
      // probeRunner is only created when background backend-state polling is disabled; with
      // polling on, readiness reuses the poller's results and only refreshes Kerberos status.
      if (probeRunner != null) {
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
      probeRunner.runRound(
          List.copyOf(router.backends()),
          config.latencyRouting().backendStatePolling().probeTimeoutMs(),
          new BackendProbeRunner.ProbeListener() {
            @Override
            public void succeeded(CatalogBackend backend, long latencyMs) {
              observability.runtimeState().recordBackendProbeSuccess(
                  backend.name(), latencyMs, config.latencyRouting());
            }

            @Override
            public void failed(CatalogBackend backend, Throwable error) {
              observability.runtimeState().recordBackendProbeFailure(
                  backend.name(), error, config.latencyRouting());
            }
          });
    }

    private KerberosHealthProbe.KerberosStatus configKerberosStatus() {
      if (!config.security().kerberosEnabled()) {
        return KerberosHealthProbe.disabled("frontDoor");
      }
      return KerberosHealthProbe.probeLoginUser("frontDoor", config.security().serverPrincipal());
    }

    private KerberosHealthProbe.KerberosStatus backendKerberosStatus() {
      boolean backendKerberosEnabled = config.catalogs().values().stream()
          .anyMatch(catalog ->
              Boolean.parseBoolean(catalog.hiveConf().getOrDefault("hive.metastore.sasl.enabled", "false"))
                  || Boolean.parseBoolean(catalog.hiveConf().getOrDefault("metastore.thrift.sasl.enabled", "false")));
      if (!backendKerberosEnabled) {
        return KerberosHealthProbe.disabled("backend");
      }
      return KerberosHealthProbe.probeBackendLogin("backend", config.security().outboundPrincipal());
    }

  }

  static String escape(String value) {
    return JsonEscapeUtil.escape(value);
  }

  static String renderKerberos(KerberosHealthProbe.KerberosStatus status) {
    return renderKerberos(status, true);
  }

  static String renderKerberos(KerberosHealthProbe.KerberosStatus status, boolean includeDetails) {
    String principal = includeDetails ? status.principal() : null;
    String detail = includeDetails ? status.detail() : sanitizeKerberosDetail(status);
    StringBuilder builder = new StringBuilder(160);
    builder.append('{')
        .append("\"component\":\"").append(escape(status.component())).append("\",")
        .append("\"enabled\":").append(status.enabled()).append(',')
        .append("\"state\":\"").append(status.state()).append("\",")
        .append("\"loggedIn\":").append(status.loggedIn()).append(',')
        .append("\"healthy\":").append(status.healthy()).append(',')
        .append("\"principal\":");
    if (principal == null) {
      builder.append("null");
    } else {
      builder.append('"').append(escape(principal)).append('"');
    }
    builder.append(",\"checkedAtEpochSecond\":").append(status.checkedAtEpochSecond() == null ? "null" : status.checkedAtEpochSecond())
        .append(",\"tgtExpiresAtEpochSecond\":").append(status.tgtExpiresAtEpochSecond() == null ? "null" : status.tgtExpiresAtEpochSecond())
        .append(",\"secondsUntilExpiry\":").append(status.secondsUntilExpiry() == null ? "null" : status.secondsUntilExpiry())
        .append(",\"detail\":");
    if (detail == null) {
      builder.append("null");
    } else {
      builder.append('"').append(escape(detail)).append('"');
    }
    builder.append('}');
    return builder.toString();
  }

  private static String sanitizeKerberosDetail(KerberosHealthProbe.KerberosStatus status) {
    if (!status.enabled() || status.detail() == null) {
      return null;
    }
    return switch (status.state()) {
      case ACTIVE -> "ok";
      case PENDING -> "pending";
      case STALE -> "stale";
      case FAILED -> "unavailable";
      case DISABLED -> null;
    };
  }
}
