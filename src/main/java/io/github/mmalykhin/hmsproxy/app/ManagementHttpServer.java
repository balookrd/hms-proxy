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
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ManagementHttpServer implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ManagementHttpServer.class);

  private final HttpServer server;

  private ManagementHttpServer(HttpServer server) {
    this.server = server;
  }

  public static ManagementHttpServer open(
      ProxyConfig config,
      CatalogRouter router,
      ProxyObservability observability
  ) throws IOException {
    if (!config.management().enabled()) {
      return null;
    }

    HttpServer server = HttpServer.create(
        new InetSocketAddress(config.management().bindHost(), config.management().port()), 0);
    server.createContext("/healthz", exchange -> {
      String body = "{\"status\":\"ok\",\"alive\":true,\"uptimeSeconds\":"
          + (System.currentTimeMillis() / 1000L - observability.runtimeState().startedAtEpochSecond())
          + "}\n";
      respond(exchange, 200, "application/json; charset=utf-8", body);
    });
    server.createContext("/readyz", new ReadinessHandler(config, router, observability));
    server.createContext("/metrics", exchange -> respond(
        exchange,
        200,
        "text/plain; version=0.0.4; charset=utf-8",
        observability.metrics().render()));
    server.setExecutor(null);
    server.start();
    LOG.info("Management HTTP listener started on {}:{}",
        config.management().bindHost(), config.management().port());
    return new ManagementHttpServer(server);
  }

  @Override
  public void close() {
    server.stop(0);
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

    private ReadinessHandler(ProxyConfig config, CatalogRouter router, ProxyObservability observability) {
      this.config = config;
      this.router = router;
      this.observability = observability;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
      boolean backendConnectivity = true;
      for (CatalogBackend backend : router.backends()) {
        try {
          backend.checkConnectivity();
          observability.runtimeState().recordBackendProbeSuccess(backend.name());
        } catch (Throwable error) {
          backendConnectivity = false;
          observability.runtimeState().recordBackendProbeFailure(backend.name(), error);
        }
      }

      KerberosHealthProbe.KerberosStatus frontDoorKerberos = configKerberosStatus();
      KerberosHealthProbe.KerberosStatus backendKerberos = backendKerberosStatus();
      boolean ready = backendConnectivity && frontDoorKerberos.healthy() && backendKerberos.healthy();

      List<ProxyRuntimeState.BackendRuntimeStatus> statuses = observability.runtimeState().backendStatuses();
      StringBuilder body = new StringBuilder(512);
      body.append("{\"status\":\"").append(ready ? "ready" : "degraded").append("\",")
          .append("\"alive\":true,")
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

    private KerberosHealthProbe.KerberosStatus configKerberosStatus() {
      if (!config.security().kerberosEnabled()) {
        return KerberosHealthProbe.disabled("frontDoor");
      }
      return KerberosHealthProbe.probe(
          "frontDoor",
          config.security().serverPrincipal(),
          config.security().keytab());
    }

    private KerberosHealthProbe.KerberosStatus backendKerberosStatus() {
      boolean backendKerberosEnabled = config.catalogs().values().stream()
          .anyMatch(catalog -> Boolean.parseBoolean(catalog.hiveConf().getOrDefault("hive.metastore.sasl.enabled", "false")));
      if (!backendKerberosEnabled) {
        return KerberosHealthProbe.disabled("backend");
      }
      return KerberosHealthProbe.probe(
          "backend",
          config.security().outboundPrincipal(),
          config.security().outboundKeytab());
    }

    private static String escape(String value) {
      return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String renderKerberos(KerberosHealthProbe.KerberosStatus status) {
      StringBuilder builder = new StringBuilder(160);
      builder.append('{')
          .append("\"component\":\"").append(escape(status.component())).append("\",")
          .append("\"enabled\":").append(status.enabled()).append(',')
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
}
