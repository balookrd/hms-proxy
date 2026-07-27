package io.github.mmalykhin.hmsproxy.restcatalog;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpPrincipal;
import io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics;
import io.github.mmalykhin.hmsproxy.security.ClientRequestContext;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.rest.RESTCatalogAdapter.HTTPMethod;
import org.apache.iceberg.rest.RESTCatalogAdapter.Route;
import org.apache.iceberg.rest.RESTRequest;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps JDK HttpExchange requests to {@link IcebergRestService} dispatches.
 * URL form: /v1/{prefix}/...  The {prefix} segment selects the target catalog
 * via {@link IcebergRestServices#serviceFor(String)}; an unknown prefix returns 404.
 */
final class IcebergHttpHandler implements HttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergHttpHandler.class);
  private static final String JSON_CONTENT_TYPE = "application/json; charset=utf-8";
  private static final String V1_PREFIX = "/v1/";
  private static final String CONFIG_SEGMENT = "config";
  private static final String WAREHOUSE_PARAM = "warehouse";
  private static final String UNKNOWN_PREFIX_LABEL = "unknown";
  private static final String ROUTE_CONFIG = "config";
  private static final String ROUTE_UNKNOWN_PREFIX = "unknown_prefix";
  private static final String ROUTE_UNKNOWN_ROUTE = "unknown_route";
  private static final String ROUTE_BAD_REQUEST = "bad_request";

  private final IcebergRestServices services;
  private final PrometheusMetrics metrics;

  IcebergHttpHandler(IcebergRestServices services, PrometheusMetrics metrics) {
    this.services = services;
    this.metrics = Objects.requireNonNull(metrics, "metrics");
  }

  /**
   * Per-request mutable state threaded through {@link #doHandle} and the write helpers so the
   * resolved prefix/route/status can be recorded once in {@link #handle}'s finally block. The
   * handler instance itself is shared across the HTTP executor's threads, so this state must
   * never live in handler fields.
   */
  private static final class RequestOutcome {
    private String prefix = UNKNOWN_PREFIX_LABEL;
    private String route = ROUTE_UNKNOWN_ROUTE;
    private int status;
  }

  @Override
  public void handle(HttpExchange exchange) throws IOException {
    String remoteAddress = exchange.getRemoteAddress() != null
        ? exchange.getRemoteAddress().getAddress().getHostAddress()
        : null;
    HttpPrincipal principal = exchange.getPrincipal();
    String remoteUser = principal != null ? principal.getUsername() : null;
    String previousAddress = ClientRequestContext.setRemoteAddress(remoteAddress);
    String previousUser = ClientRequestContext.setRemoteUser(remoteUser);
    RequestOutcome outcome = new RequestOutcome();
    long startNanos = System.nanoTime();
    try {
      doHandle(exchange, outcome);
    } finally {
      double durationSeconds = (System.nanoTime() - startNanos) / 1_000_000_000.0;
      metrics.recordRestRequest(outcome.prefix, outcome.route, outcome.status, durationSeconds);
      ClientRequestContext.restoreRemoteAddress(previousAddress);
      ClientRequestContext.restoreRemoteUser(previousUser);
    }
  }

  private void doHandle(HttpExchange exchange, RequestOutcome outcome) throws IOException {
    try {
      String rawPath = exchange.getRequestURI().getPath();
      if (!rawPath.startsWith(V1_PREFIX) && !rawPath.equals("/v1")) {
        outcome.route = ROUTE_UNKNOWN_ROUTE;
        writeError(exchange, outcome, 404, "NotImplementedException", "Path not handled by Iceberg REST endpoint");
        return;
      }
      HTTPMethod method;
      try {
        method = HTTPMethod.valueOf(exchange.getRequestMethod().toUpperCase());
      } catch (IllegalArgumentException e) {
        outcome.route = ROUTE_BAD_REQUEST;
        writeError(exchange, outcome, 405, "BadRequestException",
            "Method not allowed: " + exchange.getRequestMethod());
        return;
      }

      Map<String, String> queryParams = parseQueryString(exchange.getRequestURI().getRawQuery());
      String trimmed = rawPath.equals("/v1") ? "" : rawPath.substring(V1_PREFIX.length());
      int slash = trimmed.indexOf('/');
      String firstSegment = slash < 0 ? trimmed : trimmed.substring(0, slash);
      String remainder = slash < 0 ? "" : trimmed.substring(slash + 1);

      if (CONFIG_SEGMENT.equals(firstSegment) && remainder.isEmpty()) {
        handleConfig(exchange, queryParams, outcome);
        return;
      }

      IcebergRestService service = services.serviceFor(firstSegment);
      if (service == null) {
        outcome.route = ROUTE_UNKNOWN_PREFIX;
        writeError(exchange, outcome, 404, "NoSuchCatalogException",
            "Unknown catalog prefix in URL: " + rawPath);
        return;
      }
      outcome.prefix = service.catalogName();
      String relativePath = remainder.isEmpty() ? "v1" : "v1/" + remainder;

      Pair<Route, Map<String, String>> routeAndVars = Route.from(method, relativePath);
      if (routeAndVars == null) {
        outcome.route = ROUTE_UNKNOWN_ROUTE;
        writeError(exchange, outcome, 404, "NotImplementedException",
            "Route not supported: " + method + " " + relativePath);
        return;
      }
      Route route = routeAndVars.first();
      outcome.route = route.name().toLowerCase(Locale.ROOT);
      Object body = readBody(exchange, route);
      Class<? extends RESTResponse> responseType = route.responseClass();

      ErrorResponse[] capturedError = new ErrorResponse[1];
      RESTResponse response;
      Class<? extends RESTResponse> effectiveResponseType =
          responseType == null ? RESTResponse.class : responseType;
      try {
        response = dispatchInternal(service, method, relativePath, queryParams, body,
            effectiveResponseType, err -> capturedError[0] = err);
      } catch (RESTException e) {
        // RESTCatalogAdapter always rethrows after invoking the error handler,
        // so an error response is already captured when we get here.
        if (capturedError[0] != null) {
          writeErrorResponse(exchange, outcome, capturedError[0]);
        } else {
          writeError(exchange, outcome, 500, e.getClass().getSimpleName(), e.getMessage());
        }
        return;
      }

      if (capturedError[0] != null) {
        writeErrorResponse(exchange, outcome, capturedError[0]);
        return;
      }

      if (responseType == null || response == null) {
        outcome.status = 204;
        exchange.sendResponseHeaders(204, -1);
        exchange.getResponseBody().close();
        return;
      }
      writeJson(exchange, outcome, 200, IcebergRestMapper.mapper().writeValueAsString(response));
    } catch (Exception e) {
      LOG.warn("Unhandled error serving {} {}",
          exchange.getRequestMethod(), exchange.getRequestURI(), e);
      writeError(exchange, outcome, 500, e.getClass().getSimpleName(),
          e.getMessage() == null ? "internal error" : e.getMessage());
    }
  }

  private void handleConfig(HttpExchange exchange, Map<String, String> queryParams, RequestOutcome outcome)
      throws IOException {
    String warehouse = queryParams.get(WAREHOUSE_PARAM);
    IcebergRestService service = services.byWarehouse(warehouse);
    if (service == null) {
      outcome.route = ROUTE_BAD_REQUEST;
      writeError(exchange, outcome, 400, "BadRequestException", "Unknown warehouse: " + warehouse);
      return;
    }
    outcome.prefix = service.catalogName();
    outcome.route = ROUTE_CONFIG;
    ConfigResponse cfg = service.loadConfig();
    writeJson(exchange, outcome, 200, IcebergRestMapper.mapper().writeValueAsString(cfg));
  }

  private <T extends RESTResponse> T dispatchInternal(
      IcebergRestService service,
      HTTPMethod method,
      String relativePath,
      Map<String, String> queryParams,
      Object body,
      Class<T> responseType,
      java.util.function.Consumer<ErrorResponse> errorHandler) {
    return service.dispatch(method, relativePath, queryParams, body, responseType,
        java.util.Map.of(), errorHandler);
  }

  private Object readBody(HttpExchange exchange, Route route) throws IOException {
    Class<? extends RESTRequest> requestClass = route.requestClass();
    if (requestClass == null) {
      drain(exchange.getRequestBody());
      return null;
    }
    try (InputStream input = exchange.getRequestBody()) {
      return IcebergRestMapper.mapper().readValue(input, requestClass);
    }
  }

  private static void drain(InputStream input) throws IOException {
    try (input) {
      input.transferTo(OutputStream.nullOutputStream());
    }
  }

  private static Map<String, String> parseQueryString(String rawQuery) {
    if (rawQuery == null || rawQuery.isEmpty()) {
      return Map.of();
    }
    Map<String, String> result = new LinkedHashMap<>();
    for (String pair : rawQuery.split("&")) {
      int eq = pair.indexOf('=');
      String key = eq < 0 ? pair : pair.substring(0, eq);
      String value = eq < 0 ? "" : pair.substring(eq + 1);
      result.put(
          URLDecoder.decode(key, StandardCharsets.UTF_8),
          URLDecoder.decode(value, StandardCharsets.UTF_8));
    }
    return result;
  }

  private static void writeJson(HttpExchange exchange, RequestOutcome outcome, int status, String body)
      throws IOException {
    outcome.status = status;
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", JSON_CONTENT_TYPE);
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }

  private static void writeError(
      HttpExchange exchange, RequestOutcome outcome, int status, String type, String message) throws IOException {
    ErrorResponse error = ErrorResponse.builder()
        .responseCode(status)
        .withType(type)
        .withMessage(message == null ? "" : message)
        .withStackTrace(Arrays.asList())
        .build();
    writeErrorResponse(exchange, outcome, error);
  }

  private static void writeErrorResponse(HttpExchange exchange, RequestOutcome outcome, ErrorResponse error)
      throws IOException {
    int status = error.code() > 0 ? error.code() : 500;
    outcome.status = status;
    String body = IcebergRestMapper.mapper().writeValueAsString(error);
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", JSON_CONTENT_TYPE);
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }

  private static int statusForException(RESTException e) {
    String name = e.getClass().getSimpleName();
    if (name.contains("NoSuch")) return 404;
    if (name.contains("AlreadyExists")) return 409;
    if (name.contains("NotAuthorized")) return 401;
    if (name.contains("Forbidden")) return 403;
    if (name.contains("Validation")) return 400;
    return 500;
  }
}
