package io.github.mmalykhin.hmsproxy.restcatalog;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpPrincipal;
import io.github.mmalykhin.hmsproxy.config.restcatalog.RestCatalogConfig;
import io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics;
import io.github.mmalykhin.hmsproxy.security.ClientRequestContext;
import io.github.mmalykhin.hmsproxy.util.HttpResponseWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.rest.HTTPRequest.HTTPMethod;
import org.apache.iceberg.rest.RESTCatalogAdapter;
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
  private static final String PAGE_SIZE_PARAM = "pageSize";
  private static final String PAGE_TOKEN_PARAM = "pageToken";
  private static final String UNKNOWN_PREFIX_LABEL = "unknown";
  private static final String ROUTE_CONFIG = "config";
  private static final String ROUTE_UNKNOWN_PREFIX = "unknown_prefix";
  private static final String ROUTE_UNKNOWN_ROUTE = "unknown_route";
  private static final String ROUTE_BAD_REQUEST = "bad_request";

  private final IcebergRestServices services;
  private final PrometheusMetrics metrics;
  private final long maxRequestBodyBytes;

  IcebergHttpHandler(IcebergRestServices services, PrometheusMetrics metrics) {
    this(services, metrics, RestCatalogConfig.DEFAULT_MAX_REQUEST_BODY_BYTES);
  }

  IcebergHttpHandler(IcebergRestServices services, PrometheusMetrics metrics, long maxRequestBodyBytes) {
    this.services = services;
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    this.maxRequestBodyBytes = maxRequestBodyBytes;
  }

  /** The body is larger than {@code rest-catalog.max-request-body-bytes}; answered with 413. */
  static final class RequestBodyTooLargeException extends IOException {
    RequestBodyTooLargeException(long limit) {
      super("Request body exceeds rest-catalog.max-request-body-bytes (" + limit + " bytes)");
    }
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
        && exchange.getRemoteAddress().getAddress() != null
        ? exchange.getRemoteAddress().getAddress().getHostAddress()
        : null;
    HttpPrincipal principal = exchange.getPrincipal();
    String remoteUser = principal != null ? principal.getUsername() : null;
    String previousAddress = ClientRequestContext.setRemoteAddress(remoteAddress);
    String previousUser = ClientRequestContext.setRemoteUser(remoteUser);
    RequestOutcome outcome = new RequestOutcome();
    long startNanos = System.nanoTime();
    GuardedFileIO.beginRequest();
    try {
      doHandle(exchange, outcome);
    } finally {
      GuardedFileIO.endRequest();
      // Restore the thread-local request context before recording the metric, not after: this
      // handler instance is shared across the HTTP executor's threads, so if
      // recordRestRequest ever threw, an ordering with the restore last would leak
      // remoteAddress/remoteUser into whatever request that pool thread serves next.
      try {
        ClientRequestContext.restoreRemoteAddress(previousAddress);
      } finally {
        ClientRequestContext.restoreRemoteUser(previousUser);
      }
      double durationSeconds = (System.nanoTime() - startNanos) / 1_000_000_000.0;
      metrics.recordRestRequest(outcome.prefix, outcome.route, outcome.status, durationSeconds);
    }
  }

  private void doHandle(HttpExchange exchange, RequestOutcome outcome) throws IOException {
    try {
      String rawPath = exchange.getRequestURI().getRawPath();
      if (rawPath == null || (!rawPath.startsWith(V1_PREFIX) && !rawPath.equals("/v1"))) {
        outcome.route = ROUTE_UNKNOWN_ROUTE;
        writeError(exchange, outcome, 404, "NotImplementedException", "Path not handled by Iceberg REST endpoint");
        return;
      }
      HTTPMethod method;
      try {
        method = HTTPMethod.valueOf(exchange.getRequestMethod().toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException e) {
        outcome.route = ROUTE_BAD_REQUEST;
        writeError(exchange, outcome, 405, "BadRequestException",
            "Method not allowed: " + exchange.getRequestMethod());
        return;
      }

      Map<String, String> queryParams;
      String firstSegment;
      String rawRemainder;
      try {
        queryParams = parseQueryString(exchange.getRequestURI().getRawQuery());
        String trimmed = rawPath.equals("/v1") ? "" : rawPath.substring(V1_PREFIX.length());
        int slash = trimmed.indexOf('/');
        String rawFirstSegment = slash < 0 ? trimmed : trimmed.substring(0, slash);
        rawRemainder = slash < 0 ? "" : trimmed.substring(slash + 1);
        firstSegment = decodePathSegment(rawFirstSegment);
      } catch (IllegalArgumentException e) {
        outcome.route = ROUTE_BAD_REQUEST;
        writeError(exchange, outcome, 400, "BadRequestException", "Malformed request URI");
        return;
      }

      if (CONFIG_SEGMENT.equalsIgnoreCase(firstSegment) && rawRemainder.isEmpty() && method == HTTPMethod.GET) {
        handleConfig(exchange, queryParams, outcome);
        return;
      }

      IcebergRestService service = services.serviceFor(firstSegment);
      if (service == null) {
        outcome.route = ROUTE_UNKNOWN_PREFIX;
        writeError(exchange, outcome, 404, "NoSuchCatalogException",
            "Unknown catalog prefix");
        return;
      }
      outcome.prefix = service.catalogName();
      if (CONFIG_SEGMENT.equalsIgnoreCase(rawRemainder) && method == HTTPMethod.GET) {
        // Same writer path as unprefixed /v1/config?warehouse=<catalog>, but the catalog
        // comes from the path segment instead of a query param.
        outcome.route = ROUTE_CONFIG;
        ConfigResponse cfg = service.loadConfig();
        writeJson(exchange, outcome, 200, IcebergRestMapper.mapper().writeValueAsString(cfg));
        return;
      }
      String relativePath = rawRemainder.isEmpty() ? "v1" : "v1/" + rawRemainder;

      Pair<Route, Map<String, String>> routeAndVars = Route.from(method, relativePath);
      if (routeAndVars == null || !isSupportedProxyRoute(routeAndVars.first())) {
        outcome.route = ROUTE_UNKNOWN_ROUTE;
        writeError(exchange, outcome, 404, "NotImplementedException",
            "Route not supported: " + method);
        return;
      }
      Route route = routeAndVars.first();
      outcome.route = route.name().toLowerCase(Locale.ROOT);
      Object body;
      try {
        body = readBody(exchange, route);
      } catch (RequestBodyTooLargeException e) {
        writeError(exchange, outcome, 413, "BadRequestException", e.getMessage());
        return;
      } catch (JsonProcessingException e) {
        writeError(exchange, outcome, 400, "BadRequestException", "Malformed JSON request body");
        return;
      } catch (IOException | RuntimeException e) {
        writeError(exchange, outcome, 400, "BadRequestException",
            "Malformed request body: " + e.getMessage());
        return;
      }
      Class<? extends RESTResponse> responseType = route.responseClass();

      Map<String, String> vars = new LinkedHashMap<>(queryParams);
      vars.putAll(routeAndVars.second());
      if (vars.containsKey(PAGE_SIZE_PARAM) && !vars.containsKey(PAGE_TOKEN_PARAM)) {
        // Iceberg 1.9.2's CatalogHandlers.paginate() parses pageToken with Integer.parseInt and
        // throws NumberFormatException when it is absent, which is exactly the first-page request
        // (no prior token yet). An empty token means "start from the beginning", same as a client
        // explicitly sending pageToken=.
        vars.put(PAGE_TOKEN_PARAM, "");
      }

      String writeRefusal;
      try {
        writeRefusal = service.writeGate().check(route, vars, body);
      } catch (IllegalArgumentException e) {
        writeError(exchange, outcome, 400, "BadRequestException", e.getMessage());
        return;
      }
      if (writeRefusal != null) {
        writeError(exchange, outcome, 403, "ForbiddenException", writeRefusal);
        return;
      }

      RESTResponse response;
      Class<? extends RESTResponse> effectiveResponseType =
          responseType == null ? RESTResponse.class : responseType;
      try {
        response = service.dispatch(route, vars, body, effectiveResponseType);
      } catch (RuntimeException e) {
        handleDispatchException(exchange, outcome, rawPath, e);
        return;
      }

      if (responseType == null || response == null) {
        outcome.status = 204;
        exchange.sendResponseHeaders(204, -1);
        exchange.getResponseBody().close();
        return;
      }
      writeJson(exchange, outcome, 200, IcebergRestMapper.mapper().writeValueAsString(response));
    } catch (Throwable t) {
      // Catches Throwable, not just Exception: a backend classpath mismatch (e.g. a
      // NoSuchMethodError from a stale transitive Hadoop jar) is an Error, and letting
      // it escape here would leave the JDK HTTP server abandoning the exchange with no
      // response at all - the client hangs instead of getting a 5xx.
      LOG.warn("Unhandled error serving {} {}",
          exchange.getRequestMethod(), exchange.getRequestURI().getRawPath(), t);
      writeError(exchange, outcome, 500, t.getClass().getSimpleName(), "Internal server error");
    }
  }

  private void handleDispatchException(
      HttpExchange exchange,
      RequestOutcome outcome,
      String rawPath,
      RuntimeException e
  ) throws IOException {
    if (e instanceof NumberFormatException) {
      writeError(exchange, outcome, 400, "BadRequestException", "Invalid numeric query parameter");
      return;
    }
    if (containsJsonProcessingException(e)) {
      LOG.warn("Metadata JSON processing failed serving {} {}",
          exchange.getRequestMethod(), rawPath, e);
      writeError(exchange, outcome, 500, "InternalServerError", "Failed to read table metadata");
      return;
    }
    ErrorResponse.Builder builder = ErrorResponse.builder();
    RESTCatalogAdapter.configureResponseFromException(e, builder);
    // Upstream's helper attaches the server stack trace; it is fine in a test
    // harness but this listener answers real clients, so keep the mapped code,
    // type and drop the trace.
    builder.withStackTrace(List.of());
    ErrorResponse error = builder.build();
    if (error.code() >= 500) {
      LOG.warn("Error serving {} {}", exchange.getRequestMethod(), rawPath, e);
      if (e instanceof CommitStateUnknownException) {
        writeError(
            exchange,
            outcome,
            500,
            CommitStateUnknownException.class.getSimpleName(),
            "Transaction partially committed; final state unknown");
      } else {
        writeError(exchange, outcome, 500, e.getClass().getSimpleName(), "Internal server error");
      }
      return;
    }
    writeErrorResponse(exchange, outcome, error);
  }

  private static boolean isSupportedProxyRoute(Route route) {
    return route != Route.TOKENS
        && route != Route.SEPARATE_AUTH_TOKENS_URI
        && route != Route.CONFIG;
  }

  private static boolean containsJsonProcessingException(Throwable error) {
    Throwable current = error;
    for (int depth = 0; depth < 16 && current != null; depth++) {
      if (current instanceof JsonProcessingException) {
        return true;
      }
      current = current.getCause();
    }
    return false;
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

  private Object readBody(HttpExchange exchange, Route route) throws IOException {
    Class<? extends RESTRequest> requestClass = route.requestClass();
    if (requestClass == null) {
      drainBounded(exchange.getRequestBody(), maxRequestBodyBytes);
      return null;
    }
    String declaredLength = exchange.getRequestHeaders().getFirst("Content-Length");
    if (declaredLength != null) {
      try {
        if (Long.parseLong(declaredLength.trim()) > maxRequestBodyBytes) {
          throw new RequestBodyTooLargeException(maxRequestBodyBytes);
        }
      } catch (NumberFormatException ignored) {
        // A malformed length is the HTTP server's problem; the counting stream below still caps it.
      }
    }
    Object parsed;
    try (InputStream input = new BoundedInputStream(exchange.getRequestBody(), maxRequestBodyBytes)) {
      parsed = IcebergRestMapper.mapper().readValue(input, requestClass);
    } catch (JsonProcessingException e) {
      // Jackson wraps an exception thrown by the stream; surface the size refusal, not a parse error.
      if (e.getCause() instanceof RequestBodyTooLargeException tooLarge) {
        throw tooLarge;
      }
      throw e;
    }
    if (parsed == null) {
      throw new IllegalArgumentException("Request body must not be empty");
    }
    if (parsed instanceof RESTRequest restRequest) {
      restRequest.validate();
    }
    return parsed;
  }

  /** Fails as soon as more than {@code limit} bytes are read, so a chunked body is capped too. */
  private static final class BoundedInputStream extends java.io.FilterInputStream {
    private final long limit;
    private long read;

    BoundedInputStream(InputStream in, long limit) {
      super(in);
      this.limit = limit;
    }

    @Override
    public int read() throws IOException {
      int b = super.read();
      if (b >= 0) {
        count(1);
      }
      return b;
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
      int n = super.read(buffer, offset, length);
      if (n > 0) {
        count(n);
      }
      return n;
    }

    private void count(long n) throws RequestBodyTooLargeException {
      read += n;
      if (read > limit) {
        throw new RequestBodyTooLargeException(limit);
      }
    }
  }

  private static void drainBounded(InputStream input, long limit) throws IOException {
    try (InputStream bounded = new BoundedInputStream(input, limit)) {
      bounded.transferTo(OutputStream.nullOutputStream());
    }
  }

  private static String decodePathSegment(String raw) {
    if (raw == null || raw.isEmpty()) {
      return "";
    }
    if (raw.indexOf('%') < 0) {
      if (raw.indexOf('\0') >= 0) {
        throw new IllegalArgumentException("Path segment contains NUL byte");
      }
      return raw;
    }
    byte[] buf = new byte[raw.length()];
    int byteLen = 0;
    for (int i = 0; i < raw.length(); i++) {
      char c = raw.charAt(i);
      if (c == '%') {
        if (i + 2 >= raw.length()) {
          throw new IllegalArgumentException("Incomplete percent-encoding in path segment");
        }
        int hi = Character.digit(raw.charAt(i + 1), 16);
        int lo = Character.digit(raw.charAt(i + 2), 16);
        if (hi < 0 || lo < 0) {
          throw new IllegalArgumentException("Invalid percent-encoding in path segment");
        }
        buf[byteLen++] = (byte) ((hi << 4) | lo);
        i += 2;
      } else {
        byte[] utf8 = String.valueOf(c).getBytes(StandardCharsets.UTF_8);
        if (byteLen + utf8.length > buf.length) {
          buf = Arrays.copyOf(buf, Math.max(buf.length * 2, byteLen + utf8.length));
        }
        System.arraycopy(utf8, 0, buf, byteLen, utf8.length);
        byteLen += utf8.length;
      }
    }
    String decoded;
    try {
      decoded = StandardCharsets.UTF_8.newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .decode(ByteBuffer.wrap(buf, 0, byteLen))
          .toString();
    } catch (CharacterCodingException e) {
      throw new IllegalArgumentException("Invalid UTF-8 percent-encoding in path segment", e);
    }
    if (decoded.indexOf('/') >= 0 || decoded.indexOf('\0') >= 0) {
      throw new IllegalArgumentException("Path segment contains forbidden encoded character");
    }
    return decoded;
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
    HttpResponseWriter.sendBody(exchange, status, bytes);
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
    HttpResponseWriter.sendBody(exchange, status, bytes);
  }
}
