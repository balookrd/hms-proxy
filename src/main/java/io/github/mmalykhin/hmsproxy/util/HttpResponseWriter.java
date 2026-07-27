package io.github.mmalykhin.hmsproxy.util;

import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Writes a JDK {@code HttpServer} response body for every non-HEAD request. RFC 9110 forbids a
 * body on a HEAD response; the JDK {@code HttpServer} enforces this by throwing
 * {@code IOException: stream closed} from the body write, which surfaces as a HEAD probe failure
 * (management listener health checks) or as log noise on HEAD error paths (Iceberg REST
 * exists-checks). For HEAD, send only the headers with no content length and close the body
 * unwritten. Shared by {@code ManagementHttpServer} and {@code IcebergHttpHandler} so the guard
 * exists exactly once.
 */
public final class HttpResponseWriter {
  private HttpResponseWriter() {
  }

  public static void sendBody(HttpExchange exchange, int status, byte[] bytes) throws IOException {
    if ("HEAD".equalsIgnoreCase(exchange.getRequestMethod())) {
      exchange.sendResponseHeaders(status, -1);
      exchange.getResponseBody().close();
      return;
    }
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream output = exchange.getResponseBody()) {
      output.write(bytes);
    }
  }
}
