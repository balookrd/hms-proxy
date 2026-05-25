package io.github.mmalykhin.hmsproxy.restcatalog;

import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.restcatalog.RestCatalogConfig;
import java.io.IOException;
import java.io.OutputStream;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RestCatalogServer implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(RestCatalogServer.class);
  private static final String JSON_CONTENT_TYPE = "application/json; charset=utf-8";
  private static final int STOP_TIMEOUT_SECONDS = 5;

  private final HttpServer server;
  private final ThreadPoolExecutor executor;

  private RestCatalogServer(HttpServer server, ThreadPoolExecutor executor) {
    this.server = server;
    this.executor = executor;
  }

  public static RestCatalogServer open(ProxyConfig config) throws IOException {
    return open(config, null);
  }

  public static RestCatalogServer open(ProxyConfig config, IcebergRestService service) throws IOException {
    RestCatalogConfig restConfig = config.restCatalog();
    if (!restConfig.enabled()) {
      return null;
    }

    HttpServer server;
    try {
      server = HttpServer.create(
          new InetSocketAddress(restConfig.bindHost(), restConfig.port()), 0);
    } catch (BindException e) {
      LOG.error("Failed to bind Iceberg REST catalog listener on {}:{} - {}",
          restConfig.bindHost(), restConfig.port(), e.getMessage());
      throw e;
    }

    SpnegoAuthenticator authenticator = null;
    if (restConfig.kerberosEnabled()) {
      try {
        UserGroupInformation ugi = RestKerberosBootstrap.login(restConfig);
        authenticator = new SpnegoAuthenticator(ugi, restConfig.kerberosPrincipal());
        LOG.info("Iceberg REST SPNEGO enabled for principal {}", restConfig.kerberosPrincipal());
      } catch (Exception e) {
        server.stop(0);
        throw new IOException(
            "Failed to initialise SPNEGO authenticator for Iceberg REST listener: " + e.getMessage(), e);
      }
    }

    HttpContext v1Context;
    if (service != null) {
      v1Context = server.createContext("/v1/", new IcebergHttpHandler(service));
    } else {
      v1Context = server.createContext("/v1/config", new ConfigHandler());
    }
    if (authenticator != null) {
      v1Context.setAuthenticator(authenticator);
    }
    server.createContext("/", new NotFoundHandler());

    ThreadPoolExecutor executor = new ThreadPoolExecutor(
        restConfig.minWorkerThreads(),
        restConfig.maxWorkerThreads(),
        60L,
        TimeUnit.SECONDS,
        new SynchronousQueue<>(),
        namedThreadFactory("hms-proxy-rest"),
        new ThreadPoolExecutor.CallerRunsPolicy());
    server.setExecutor(executor);
    server.start();
    LOG.info("Iceberg REST catalog listener started on {}:{} (threads: {}..{})",
        restConfig.bindHost(), restConfig.port(),
        restConfig.minWorkerThreads(), restConfig.maxWorkerThreads());
    return new RestCatalogServer(server, executor);
  }

  public int boundPort() {
    return server.getAddress().getPort();
  }

  @Override
  public void close() {
    server.stop(0);
    executor.shutdown();
    try {
      if (!executor.awaitTermination(STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        executor.shutdownNow();
        LOG.warn("Iceberg REST catalog executor did not terminate within {}s after shutdown",
            STOP_TIMEOUT_SECONDS);
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  private static ThreadFactory namedThreadFactory(String prefix) {
    AtomicLong counter = new AtomicLong();
    return runnable -> {
      Thread thread = new Thread(runnable);
      thread.setName(prefix + "-" + counter.incrementAndGet());
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

  private static final class ConfigHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      String method = exchange.getRequestMethod();
      if (!"GET".equalsIgnoreCase(method) && !"HEAD".equalsIgnoreCase(method)) {
        exchange.getResponseHeaders().set("Allow", "GET, HEAD");
        respond(exchange, 405, JSON_CONTENT_TYPE,
            "{\"error\":{\"message\":\"Method not allowed\",\"type\":\"BadRequestException\",\"code\":405}}");
        return;
      }
      respond(exchange, 200, JSON_CONTENT_TYPE, "{\"defaults\":{},\"overrides\":{}}");
    }
  }

  private static final class NotFoundHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      respond(exchange, 404, JSON_CONTENT_TYPE,
          "{\"error\":{\"message\":\"Not implemented\",\"type\":\"NotImplementedException\",\"code\":404}}");
    }
  }
}
