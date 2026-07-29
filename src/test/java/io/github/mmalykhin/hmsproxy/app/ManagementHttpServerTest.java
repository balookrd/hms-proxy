package io.github.mmalykhin.hmsproxy.app;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.management.ManagementConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityMode;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreMode;
import io.github.mmalykhin.hmsproxy.observability.ProxyObservability;
import io.github.mmalykhin.hmsproxy.routing.CatalogRouter;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import org.junit.Assert;
import org.junit.Test;

public class ManagementHttpServerTest {
  @Test
  public void servesEveryEndpointFromADedicatedThreadPool() throws Exception {
    int port = freePort();
    ProxyConfig config = config(port, ManagementConfig.DEFAULT_READINESS_CACHE_MS);
    try (CatalogRouter router = CatalogRouter.open(config);
         ManagementHttpServer server = ManagementHttpServer.open(
             config, router, new ProxyObservability(config))) {
      Assert.assertNotNull(server);

      // The built-in default (setExecutor(null)) serves every context from one dispatcher thread,
      // so a blocked /readyz would also stall liveness and metrics.
      ThreadPoolExecutor executor = (ThreadPoolExecutor) server.httpExecutor();
      Assert.assertEquals(ManagementConfig.DEFAULT_THREADS, executor.getCorePoolSize());
      Assert.assertTrue("liveness and metrics need a thread while /readyz probes",
          executor.getCorePoolSize() > 1);

      Assert.assertTrue(get(port, "/healthz").contains("\"alive\":true"));
      Assert.assertTrue(get(port, "/metrics").contains("hms_proxy_requests_total"));

      String readyz = get(port, "/readyz");
      Assert.assertTrue(readyz, readyz.contains("\"status\":\"ready\""));
      Assert.assertTrue(readyz, readyz.contains("\"probeAgeMs\":"));
    }
  }

  @Test
  public void headRequestsMatchGetStatusOnEveryEndpoint() throws Exception {
    int port = freePort();
    ProxyConfig config = config(port, ManagementConfig.DEFAULT_READINESS_CACHE_MS);
    try (CatalogRouter router = CatalogRouter.open(config);
         ManagementHttpServer server = ManagementHttpServer.open(
             config, router, new ProxyObservability(config))) {
      Assert.assertNotNull(server);

      // This is an end-to-end sanity check that a health checker's actual HEAD request gets a
      // response with no exception, not the regression guard for the HEAD "stream closed" bug:
      // the client-visible status here is identical whether or not the shared
      // HttpResponseWriter's HEAD guard exists, so this test cannot fail if that guard is
      // removed. HttpResponseWriterTest asserts the (status, contentLength) pair and body bytes
      // the helper actually sends and is the test that fails in that case.
      for (String path : new String[] {"/healthz", "/metrics", "/readyz"}) {
        int getStatus = statusOf(port, path, "GET");
        int headStatus = statusOf(port, path, "HEAD");
        Assert.assertEquals("HEAD status must match GET status for " + path, getStatus, headStatus);
      }
    }
  }

  @Test
  public void readinessProbesAreReusedBetweenScrapes() throws Exception {
    int port = freePort();
    ProxyConfig config = config(port, 60_000L);
    try (CatalogRouter router = CatalogRouter.open(config);
         ManagementHttpServer server = ManagementHttpServer.open(
             config, router, new ProxyObservability(config))) {
      Assert.assertNotNull(server);

      long firstAge = probeAge(get(port, "/readyz"));
      Thread.sleep(60L);
      long secondAge = probeAge(get(port, "/readyz"));

      Assert.assertTrue("second scrape must reuse the first probe result, got probeAgeMs="
          + secondAge, secondAge >= firstAge + 50L);
    }
  }

  @Test
  public void zeroCacheProbesOnEveryScrape() throws Exception {
    int port = freePort();
    ProxyConfig config = config(port, 0L);
    try (CatalogRouter router = CatalogRouter.open(config);
         ManagementHttpServer server = ManagementHttpServer.open(
             config, router, new ProxyObservability(config))) {
      Assert.assertNotNull(server);

      Assert.assertTrue(probeAge(get(port, "/readyz")) < 20L);
      Thread.sleep(60L);
      long secondAge = probeAge(get(port, "/readyz"));
      Assert.assertTrue("cache disabled must re-probe, got probeAgeMs=" + secondAge, secondAge < 20L);
    }
  }

  private static long probeAge(String readyz) {
    String field = "\"probeAgeMs\":";
    int start = readyz.indexOf(field);
    Assert.assertTrue(readyz, start >= 0);
    int from = start + field.length();
    return Long.parseLong(readyz.substring(from, readyz.indexOf(',', from)));
  }

  private static int statusOf(int port, String path, String method) throws Exception {
    HttpURLConnection connection =
        (HttpURLConnection) URI.create("http://127.0.0.1:" + port + path).toURL().openConnection();
    connection.setRequestMethod(method);
    connection.setConnectTimeout(5_000);
    connection.setReadTimeout(10_000);
    try {
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  private static String get(int port, String path) throws Exception {
    HttpURLConnection connection =
        (HttpURLConnection) URI.create("http://127.0.0.1:" + port + path).toURL().openConnection();
    connection.setConnectTimeout(5_000);
    connection.setReadTimeout(10_000);
    try (InputStream input = connection.getInputStream()) {
      return new String(input.readAllBytes(), StandardCharsets.UTF_8);
    } finally {
      connection.disconnect();
    }
  }

  private static int freePort() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private static ProxyConfig config(int managementPort, long readinessCacheMs) {
    return ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of())
        .management(new ManagementConfig(true, "127.0.0.1", managementPort,
            ManagementConfig.DEFAULT_THREADS, readinessCacheMs))
        .syntheticReadLockStore(
            new SyntheticReadLockStoreConfig(SyntheticReadLockStoreMode.IN_MEMORY, null))
        .build();
  }
}
