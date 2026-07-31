package io.github.mmalykhin.hmsproxy.restcatalog;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.restcatalog.RestCatalogConfig;
import io.github.mmalykhin.hmsproxy.config.restcatalog.RestCatalogPurgeMode;
import io.github.mmalykhin.hmsproxy.config.routing.BackendConfig;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;
import io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class RestCatalogServerTest {
  private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(5);

  @Test
  public void returnsNullWhenDisabled() throws Exception {
    ProxyConfig config = buildConfig(new RestCatalogConfig(
        false, "127.0.0.1", 0, 1, 4, null, null, RestCatalogPurgeMode.ALLOW, List.of(), true));
    RestCatalogServer server = RestCatalogServer.open(config, null, new PrometheusMetrics());
    Assert.assertNull(server);
  }

  @Test
  public void servesEmptyIcebergConfigOnGet() throws Exception {
    ProxyConfig config = buildConfig(new RestCatalogConfig(
        true, "127.0.0.1", 0, 1, 4, null, null, RestCatalogPurgeMode.ALLOW, List.of(), true));
    try (RestCatalogServer server = RestCatalogServer.open(config, null, new PrometheusMetrics())) {
      Assert.assertNotNull(server);
      HttpResponse<String> response = request(server, "/v1/config", "GET");
      Assert.assertEquals(200, response.statusCode());
      Assert.assertEquals("{\"defaults\":{},\"overrides\":{}}", response.body());
      Assert.assertTrue(response.headers().firstValue("Content-Type").orElse("")
          .startsWith("application/json"));
    }
  }

  @Test
  public void rejectsNonReadMethodsOnConfig() throws Exception {
    ProxyConfig config = buildConfig(new RestCatalogConfig(
        true, "127.0.0.1", 0, 1, 4, null, null, RestCatalogPurgeMode.ALLOW, List.of(), true));
    try (RestCatalogServer server = RestCatalogServer.open(config, null, new PrometheusMetrics())) {
      HttpResponse<String> response = request(server, "/v1/config", "POST");
      Assert.assertEquals(405, response.statusCode());
      Assert.assertEquals("GET, HEAD", response.headers().firstValue("Allow").orElse(""));
    }
  }

  @Test
  public void respondsWithNotFoundForUnknownPath() throws Exception {
    ProxyConfig config = buildConfig(new RestCatalogConfig(
        true, "127.0.0.1", 0, 1, 4, null, null, RestCatalogPurgeMode.ALLOW, List.of(), true));
    try (RestCatalogServer server = RestCatalogServer.open(config, null, new PrometheusMetrics())) {
      HttpResponse<String> response = request(server, "/v1/namespaces", "GET");
      Assert.assertEquals(404, response.statusCode());
      Assert.assertTrue(response.body().contains("\"code\":404"));
    }
  }

  @Test
  public void boundPortReflectsActualListener() throws Exception {
    ProxyConfig config = buildConfig(new RestCatalogConfig(
        true, "127.0.0.1", 0, 1, 4, null, null, RestCatalogPurgeMode.ALLOW, List.of(), true));
    try (RestCatalogServer server = RestCatalogServer.open(config, null, new PrometheusMetrics())) {
      Assert.assertTrue("port must be allocated", server.boundPort() > 0);
    }
  }

  private static HttpResponse<String> request(RestCatalogServer server, String path, String method) throws Exception {
    HttpClient client = HttpClient.newBuilder().connectTimeout(HTTP_TIMEOUT).build();
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + server.boundPort() + path))
        .timeout(HTTP_TIMEOUT)
        .method(method, HttpRequest.BodyPublishers.noBody())
        .build();
    return client.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private static ProxyConfig buildConfig(RestCatalogConfig restCatalog) {
    return ProxyConfig.builder()
        .server(new ServerConfig("hms-proxy-test", "127.0.0.1", 9083, 1, 4))
        .catalogDbSeparator(".")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", new CatalogConfig(
            "catalog1",
            null,
            null,
            false,
            CatalogAccessMode.READ_WRITE,
            List.of(),
            null,
            null,
            Map.of("hive.metastore.uris", "thrift://hms-test:9083"))))
        .backend(new BackendConfig(Map.of()))
        .restCatalog(restCatalog)
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();
  }
}
