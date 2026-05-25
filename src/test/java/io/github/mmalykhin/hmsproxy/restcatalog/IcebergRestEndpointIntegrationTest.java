package io.github.mmalykhin.hmsproxy.restcatalog;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.restcatalog.RestCatalogConfig;
import io.github.mmalykhin.hmsproxy.config.routing.BackendConfig;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IcebergRestEndpointIntegrationTest {
  private static final String CATALOG_NAME = "catalog1";
  private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(5);

  private RecordingThriftIface delegate;
  private IcebergRestService service;
  private RestCatalogServer server;

  @Before
  public void setUp() throws Exception {
    delegate = new RecordingThriftIface();
    delegate.allDatabases = List.of("sales", "marketing");
    delegate.databases.put("sales", RecordingThriftIface.database("sales"));
    delegate.databases.put("marketing", RecordingThriftIface.database("marketing"));
    delegate.tablesByDatabase.put("sales", List.of("orders"));
    delegate.tables.put("sales.orders", RecordingThriftIface.table("sales", "orders"));

    ProxyConfig config = buildConfig();
    service = new IcebergRestService(config, delegate.iface);
    server = RestCatalogServer.open(config, service);
    Assert.assertNotNull("server must start", server);
  }

  @After
  public void tearDown() throws Exception {
    if (server != null) {
      server.close();
    }
    if (service != null) {
      service.close();
    }
  }

  @Test
  public void configEndpointReturnsDefaultCatalogPrefix() throws Exception {
    HttpResponse<String> response = get("/v1/config");
    Assert.assertEquals("body: " + response.body(), 200, response.statusCode());
    Assert.assertTrue("response: " + response.body(),
        response.body().contains("\"prefix\""));
    Assert.assertTrue("response: " + response.body(),
        response.body().contains("\"" + CATALOG_NAME + "\""));
  }

  @Test
  public void listNamespacesReturnsAllDatabases() throws Exception {
    HttpResponse<String> response = get("/v1/" + CATALOG_NAME + "/namespaces");
    Assert.assertEquals("body: " + response.body(), 200, response.statusCode());
    String body = response.body();
    Assert.assertTrue(body, body.contains("\"sales\""));
    Assert.assertTrue(body, body.contains("\"marketing\""));
  }

  @Test
  public void loadNamespaceReturnsDatabaseMetadata() throws Exception {
    HttpResponse<String> response = get("/v1/" + CATALOG_NAME + "/namespaces/sales");
    Assert.assertEquals("body: " + response.body(), 200, response.statusCode());
    Assert.assertTrue(response.body(), response.body().contains("\"sales\""));
  }

  @Test
  public void loadMissingNamespaceReturns404() throws Exception {
    HttpResponse<String> response = get("/v1/" + CATALOG_NAME + "/namespaces/missing");
    Assert.assertEquals("body: " + response.body(), 404, response.statusCode());
  }

  @Test
  public void listTablesReturnsTableNames() throws Exception {
    HttpResponse<String> response = get("/v1/" + CATALOG_NAME + "/namespaces/sales/tables");
    Assert.assertEquals("body: " + response.body(), 200, response.statusCode());
    Assert.assertTrue(response.body(), response.body().contains("\"orders\""));
  }

  @Test
  public void unknownCatalogPrefixReturns404() throws Exception {
    HttpResponse<String> response = get("/v1/unknown-catalog/namespaces");
    Assert.assertEquals(404, response.statusCode());
  }

  @Test
  public void rootPathReturns404() throws Exception {
    HttpResponse<String> response = get("/");
    Assert.assertEquals(404, response.statusCode());
  }

  @Test
  public void loadingNonIcebergTableReturnsErrorResponse() throws Exception {
    HttpResponse<String> response = get("/v1/" + CATALOG_NAME + "/namespaces/sales/tables/orders");
    Assert.assertTrue("expected 4xx for non-iceberg table, got " + response.statusCode()
            + " body: " + response.body(),
        response.statusCode() >= 400 && response.statusCode() < 500);
  }

  private HttpResponse<String> get(String path) throws Exception {
    HttpClient client = HttpClient.newBuilder().connectTimeout(HTTP_TIMEOUT).build();
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + server.boundPort() + path))
        .timeout(HTTP_TIMEOUT)
        .GET()
        .build();
    return client.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private static ProxyConfig buildConfig() {
    return ProxyConfig.builder()
        .server(new ServerConfig("hms-proxy-test", "127.0.0.1", 9083, 1, 4))
        .catalogDbSeparator(".")
        .defaultCatalog(CATALOG_NAME)
        .catalogs(Map.of(CATALOG_NAME, new CatalogConfig(
            CATALOG_NAME,
            null,
            null,
            false,
            CatalogAccessMode.READ_WRITE,
            List.of(),
            null,
            null,
            Map.of("hive.metastore.uris", "thrift://hms-test:9083"))))
        .backend(new BackendConfig(Map.of()))
        .restCatalog(new RestCatalogConfig(true, "127.0.0.1", 0, 1, 4, null, null))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();
  }
}
