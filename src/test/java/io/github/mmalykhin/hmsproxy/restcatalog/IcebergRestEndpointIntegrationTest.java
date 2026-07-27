package io.github.mmalykhin.hmsproxy.restcatalog;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.restcatalog.RestCatalogConfig;
import io.github.mmalykhin.hmsproxy.config.routing.BackendConfig;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;
import io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics;
import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class IcebergRestEndpointIntegrationTest {
  private static final String CATALOG_NAME = "catalog1";
  private static final String CATALOG2_NAME = "catalog2";
  private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(5);

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private RecordingThriftIface delegate;
  private IcebergRestServices services;
  private RestCatalogServer server;
  private PrometheusMetrics metrics;

  @Before
  public void setUp() throws Exception {
    delegate = new RecordingThriftIface();
    // "default" and "catalog2__default" back catalog2's clean view (translated
    // to/from "default"); "sales"/"marketing" back the existing catalog1 cases.
    delegate.allDatabases = List.of("sales", "marketing", "default", "catalog2__default");
    delegate.databases.put("sales", RecordingThriftIface.database("sales"));
    delegate.databases.put("marketing", RecordingThriftIface.database("marketing"));
    delegate.databases.put("default", RecordingThriftIface.database("default"));
    delegate.databases.put("catalog2__default", RecordingThriftIface.database("catalog2__default"));
    delegate.tablesByDatabase.put("sales", List.of("orders", "shipments"));
    delegate.tables.put("sales.orders", RecordingThriftIface.table("sales", "orders"));
    delegate.tables.put("sales.shipments", RecordingThriftIface.table("sales", "shipments"));

    // "t1" and "events" carry a real metadata_location (unlike "orders"/"shipments" above,
    // which are deliberately non-loadable so loadingNonIcebergTableReturnsErrorResponse has
    // a plain Hive table to exercise), so the exists routes can genuinely resolve them to a
    // valid Iceberg table rather than tripping NoSuchTableException on the missing metadata.
    Table t1 = RecordingThriftIface.table("default", "t1");
    t1.getParameters().put("metadata_location", writeIcebergTableMetadata("t1"));
    delegate.tablesByDatabase.put("default", List.of("t1"));
    delegate.tables.put("default.t1", t1);

    Table events = RecordingThriftIface.table("catalog2__default", "events");
    events.getParameters().put("metadata_location", writeIcebergTableMetadata("events"));
    delegate.tablesByDatabase.put("catalog2__default", List.of("events"));
    delegate.tables.put("catalog2__default.events", events);

    ProxyConfig config = buildConfig();
    services = IcebergRestServices.open(config, delegate.iface);
    metrics = new PrometheusMetrics();
    server = RestCatalogServer.open(config, services, metrics);
    Assert.assertNotNull("server must start", server);
  }

  @After
  public void tearDown() throws Exception {
    if (server != null) {
      server.close();
    }
    if (services != null) {
      services.close();
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
  public void listNamespacesWithPageSizeReturnsFirstPage() throws Exception {
    // Regression test: Iceberg 1.9.2's CatalogHandlers.paginate() throws
    // NumberFormatException on a null pageToken, which is exactly what a client requesting
    // the first page (pageSize but no pageToken yet) sends. See IcebergHttpHandler's
    // pageToken defaulting for the fix.
    HttpResponse<String> response = get("/v1/" + CATALOG_NAME + "/namespaces?pageSize=1");
    Assert.assertEquals("body: " + response.body(), 200, response.statusCode());
    Assert.assertTrue("response: " + response.body(),
        response.body().contains("\"next-page-token\""));
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
  public void listTablesWithPageSizeReturnsFirstPage() throws Exception {
    // Same regression as listNamespacesWithPageSizeReturnsFirstPage but for the tables route.
    HttpResponse<String> response = get("/v1/" + CATALOG_NAME + "/namespaces/sales/tables?pageSize=1");
    Assert.assertEquals("body: " + response.body(), 200, response.statusCode());
    Assert.assertTrue("response: " + response.body(),
        response.body().contains("\"next-page-token\""));
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

  @Test
  public void configWithWarehouseSelectsCatalogPrefix() throws Exception {
    HttpResponse<String> response = get("/v1/config?warehouse=catalog2");
    Assert.assertEquals(200, response.statusCode());
    Assert.assertTrue(response.body(), response.body().contains("\"prefix\":\"catalog2\""));
  }

  @Test
  public void configWithUnknownWarehouseReturns400() throws Exception {
    Assert.assertEquals(400, get("/v1/config?warehouse=nope").statusCode());
  }

  @Test
  public void secondPrefixShowsCleanView() throws Exception {
    HttpResponse<String> response = get("/v1/catalog2/namespaces");
    Assert.assertEquals(200, response.statusCode());
    Assert.assertTrue(response.body(), response.body().contains("[\"default\"]"));
    Assert.assertFalse(response.body(), response.body().contains("catalog2__default"));
  }

  @Test
  public void defaultPrefixKeepsFederatedView() throws Exception {
    HttpResponse<String> response = get("/v1/catalog1/namespaces");
    Assert.assertEquals(200, response.statusCode());
    Assert.assertTrue(response.body(), response.body().contains("[\"catalog2__default\"]"));
  }

  @Test
  public void recordsRestRequestMetricsAndListenerInfo() throws Exception {
    Assert.assertEquals(200, get("/v1/" + CATALOG_NAME + "/namespaces").statusCode());
    Assert.assertEquals(404, get("/v1/nope/namespaces").statusCode());
    Assert.assertEquals(400, get("/v1/config?warehouse=nope").statusCode());

    String rendered = metrics.render();
    Assert.assertTrue("rendered: " + rendered,
        rendered.contains("prefix=\"" + CATALOG_NAME + "\",route=\"list_namespaces\",status=\"200\""));
    Assert.assertTrue("rendered: " + rendered,
        rendered.contains("prefix=\"unknown\",route=\"unknown_prefix\",status=\"404\""));
    Assert.assertTrue("rendered: " + rendered,
        rendered.contains("prefix=\"unknown\",route=\"bad_request\",status=\"400\""));
    Assert.assertTrue("rendered: " + rendered, rendered.contains("hms_proxy_rest_listener_info"));
  }

  @Test
  public void errorResponsesCarryNoStackTrace() throws Exception {
    HttpResponse<String> response = get("/v1/catalog1/namespaces/no_such_ns_probe");
    Assert.assertEquals(404, response.statusCode());
    Assert.assertTrue(response.body().contains("\"type\""));
    Assert.assertFalse("error body must not leak a server stack trace: " + response.body(),
        response.body().contains("\"stack\":[\""));
  }

  @Test
  public void unparseableRequestBodyReturns400() throws Exception {
    HttpResponse<String> response = post(
        "/v1/catalog1/namespaces/default/tables/t1/metrics", "not json at all");
    Assert.assertEquals(400, response.statusCode());
    Assert.assertTrue(response.body().contains("BadRequestException"));
  }

  @Test
  public void configAdvertisesOnlyServedEndpoints() throws Exception {
    HttpResponse<String> response = get("/v1/config");
    Assert.assertEquals(200, response.statusCode());
    String body = response.body();
    Assert.assertTrue(body, body.contains("GET /v1/{prefix}/namespaces"));
    Assert.assertTrue(body, body.contains("HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}"));
    Assert.assertFalse("read-only endpoint must not advertise writes: " + body,
        body.contains("POST /v1/{prefix}/namespaces/{namespace}/tables"));
    Assert.assertFalse("read-only endpoint must not advertise deletes: " + body,
        body.contains("DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}"));
  }

  @Test
  public void prefixedConfigAnswersLikeConfigWithWarehouse() throws Exception {
    HttpResponse<String> response = get("/v1/catalog2/config");
    Assert.assertEquals(200, response.statusCode());
    Assert.assertTrue(response.body(), response.body().contains("\"prefix\":\"catalog2\""));
    Assert.assertFalse("prefixed config must not advertise writes either: " + response.body(),
        response.body().contains("POST /v1/{prefix}/namespaces/{namespace}/tables"));
  }

  @Test
  public void prefixedConfigForUnknownCatalogReturns404() throws Exception {
    Assert.assertEquals(404, get("/v1/no_such_catalog_probe/config").statusCode());
  }

  @Test
  public void headOnExistingNamespaceReturns204() throws Exception {
    Assert.assertEquals(204, head("/v1/catalog1/namespaces/default").statusCode());
  }

  @Test
  public void headOnMissingNamespaceReturns404() throws Exception {
    Assert.assertEquals(404, head("/v1/catalog1/namespaces/no_such_ns_probe").statusCode());
  }

  @Test
  public void headOnExistingTableReturns204() throws Exception {
    Assert.assertEquals(204, head("/v1/catalog1/namespaces/default/tables/t1").statusCode());
  }

  @Test
  public void headOnMissingTableReturns404() throws Exception {
    Assert.assertEquals(404, head("/v1/catalog1/namespaces/default/tables/no_such_table_probe").statusCode());
  }

  @Test
  public void headOnTableUnderSecondPrefixReturns204() throws Exception {
    Assert.assertEquals(204, head("/v1/catalog2/namespaces/default/tables/events").statusCode());
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

  // Writes a minimal but genuinely readable Iceberg table metadata.json to a local temp
  // directory and returns its path, suitable for the "metadata_location" table parameter.
  // HiveTableOperations.doRefresh() parses that file for real, so a table only resolves
  // as an existing Iceberg table (and thus exists routes) when this points at valid JSON.
  private String writeIcebergTableMetadata(String tableName) throws Exception {
    File tableDir = tempFolder.newFolder(tableName);
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    TableMetadata metadata = TableMetadata.newTableMetadata(
        schema, PartitionSpec.unpartitioned(), "file://" + tableDir.getAbsolutePath(), Map.of());
    File metadataFile = new File(tableDir, "metadata/v1.metadata.json");
    metadataFile.getParentFile().mkdirs();
    TableMetadataParser.write(
        metadata,
        HadoopOutputFile.fromPath(new Path(metadataFile.getAbsolutePath()), new Configuration()));
    return metadataFile.getAbsolutePath();
  }

  private HttpResponse<String> head(String path) throws Exception {
    HttpClient client = HttpClient.newBuilder().connectTimeout(HTTP_TIMEOUT).build();
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + server.boundPort() + path))
        .timeout(HTTP_TIMEOUT)
        .method("HEAD", HttpRequest.BodyPublishers.noBody())
        .build();
    return client.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private HttpResponse<String> post(String path, String body) throws Exception {
    HttpClient client = HttpClient.newBuilder().connectTimeout(HTTP_TIMEOUT).build();
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + server.boundPort() + path))
        .timeout(HTTP_TIMEOUT)
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body))
        .build();
    return client.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private static ProxyConfig buildConfig() {
    return ProxyConfig.builder()
        .server(new ServerConfig("hms-proxy-test", "127.0.0.1", 9083, 1, 4))
        .catalogDbSeparator("__")
        .defaultCatalog(CATALOG_NAME)
        .catalogs(Map.of(
            CATALOG_NAME, new CatalogConfig(
                CATALOG_NAME,
                null,
                null,
                false,
                CatalogAccessMode.READ_WRITE,
                List.of(),
                null,
                null,
                Map.of("hive.metastore.uris", "thrift://hms-test:9083")),
            CATALOG2_NAME, new CatalogConfig(
                CATALOG2_NAME,
                null,
                null,
                false,
                CatalogAccessMode.READ_WRITE,
                List.of(),
                null,
                null,
                Map.of("hive.metastore.uris", "thrift://hms-test:9084"))))
        .backend(new BackendConfig(Map.of()))
        .restCatalog(new RestCatalogConfig(true, "127.0.0.1", 0, 1, 4, null, null))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();
  }
}
