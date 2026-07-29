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
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
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
    // Resolves the way CatalogRouter.resolveDatabase would for this fixture (default catalog1,
    // other catalog2, separator "__"), without needing a real CatalogRouter: CatalogRouter.open
    // eagerly connects to each catalog's hive.metastore.uris, which the fake URIs below cannot
    // satisfy.
    Function<String, String> catalogForExternalDb = externalDbName ->
        externalDbName != null && externalDbName.startsWith(CATALOG2_NAME + "__")
            ? CATALOG2_NAME
            : CATALOG_NAME;
    services = IcebergRestServices.open(config, delegate.iface, catalogForExternalDb);
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
  public void unhandledErrorFromDispatchYieldsResponseInsteadOfHang() throws Exception {
    // Regression test for IcebergHttpHandler.doHandle's catch-all: it used to catch only
    // Exception, so an Error thrown below the REST adapter (like the real
    // NoSuchMethodError from a classpath mismatch this phase hit) would escape doHandle and
    // handle() entirely, leaving the JDK HTTP server to abandon the exchange with no
    // response - the client would hang until its own timeout instead of seeing a 5xx. If
    // that regressed, this get() call would throw (timeout or connection reset) rather than
    // returning a response, so this test fails loudly instead of asserting a wrong status.
    HttpResponse<String> response =
        get("/v1/" + CATALOG_NAME + "/namespaces/" + RecordingThriftIface.THROWS_ERROR_PROBE_DB);
    Assert.assertEquals("body: " + response.body(), 500, response.statusCode());
    Assert.assertTrue(response.body(), response.body().contains("\"type\":\"Error\""));
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

  // Body used by the two refusal tests below: a minimal but genuinely parseable
  // CreateTableRequest. An empty "schema":{} (as a bare write-route smoke check might use)
  // fails Iceberg's own SchemaParser before the request ever reaches the write gate, which
  // would make these tests report a body-parsing 400 instead of exercising the gate.
  private static final String MINIMAL_CREATE_TABLE_BODY =
      "{\"name\":\"t9\",\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[]}}";

  @Test
  public void createTableUnderNonDefaultPrefixIsRefused() throws Exception {
    HttpResponse<String> response = post(
        "/v1/catalog2/namespaces/default/tables", MINIMAL_CREATE_TABLE_BODY);
    Assert.assertEquals(403, response.statusCode());
    Assert.assertTrue(response.body(), response.body().contains("ForbiddenException"));
  }

  @Test
  public void createTableUnderFederatedNamespaceIsRefused() throws Exception {
    // The whole point of this test: prefix catalog1 IS the default catalog, so a gate that keys
    // on the URL prefix instead of the catalog the namespace resolves to would wrongly allow
    // this. "catalog2__default" is catalog2's database, exposed under catalog1's federated view.
    HttpResponse<String> response = post(
        "/v1/catalog1/namespaces/catalog2__default/tables", MINIMAL_CREATE_TABLE_BODY);
    Assert.assertEquals(403, response.statusCode());
    Assert.assertTrue(response.body(), response.body().contains("ForbiddenException"));
  }

  // --- Coverage for the write routes WriteRouteGate had to be extended to cover: every write
  // route RESTCatalogAdapter.Route exposes, not just the original five table routes. Each
  // request below only needs to survive JSON parsing far enough to reach the gate - the gate
  // check runs, and refuses, before the request would ever reach RoutingHiveCatalog's dispatch.

  @Test
  public void dropViewUnderFederatedNamespaceIsRefused() throws Exception {
    // Path-shaped: namespace comes from the URL, same as the table routes above.
    HttpResponse<String> response =
        delete("/v1/catalog1/namespaces/catalog2__default/views/v1");
    Assert.assertEquals(403, response.statusCode());
    Assert.assertTrue(response.body(), response.body().contains("ForbiddenException"));
  }

  // Body used by the rename-view refusal test: a RenameTableRequest whose destination lands
  // in catalog2's federated namespace. RENAME_VIEW carries the same request shape as
  // RENAME_TABLE, so the gate reuses that same source/destination check.
  private static final String RENAME_VIEW_FEDERATED_DESTINATION_BODY =
      "{\"source\":{\"namespace\":[\"default\"],\"name\":\"v1\"},"
          + "\"destination\":{\"namespace\":[\"catalog2__default\"],\"name\":\"v2\"}}";

  @Test
  public void renameViewToFederatedNamespaceIsRefused() throws Exception {
    // Rename-shaped: both source and destination are in the body, not the URL.
    HttpResponse<String> response =
        post("/v1/catalog1/views/rename", RENAME_VIEW_FEDERATED_DESTINATION_BODY);
    Assert.assertEquals(403, response.statusCode());
    Assert.assertTrue(response.body(), response.body().contains("ForbiddenException"));
  }

  // Body used by the create-namespace refusal test: a CreateNamespaceRequest whose namespace
  // is catalog2's federated database name.
  private static final String CREATE_NAMESPACE_FEDERATED_BODY =
      "{\"namespace\":[\"catalog2__default\"],\"properties\":{}}";

  @Test
  public void createNamespaceUnderFederatedNamespaceIsRefused() throws Exception {
    // Body-shaped: CreateNamespaceRequest carries its namespace in the body, not the URL.
    HttpResponse<String> response = post("/v1/catalog1/namespaces", CREATE_NAMESPACE_FEDERATED_BODY);
    Assert.assertEquals(403, response.statusCode());
    Assert.assertTrue(response.body(), response.body().contains("ForbiddenException"));
  }

  // Body used by the commit-transaction refusal test: two table changes, one for a
  // default-catalog table ("default.t1") and one for a table in catalog2's federated
  // namespace ("catalog2__default.t2"). This is the shape of the hole this task closes: a
  // client bundling one federated table into an otherwise-legitimate multi-table atomic
  // commit must have the whole commit refused, not just the one table silently dropped.
  private static final String COMMIT_TRANSACTION_MIXED_CATALOG_BODY =
      "{\"table-changes\":["
          + "{\"identifier\":{\"namespace\":[\"default\"],\"name\":\"t1\"},"
          + "\"requirements\":[],\"updates\":[]},"
          + "{\"identifier\":{\"namespace\":[\"catalog2__default\"],\"name\":\"t2\"},"
          + "\"requirements\":[],\"updates\":[]}]}";

  @Test
  public void commitTransactionMixingFederatedTableIsRefused() throws Exception {
    // Body-shaped, worst case: COMMIT_TRANSACTION is the standard multi-table atomic-commit
    // endpoint. Before this fix, it dispatched unguarded regardless of any of its tables'
    // catalogs - this is the proof that it no longer does, for the specific mixed case a
    // per-table check must not miss.
    HttpResponse<String> response =
        post("/v1/catalog1/transactions/commit", COMMIT_TRANSACTION_MIXED_CATALOG_BODY);
    Assert.assertEquals(403, response.statusCode());
    Assert.assertTrue(response.body(), response.body().contains("ForbiddenException"));
  }

  // Iceberg's Endpoint#toString() renders "<HTTP method> <path>", and several write routes'
  // renderings are a plain prefix of a sibling route's rendering - e.g. V1_CREATE_TABLE's
  // "POST /v1/{prefix}/namespaces/{namespace}/tables" is a prefix of V1_UPDATE_TABLE's
  // ".../tables/{table}", and V1_CREATE_NAMESPACE's bare "POST /v1/{prefix}/namespaces" is a
  // prefix of nearly every other namespace/table/view write route. A plain body.contains(...) on
  // one of these bare renderings would still pass even if that exact Endpoint constant were
  // dropped from IcebergRestService.WRITE_ENDPOINTS, as long as the sibling with the longer
  // rendering stayed - it would not pin the route it claims to. Printing the actual
  // ConfigResponse JSON (via ConfigResponseParser, e.g. {"endpoints":["POST /v1/{prefix}/namespaces",
  // "POST /v1/{prefix}/namespaces/{namespace}/tables", ...]}) confirms each endpoint is emitted as
  // its own JSON string, so wrapping the expected rendering in its JSON quotes pins it to exactly
  // the one Endpoint constant that produces that full quoted token.
  private static String jsonEndpoint(String httpMethodAndPath) {
    return "\"" + httpMethodAndPath + "\"";
  }

  @Test
  public void configAdvertisesOnlyServedEndpoints() throws Exception {
    // /v1/config with no warehouse resolves to the default catalog (catalog1 in this fixture),
    // which serves table writes (phase 5a) plus view writes, namespace DDL and the
    // transaction-commit (phase 5b) - all thirteen routes WriteRouteGate gates are now genuinely
    // working features, dispatched through the same generic RESTCatalogAdapter/RoutingHiveCatalog
    // path the table routes use, so all of them belong on this list.
    HttpResponse<String> response = get("/v1/config");
    Assert.assertEquals(200, response.statusCode());
    String body = response.body();
    Assert.assertTrue(body, body.contains("GET /v1/{prefix}/namespaces"));
    Assert.assertTrue(body, body.contains("HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}"));
    Assert.assertTrue("default catalog must advertise table writes: " + body,
        body.contains(jsonEndpoint("POST /v1/{prefix}/namespaces/{namespace}/tables")));
    Assert.assertTrue("default catalog must advertise table deletes: " + body,
        body.contains("DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}"));
    Assert.assertTrue(
        "default catalog must advertise view writes: " + body,
        body.contains(jsonEndpoint("POST /v1/{prefix}/namespaces/{namespace}/views")));
    Assert.assertTrue(
        "default catalog must advertise view renames: " + body,
        body.contains("POST /v1/{prefix}/views/rename"));
    Assert.assertTrue(
        "default catalog must advertise namespace creation: " + body,
        body.contains(jsonEndpoint("POST /v1/{prefix}/namespaces")));
    Assert.assertTrue(
        "default catalog must advertise namespace property updates: " + body,
        body.contains("POST /v1/{prefix}/namespaces/{namespace}/properties"));
    Assert.assertTrue(
        "default catalog must advertise namespace deletes: " + body,
        body.contains(jsonEndpoint("DELETE /v1/{prefix}/namespaces/{namespace}")));
    Assert.assertTrue(
        "default catalog must advertise the transaction commit: " + body,
        body.contains("POST /v1/{prefix}/transactions/commit"));
  }

  @Test
  public void defaultCatalogAdvertisesViewAndNamespaceWrites() throws Exception {
    String body = get("/v1/" + CATALOG_NAME + "/config").body();
    Assert.assertTrue(body, body.contains(jsonEndpoint("POST /v1/{prefix}/namespaces/{namespace}/views")));
    Assert.assertTrue(body, body.contains(jsonEndpoint("POST /v1/{prefix}/namespaces")));
    Assert.assertTrue(body, body.contains("POST /v1/{prefix}/transactions/commit"));
  }

  @Test
  public void nonDefaultCatalogAdvertisesNoWritesAtAll() throws Exception {
    String body = get("/v1/" + CATALOG2_NAME + "/config").body();
    Assert.assertFalse(body, body.contains("POST /v1/{prefix}/namespaces/{namespace}/views"));
    Assert.assertFalse(body, body.contains("POST /v1/{prefix}/transactions/commit"));
    Assert.assertTrue(body, body.contains("GET /v1/{prefix}/namespaces"));
  }

  @Test
  public void defaultCatalogConfigAdvertisesWrites() throws Exception {
    String body = get("/v1/" + CATALOG_NAME + "/config").body();
    Assert.assertTrue(body, body.contains(jsonEndpoint("POST /v1/{prefix}/namespaces/{namespace}/tables")));
  }

  @Test
  public void nonDefaultCatalogConfigStillAdvertisesReadsOnly() throws Exception {
    String body = get("/v1/" + CATALOG2_NAME + "/config").body();
    Assert.assertFalse(body, body.contains("POST /v1/{prefix}/namespaces/{namespace}/tables"));
    Assert.assertTrue(body, body.contains("GET /v1/{prefix}/namespaces"));
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
  public void postToConfigReturns404() throws Exception {
    Assert.assertEquals(404, post("/v1/config", "{}").statusCode());
  }

  @Test
  public void postToPrefixedConfigReturns404() throws Exception {
    Assert.assertEquals(404, post("/v1/" + CATALOG2_NAME + "/config", "{}").statusCode());
  }

  @Test
  public void headOnExistingNamespaceReturns204() throws Exception {
    Assert.assertEquals(204, head("/v1/catalog1/namespaces/default").statusCode());
  }

  @Test
  public void headOnMissingNamespaceReturns404() throws Exception {
    HttpResponse<String> response = headAssertingNoUnhandledServerError(
        "/v1/catalog1/namespaces/no_such_ns_probe");
    Assert.assertEquals(404, response.statusCode());
  }

  @Test
  public void headOnExistingTableReturns204() throws Exception {
    Assert.assertEquals(204, head("/v1/catalog1/namespaces/default/tables/t1").statusCode());
  }

  @Test
  public void headOnMissingTableReturns404() throws Exception {
    HttpResponse<String> response = headAssertingNoUnhandledServerError(
        "/v1/catalog1/namespaces/default/tables/no_such_table_probe");
    Assert.assertEquals(404, response.statusCode());
  }

  @Test
  public void headOnTableUnderSecondPrefixReturns204() throws Exception {
    Assert.assertEquals(204, head("/v1/catalog2/namespaces/default/tables/events").statusCode());
  }

  @Test
  public void dropTableWithPurgeDeletesDataFiles() throws Exception {
    File dataFile = registerTableWithCommittedDataFile("purge_me");

    HttpResponse<String> response =
        delete("/v1/" + CATALOG_NAME + "/namespaces/default/tables/purge_me?purgeRequested=true");

    Assert.assertEquals("body: " + response.body(), 204, response.statusCode());
    Assert.assertTrue(delegate.calls.toString(),
        delegate.calls.contains("drop_table:default.purge_me"));
    Assert.assertFalse("purge must delete the table's data files, but " + dataFile
        + " is still there", dataFile.exists());
  }

  @Test
  public void dropTableWithoutPurgeKeepsDataFiles() throws Exception {
    File dataFile = registerTableWithCommittedDataFile("keep_me");

    HttpResponse<String> response =
        delete("/v1/" + CATALOG_NAME + "/namespaces/default/tables/keep_me");

    Assert.assertEquals("body: " + response.body(), 204, response.statusCode());
    Assert.assertTrue("a drop without purge must leave the data files alone, but " + dataFile
        + " is gone", dataFile.exists());
  }

  @Test
  public void dropTableWithPurgeUnderFederatedNamespaceDeletesNothing() throws Exception {
    // The write gate has always refused this route, but until purge worked the refusal was
    // belt-and-braces: the request died on Avro before it could touch a file either way. Now
    // that a purge really deletes data, the gate is the only thing standing between a client
    // and another catalog's files, so pin both halves - the 403 and the untouched data file.
    File dataFile = registerTableWithCommittedDataFile("federated_purge_me");
    delegate.tables.put(
        "catalog2__default.federated_purge_me", delegate.tables.get("default.federated_purge_me"));

    HttpResponse<String> response = delete("/v1/" + CATALOG_NAME
        + "/namespaces/catalog2__default/tables/federated_purge_me?purgeRequested=true");

    Assert.assertEquals(403, response.statusCode());
    Assert.assertTrue(response.body(), response.body().contains("ForbiddenException"));
    Assert.assertTrue("a refused purge must not delete anything, but " + dataFile + " is gone",
        dataFile.exists());
  }

  /**
   * A canary for the Avro codecs the fat jar does NOT carry. Avro's snappy, xz and zstd codecs
   * are optional dependencies, so the purge can only read manifests written with deflate - and
   * it gets away with that because {@code ManifestWriter} never applies table properties to the
   * manifest appender: whatever `write.avro.compression-codec` says (it drives data files and
   * delete files, which this proxy never reads), the manifest itself is always deflate, which
   * Avro compresses with the JDK's own Deflater.
   *
   * <p>Should a future Iceberg start honouring that property for manifests, this test fails
   * with a missing codec instead of the purge silently breaking in production - and that is the
   * moment to add {@code org.xerial.snappy:snappy-java} and friends, not before.
   */
  @Test
  public void dropTableWithPurgeReadsManifestsOfATableAskingForSnappy() throws Exception {
    File dataFile = registerTableWithCommittedDataFile(
        "snappy_purge_me", Map.of("write.avro.compression-codec", "snappy"));

    HttpResponse<String> response = delete(
        "/v1/" + CATALOG_NAME + "/namespaces/default/tables/snappy_purge_me?purgeRequested=true");

    Assert.assertEquals("body: " + response.body(), 204, response.statusCode());
    Assert.assertFalse("purge must delete the table's data files, but " + dataFile
        + " is still there", dataFile.exists());
  }

  /**
   * A multi-table transaction whose second table's requirement cannot hold. The REST spec calls
   * this route a transaction, so a client may well assume all-or-nothing; this pins what the
   * proxy actually does with the first table's changes when the second one is rejected.
   */
  @Test
  public void multiTableTransactionWithOneFailingRequirementLeavesNothingApplied() throws Exception {
    registerTableWithCommittedDataFile("txn_a");
    registerTableWithCommittedDataFile("txn_b");
    String uuidA = tableUuid("txn_a");
    String uuidB = tableUuid("txn_b");

    // Table A's requirement holds; table B's names a uuid the table does not have, so its own
    // commit must be refused.
    String body = "{\"table-changes\":["
        + "{\"identifier\":{\"namespace\":[\"default\"],\"name\":\"txn_a\"},"
        + "\"requirements\":[{\"type\":\"assert-table-uuid\",\"uuid\":\"" + uuidA + "\"}],"
        + "\"updates\":[{\"action\":\"set-properties\",\"updates\":{\"txn\":\"applied\"}}]},"
        + "{\"identifier\":{\"namespace\":[\"default\"],\"name\":\"txn_b\"},"
        + "\"requirements\":[{\"type\":\"assert-table-uuid\",\"uuid\":\"00000000-0000-0000-0000-000000000000\"}],"
        + "\"updates\":[{\"action\":\"set-properties\",\"updates\":{\"txn\":\"applied\"}}]}]}";

    HttpResponse<String> response = post("/v1/" + CATALOG_NAME + "/transactions/commit", body);

    // The refusal has to come from the requirement check itself. A 400 here would mean the body
    // never reached the tables, and every assertion below would hold vacuously.
    Assert.assertEquals("expected a requirement failure, got " + response.statusCode()
        + ": " + response.body(), 409, response.statusCode());
    Assert.assertTrue(response.body(), response.body().contains("Requirement failed"));
    Assert.assertFalse("table B's uuid did not match, so its change must not be applied: "
        + get("/v1/" + CATALOG_NAME + "/namespaces/default/tables/txn_b").body(),
        get("/v1/" + CATALOG_NAME + "/namespaces/default/tables/txn_b").body().contains("\"txn\":\"applied\""));
    Assert.assertFalse("the transaction failed, so table A must not keep its change either: "
        + get("/v1/" + CATALOG_NAME + "/namespaces/default/tables/txn_a").body(),
        get("/v1/" + CATALOG_NAME + "/namespaces/default/tables/txn_a").body().contains("\"txn\":\"applied\""));
    Assert.assertNotNull(uuidB);
  }

  /**
   * A multi-table transaction whose second table is refused by the metastore itself (not by a
   * requirement): the request must not answer success, and the refused table must keep serving
   * the metadata the metastore still points at.
   *
   * <p>The first table stays committed - the transaction route is not atomic, as
   * RESTCatalogAdapter's own javadoc says: it validates every requirement up front, then commits
   * the tables one by one with no rollback. That half is pinned here too, so a future Iceberg
   * that changes it is noticed rather than silently trusted.
   *
   * <p>Confirmed against the smoke stand's real Apache Hive 4.1.0 metastore (profile hive4,
   * Kerberos) by starving the ddl rate-limit class so one alter_table in a multi-table commit was
   * refused: the response was 500 CommitStateUnknownException, the refused table kept its old
   * metadata_location and served no uncommitted properties, and the tables committed before it
   * stayed committed - exactly what this test asserts.
   */
  @Test
  public void multiTableTransactionMustNotReportSuccessWhenTheSecondCommitFails() throws Exception {
    registerTableWithCommittedDataFile("txn_c");
    registerTableWithCommittedDataFile("txn_d");
    String uuidC = tableUuid("txn_c");
    String uuidD = tableUuid("txn_d");
    String locationBefore =
        delegate.tables.get("default.txn_d").getParameters().get("metadata_location");
    delegate.alterTableFailures.add("txn_d");

    String body = "{\"table-changes\":["
        + "{\"identifier\":{\"namespace\":[\"default\"],\"name\":\"txn_c\"},"
        + "\"requirements\":[{\"type\":\"assert-table-uuid\",\"uuid\":\"" + uuidC + "\"}],"
        + "\"updates\":[{\"action\":\"set-properties\",\"updates\":{\"txn\":\"applied\"}}]},"
        + "{\"identifier\":{\"namespace\":[\"default\"],\"name\":\"txn_d\"},"
        + "\"requirements\":[{\"type\":\"assert-table-uuid\",\"uuid\":\"" + uuidD + "\"}],"
        + "\"updates\":[{\"action\":\"set-properties\",\"updates\":{\"txn\":\"applied\"}}]}]}";

    HttpResponse<String> response = post("/v1/" + CATALOG_NAME + "/transactions/commit", body);

    // The refusal has to come from alter_table, not from a requirement check: otherwise every
    // assertion below would hold for the wrong reason.
    Assert.assertTrue(delegate.calls.toString(),
        delegate.calls.contains("alter_table_injected_failure:default.txn_d"));
    Assert.assertNotEquals("the second table's commit was rejected by the metastore, so the"
        + " request must not report success", 204, response.statusCode());
    Assert.assertEquals("the refused alter_table must leave metadata_location untouched",
        locationBefore, delegate.tables.get("default.txn_d").getParameters().get("metadata_location"));
    Assert.assertFalse("the rejected table must not be served with its uncommitted metadata",
        get("/v1/" + CATALOG_NAME + "/namespaces/default/tables/txn_d").body().contains("\"txn\":\"applied\""));
    Assert.assertTrue("the transaction route is not atomic: the table committed before the"
        + " failure stays committed - " + get("/v1/" + CATALOG_NAME + "/namespaces/default/tables/txn_c").body(),
        get("/v1/" + CATALOG_NAME + "/namespaces/default/tables/txn_c").body().contains("\"txn\":\"applied\""));
  }

  private String tableUuid(String tableName) throws Exception {
    String body = get("/v1/" + CATALOG_NAME + "/namespaces/default/tables/" + tableName).body();
    java.util.regex.Matcher matcher =
        java.util.regex.Pattern.compile("\"table-uuid\"\\s*:\\s*\"([^\"]+)\"").matcher(body);
    Assert.assertTrue("no table-uuid in " + body, matcher.find());
    return matcher.group(1);
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

  // Creates a real Iceberg table on local disk, commits one data file into it and registers it
  // in the fake metastore under "default", returning that data file. Unlike
  // writeIcebergTableMetadata above, the committed snapshot gives the table a manifest list and
  // a manifest - the only shape that makes a purge actually walk the manifests (which is what
  // reads Avro) rather than just unlinking metadata JSON.
  private File registerTableWithCommittedDataFile(String tableName) throws Exception {
    return registerTableWithCommittedDataFile(tableName, Map.of());
  }

  private File registerTableWithCommittedDataFile(String tableName, Map<String, String> properties)
      throws Exception {
    File tableDir = tempFolder.newFolder(tableName);
    File dataFile = new File(tableDir, "data/data.parquet");
    dataFile.getParentFile().mkdirs();
    Files.write(dataFile.toPath(), new byte[] {1, 2, 3});

    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    org.apache.iceberg.Table table = new HadoopTables(new Configuration()).create(
        schema, PartitionSpec.unpartitioned(), properties, "file://" + tableDir.getAbsolutePath());
    table.newAppend()
        .appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("file://" + dataFile.getAbsolutePath())
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(dataFile.length())
            .withRecordCount(1)
            .build())
        .commit();

    Table hiveTable = RecordingThriftIface.table("default", tableName);
    hiveTable.getParameters().put(
        "metadata_location", ((HasTableOperations) table).operations().current().metadataFileLocation());
    delegate.tables.put("default." + tableName, hiveTable);
    return dataFile;
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

  // The JDK HttpServer forces contentLen to 0 for every HEAD response and never sets a
  // Content-length header for one, regardless of what the handler passed to
  // sendResponseHeaders(status, ...) - so neither response.body() (always "" per RFC 9110) nor
  // the Content-Length header can tell apart sendResponseHeaders(status, -1) (no body, correct)
  // from sendResponseHeaders(status, bytes.length) (a declared body whose write then fails with
  // "stream closed"): both are wire-identical to the client. The one thing that does differ is
  // that the failed write throws, which the handler's catch-all logs as "Unhandled error serving
  // ..." at WARN. Assert on that instead: attach an appender to the handler's logger for the
  // duration of the request and require it to have logged nothing.
  private HttpResponse<String> headAssertingNoUnhandledServerError(String path) throws Exception {
    Logger logger = Logger.getLogger(IcebergHttpHandler.class);
    List<LoggingEvent> captured = new CopyOnWriteArrayList<>();
    Appender appender = new AppenderSkeleton() {
      @Override
      protected void append(LoggingEvent event) {
        captured.add(event);
      }

      @Override
      public void close() {
      }

      @Override
      public boolean requiresLayout() {
        return false;
      }
    };
    logger.addAppender(appender);
    try {
      HttpResponse<String> response = head(path);
      // LOG.warn(...) runs synchronously on the request-handling thread, strictly after the
      // response is already flushed to the client socket; poll briefly so a slow-to-log event
      // is not missed, but return as soon as one shows up.
      long deadlineNanos = System.nanoTime() + Duration.ofMillis(500).toNanos();
      while (captured.isEmpty() && System.nanoTime() < deadlineNanos) {
        Thread.sleep(10);
      }
      Assert.assertTrue(
          "expected no server-side error log from the HEAD handler, but got: " + captured,
          captured.isEmpty());
      return response;
    } finally {
      logger.removeAppender(appender);
    }
  }

  private HttpResponse<String> delete(String path) throws Exception {
    HttpClient client = HttpClient.newBuilder().connectTimeout(HTTP_TIMEOUT).build();
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + server.boundPort() + path))
        .timeout(HTTP_TIMEOUT)
        .DELETE()
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
