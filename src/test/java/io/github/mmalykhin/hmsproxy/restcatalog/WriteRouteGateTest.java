package io.github.mmalykhin.hmsproxy.restcatalog;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.RESTCatalogAdapter.Route;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Exercises {@link WriteRouteGate} against a two-catalog fixture: default catalog "hdp", other
 * catalog "apache", separator "__" - the same shape as {@link IcebergRestEndpointIntegrationTest}'s
 * two-catalog setup. A real {@link io.github.mmalykhin.hmsproxy.routing.CatalogRouter} cannot be
 * built here without a live backend: {@code CatalogRouter.open} eagerly connects to each catalog's
 * configured {@code hive.metastore.uris} (verified by inspection of {@code CatalogBackend.open} /
 * {@code BackendRuntime.open}, which construct a real {@code HiveMetaStoreClient} that dials out
 * before returning). So this test drives the gate through the narrower
 * {@code Function<String, String>} collaborator it is defined to accept, hand-computed the same
 * way {@code CatalogRouter.resolveDatabase} would resolve these two catalogs.
 */
public class WriteRouteGateTest {
  private static final String DEFAULT_CATALOG = "hdp";
  private static final String OTHER_CATALOG = "apache";
  private static final String SEPARATOR = "__";

  private static final WriteRouteGate GATE =
      new WriteRouteGate(DEFAULT_CATALOG, WriteRouteGateTest::catalogForNamespace);

  // Every Route constant that is deliberately NOT a write, spelled out one by one so that adding
  // a new constant to RESTCatalogAdapter.Route (e.g. on the next vendored-adapter upgrade) fails
  // everyRouteIsClassifiedAsWriteOrDeliberateNonWrite below until someone classifies it - this is
  // the exact gap that once let COMMIT_TRANSACTION and the view/namespace writes through
  // unguarded (see git history around "Close write-gate hole").
  private static final Set<Route> DELIBERATE_NON_WRITE_ROUTES = EnumSet.of(
      Route.TOKENS, Route.SEPARATE_AUTH_TOKENS_URI, Route.CONFIG,
      Route.LIST_NAMESPACES, Route.NAMESPACE_EXISTS, Route.LOAD_NAMESPACE,
      Route.LIST_TABLES, Route.TABLE_EXISTS, Route.LOAD_TABLE,
      Route.REPORT_METRICS,
      Route.LIST_VIEWS, Route.VIEW_EXISTS, Route.LOAD_VIEW);

  // Hand-maintained Route -> Endpoint correspondence for the write surface: WriteRouteGate gates
  // on org.apache.iceberg.rest.RESTCatalogAdapter.Route, while IcebergRestService advertises
  // discovery through the unrelated org.apache.iceberg.rest.Endpoint type, so nothing in the
  // production code ties the two enumerations together. This table is that tie: it must be kept
  // in sync with both WriteRouteGate.WRITE_ROUTES and IcebergRestService.WRITE_ENDPOINTS whenever
  // either changes, and everyGatedWriteRouteHasAnAdvertisedEndpointAndViceVersa below fails until
  // it is.
  private static final Map<Route, Endpoint> WRITE_ROUTE_TO_ENDPOINT = new EnumMap<>(Route.class);

  static {
    WRITE_ROUTE_TO_ENDPOINT.put(Route.CREATE_TABLE, Endpoint.V1_CREATE_TABLE);
    WRITE_ROUTE_TO_ENDPOINT.put(Route.UPDATE_TABLE, Endpoint.V1_UPDATE_TABLE);
    WRITE_ROUTE_TO_ENDPOINT.put(Route.DROP_TABLE, Endpoint.V1_DELETE_TABLE);
    WRITE_ROUTE_TO_ENDPOINT.put(Route.RENAME_TABLE, Endpoint.V1_RENAME_TABLE);
    WRITE_ROUTE_TO_ENDPOINT.put(Route.REGISTER_TABLE, Endpoint.V1_REGISTER_TABLE);
    WRITE_ROUTE_TO_ENDPOINT.put(Route.CREATE_VIEW, Endpoint.V1_CREATE_VIEW);
    WRITE_ROUTE_TO_ENDPOINT.put(Route.UPDATE_VIEW, Endpoint.V1_UPDATE_VIEW);
    WRITE_ROUTE_TO_ENDPOINT.put(Route.DROP_VIEW, Endpoint.V1_DELETE_VIEW);
    WRITE_ROUTE_TO_ENDPOINT.put(Route.RENAME_VIEW, Endpoint.V1_RENAME_VIEW);
    WRITE_ROUTE_TO_ENDPOINT.put(Route.CREATE_NAMESPACE, Endpoint.V1_CREATE_NAMESPACE);
    WRITE_ROUTE_TO_ENDPOINT.put(Route.UPDATE_NAMESPACE, Endpoint.V1_UPDATE_NAMESPACE);
    WRITE_ROUTE_TO_ENDPOINT.put(Route.DROP_NAMESPACE, Endpoint.V1_DELETE_NAMESPACE);
    WRITE_ROUTE_TO_ENDPOINT.put(Route.COMMIT_TRANSACTION, Endpoint.V1_COMMIT_TRANSACTION);
  }

  @Test
  public void everyGatedWriteRouteHasAnAdvertisedEndpointAndViceVersa() {
    Set<Route> gatedRoutes = WriteRouteGate.writeRoutesForTesting();
    List<Endpoint> advertisedEndpoints = IcebergRestService.writeEndpointsForTesting();
    String updateBothMessage = "WriteRouteGate.WRITE_ROUTES, IcebergRestService.WRITE_ENDPOINTS and "
        + "this test's WRITE_ROUTE_TO_ENDPOINT table have drifted apart - update all three together.";

    Assert.assertEquals(
        "every gated write Route must have exactly one corresponding advertised Endpoint mapped "
            + "in this test's WRITE_ROUTE_TO_ENDPOINT; " + updateBothMessage,
        gatedRoutes, WRITE_ROUTE_TO_ENDPOINT.keySet());

    for (Map.Entry<Route, Endpoint> entry : WRITE_ROUTE_TO_ENDPOINT.entrySet()) {
      Assert.assertTrue(
          "Route." + entry.getKey().name() + " is gated as a write but its mapped Endpoint "
              + entry.getValue() + " is not in IcebergRestService.WRITE_ENDPOINTS; "
              + updateBothMessage,
          advertisedEndpoints.contains(entry.getValue()));
    }
    for (Endpoint endpoint : advertisedEndpoints) {
      Assert.assertTrue(
          "IcebergRestService advertises write endpoint " + endpoint + " that no gated write "
              + "Route maps to in this test's WRITE_ROUTE_TO_ENDPOINT; " + updateBothMessage,
          WRITE_ROUTE_TO_ENDPOINT.containsValue(endpoint));
    }
    Assert.assertEquals(
        "IcebergRestService.WRITE_ENDPOINTS has a duplicate or is missing an entry relative to "
            + "the gated write routes; " + updateBothMessage,
        WRITE_ROUTE_TO_ENDPOINT.size(), advertisedEndpoints.size());
  }

  private static String catalogForNamespace(String externalDbName) {
    if (externalDbName != null && externalDbName.startsWith(OTHER_CATALOG + SEPARATOR)) {
      return OTHER_CATALOG;
    }
    return DEFAULT_CATALOG;
  }

  @Test
  public void everyRouteIsClassifiedAsWriteOrDeliberateNonWrite() {
    Set<Route> writeRoutes = WriteRouteGate.writeRoutesForTesting();
    for (Route route : Route.values()) {
      boolean isWrite = writeRoutes.contains(route);
      boolean isDeliberateNonWrite = DELIBERATE_NON_WRITE_ROUTES.contains(route);
      Assert.assertFalse(
          "Route." + route.name() + " is classified as both a write route (in "
              + "WriteRouteGate.WRITE_ROUTES) and a deliberate non-write route (in this test's "
              + "DELIBERATE_NON_WRITE_ROUTES); fix whichever set is wrong.",
          isWrite && isDeliberateNonWrite);
      Assert.assertTrue(
          "Route." + route.name() + " is not classified anywhere: classify the new route as a "
              + "write (add it to WriteRouteGate.WRITE_ROUTES) or as a deliberate non-write (add "
              + "it to this test's DELIBERATE_NON_WRITE_ROUTES).",
          isWrite || isDeliberateNonWrite);
    }
  }

  @Test
  public void readRoutesAreAlwaysAllowed() {
    Map<String, String> vars = Map.of("namespace", OTHER_CATALOG + SEPARATOR + "default");
    Assert.assertNull(GATE.check(Route.LOAD_TABLE, vars, null));
  }

  @Test
  public void reportMetricsIsNotTreatedAsAWrite() {
    Map<String, String> vars = Map.of("namespace", OTHER_CATALOG + SEPARATOR + "default");
    Assert.assertNull(GATE.check(Route.REPORT_METRICS, vars, null));
  }

  @Test
  public void writeToDefaultCatalogNamespaceIsAllowed() {
    Map<String, String> vars = Map.of("namespace", "default");
    Assert.assertNull(GATE.check(Route.CREATE_TABLE, vars, null));
  }

  @Test
  public void writeToUnresolvableNamespaceIsRefused() {
    // The gate must fail closed: if the resolver cannot say which catalog owns this namespace,
    // it is refused rather than permitted - an unknown catalog is exactly the ambiguous case the
    // synthetic lock shim's lack of conflict checking makes unsafe to guess about.
    WriteRouteGate gateWithUnresolvingCatalogLookup = new WriteRouteGate(DEFAULT_CATALOG, externalDbName -> null);
    Map<String, String> vars = Map.of("namespace", "mystery");
    String refusal = gateWithUnresolvingCatalogLookup.check(Route.CREATE_TABLE, vars, null);
    Assert.assertNotNull("an unresolvable namespace must be refused, not allowed", refusal);
    Assert.assertTrue(refusal, refusal.contains("could not be determined"));
  }

  @Test
  public void writeToFederatedNamespaceUnderDefaultPrefixIsRefused() {
    Map<String, String> vars = Map.of("namespace", OTHER_CATALOG + SEPARATOR + "default");
    String refusal = GATE.check(Route.CREATE_TABLE, vars, null);
    Assert.assertNotNull(refusal);
    Assert.assertTrue(refusal, refusal.contains(OTHER_CATALOG));
    Assert.assertTrue(refusal, refusal.contains(DEFAULT_CATALOG));
  }

  @Test
  public void renameIsCheckedOnBothSourceAndDestination() {
    RenameTableRequest federatedSource = RenameTableRequest.builder()
        .withSource(TableIdentifier.of(OTHER_CATALOG + SEPARATOR + "default", "t1"))
        .withDestination(TableIdentifier.of("default", "t2"))
        .build();
    Assert.assertNotNull(GATE.check(Route.RENAME_TABLE, Map.of(), federatedSource));

    RenameTableRequest federatedDestination = RenameTableRequest.builder()
        .withSource(TableIdentifier.of("default", "t1"))
        .withDestination(TableIdentifier.of(OTHER_CATALOG + SEPARATOR + "default", "t2"))
        .build();
    Assert.assertNotNull(GATE.check(Route.RENAME_TABLE, Map.of(), federatedDestination));
  }

  // --- Path-shaped view/namespace routes: same vars-based lookup as the table routes above. ---

  @Test
  public void createViewToDefaultCatalogNamespaceIsAllowed() {
    Assert.assertNull(GATE.check(Route.CREATE_VIEW, Map.of("namespace", "default"), null));
  }

  @Test
  public void createViewUnderFederatedNamespaceIsRefused() {
    Map<String, String> vars = Map.of("namespace", OTHER_CATALOG + SEPARATOR + "default");
    Assert.assertNotNull(GATE.check(Route.CREATE_VIEW, vars, null));
  }

  @Test
  public void updateViewToDefaultCatalogNamespaceIsAllowed() {
    Assert.assertNull(GATE.check(Route.UPDATE_VIEW, Map.of("namespace", "default"), null));
  }

  @Test
  public void updateViewUnderFederatedNamespaceIsRefused() {
    Map<String, String> vars = Map.of("namespace", OTHER_CATALOG + SEPARATOR + "default");
    Assert.assertNotNull(GATE.check(Route.UPDATE_VIEW, vars, null));
  }

  @Test
  public void dropViewToDefaultCatalogNamespaceIsAllowed() {
    Assert.assertNull(GATE.check(Route.DROP_VIEW, Map.of("namespace", "default"), null));
  }

  @Test
  public void dropViewUnderFederatedNamespaceIsRefused() {
    Map<String, String> vars = Map.of("namespace", OTHER_CATALOG + SEPARATOR + "default");
    Assert.assertNotNull(GATE.check(Route.DROP_VIEW, vars, null));
  }

  @Test
  public void dropNamespaceToDefaultCatalogNamespaceIsAllowed() {
    Assert.assertNull(GATE.check(Route.DROP_NAMESPACE, Map.of("namespace", "default"), null));
  }

  @Test
  public void dropNamespaceUnderFederatedNamespaceIsRefused() {
    Map<String, String> vars = Map.of("namespace", OTHER_CATALOG + SEPARATOR + "default");
    Assert.assertNotNull(GATE.check(Route.DROP_NAMESPACE, vars, null));
  }

  @Test
  public void updateNamespaceToDefaultCatalogNamespaceIsAllowed() {
    Assert.assertNull(GATE.check(Route.UPDATE_NAMESPACE, Map.of("namespace", "default"), null));
  }

  @Test
  public void updateNamespaceUnderFederatedNamespaceIsRefused() {
    Map<String, String> vars = Map.of("namespace", OTHER_CATALOG + SEPARATOR + "default");
    Assert.assertNotNull(GATE.check(Route.UPDATE_NAMESPACE, vars, null));
  }

  // --- Rename-shaped: RENAME_VIEW carries the same RenameTableRequest {source, destination}
  // body shape as RENAME_TABLE, checked above. ---

  @Test
  public void renameViewIsCheckedOnBothSourceAndDestination() {
    RenameTableRequest federatedSource = RenameTableRequest.builder()
        .withSource(TableIdentifier.of(OTHER_CATALOG + SEPARATOR + "default", "v1"))
        .withDestination(TableIdentifier.of("default", "v2"))
        .build();
    Assert.assertNotNull(GATE.check(Route.RENAME_VIEW, Map.of(), federatedSource));

    RenameTableRequest federatedDestination = RenameTableRequest.builder()
        .withSource(TableIdentifier.of("default", "v1"))
        .withDestination(TableIdentifier.of(OTHER_CATALOG + SEPARATOR + "default", "v2"))
        .build();
    Assert.assertNotNull(GATE.check(Route.RENAME_VIEW, Map.of(), federatedDestination));
  }

  // --- Body-shaped: CREATE_NAMESPACE carries its namespace in CreateNamespaceRequest. ---

  @Test
  public void createNamespaceToDefaultCatalogNamespaceIsAllowed() {
    CreateNamespaceRequest request = CreateNamespaceRequest.builder()
        .withNamespace(Namespace.of("default"))
        .build();
    Assert.assertNull(GATE.check(Route.CREATE_NAMESPACE, Map.of(), request));
  }

  @Test
  public void createNamespaceUnderFederatedNamespaceIsRefused() {
    CreateNamespaceRequest request = CreateNamespaceRequest.builder()
        .withNamespace(Namespace.of(OTHER_CATALOG + SEPARATOR + "default"))
        .build();
    Assert.assertNotNull(GATE.check(Route.CREATE_NAMESPACE, Map.of(), request));
  }

  // --- Body-shaped: COMMIT_TRANSACTION carries one or more table changes, each with its own
  // identifier() - every one of them must resolve to the default catalog. ---

  private static UpdateTableRequest tableChange(String db, String table) {
    return UpdateTableRequest.create(
        TableIdentifier.of(db, table), List.<UpdateRequirement>of(), List.<MetadataUpdate>of());
  }

  @Test
  public void commitTransactionAllDefaultCatalogTableChangesIsAllowed() {
    CommitTransactionRequest request = new CommitTransactionRequest(
        List.of(tableChange("default", "t1"), tableChange("default", "t2")));
    Assert.assertNull(GATE.check(Route.COMMIT_TRANSACTION, Map.of(), request));
  }

  @Test
  public void commitTransactionWithOneFederatedTableChangeIsRefusedAsAWhole() {
    // This is the case a per-route allowlist misses: a client bundling one federated-catalog
    // table alongside a default-catalog one into a single atomic commit must have the whole
    // request refused, not just the federated table change silently dropped or allowed through.
    CommitTransactionRequest request = new CommitTransactionRequest(List.of(
        tableChange("default", "t1"),
        tableChange(OTHER_CATALOG + SEPARATOR + "default", "t2")));
    String refusal = GATE.check(Route.COMMIT_TRANSACTION, Map.of(), request);
    Assert.assertNotNull(refusal);
    Assert.assertTrue(refusal, refusal.contains(OTHER_CATALOG));
  }
}
