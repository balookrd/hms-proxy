package io.github.mmalykhin.hmsproxy.restcatalog;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
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

  private static String catalogForNamespace(String externalDbName) {
    if (externalDbName != null && externalDbName.startsWith(OTHER_CATALOG + SEPARATOR)) {
      return OTHER_CATALOG;
    }
    return DEFAULT_CATALOG;
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
