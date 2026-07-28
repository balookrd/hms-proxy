package io.github.mmalykhin.hmsproxy.restcatalog;

import java.util.Map;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalogAdapter.Route;
import org.apache.iceberg.rest.requests.RenameTableRequest;
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
}
