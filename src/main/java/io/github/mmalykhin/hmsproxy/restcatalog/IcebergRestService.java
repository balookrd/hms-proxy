package io.github.mmalykhin.hmsproxy.restcatalog;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.responses.ConfigResponse;

/**
 * Bridges Iceberg REST calls to the proxy's ThriftHiveMetastore.Iface via a
 * RoutingHiveCatalog. Each instance serves a single catalog prefix and is
 * looked up through the IcebergRestServices registry. The default catalog's
 * service exposes the federated view as-is, with no name translation; every
 * other catalog's service is given a CatalogNameTranslation so REST clients
 * see that catalog's internal database names instead of the federated ones.
 * Only the default catalog's service advertises (and, via {@link WriteRouteGate},
 * actually allows) table writes - every other catalog is discovery-only, matching
 * the synthetic lock shim that backs its writes with no real conflict checking.
 */
public final class IcebergRestService implements AutoCloseable {
  private static final String UNUSED_URI = "thrift://hms-proxy-loopback:0";

  /**
   * Read routes every catalog's front door serves. Kept in sync by hand with the
   * dispatch table in {@link IcebergHttpHandler}: every entry here must answer for
   * real, and every route the handler serves must be listed here so REST clients
   * (which use this list for discovery) do not fail to learn about a read route we
   * do have, one request at a time.
   */
  private static final List<Endpoint> READ_ENDPOINTS = List.of(
      Endpoint.V1_LIST_NAMESPACES,
      Endpoint.V1_LOAD_NAMESPACE,
      Endpoint.V1_NAMESPACE_EXISTS,
      Endpoint.V1_LIST_TABLES,
      Endpoint.V1_LOAD_TABLE,
      Endpoint.V1_TABLE_EXISTS,
      Endpoint.V1_LIST_VIEWS,
      Endpoint.V1_LOAD_VIEW,
      Endpoint.V1_VIEW_EXISTS);

  /**
   * Table write routes phase 5a actually implements: only the default catalog's tables are
   * backed by a real HMS lock, so only the default catalog's service advertises these. {@link
   * WriteRouteGate} additionally gates view, namespace and transaction-commit writes for safety,
   * but this phase does not implement any of those as working features, so they must stay off
   * this list - advertising them would promise a capability the proxy does not deliver.
   */
  private static final List<Endpoint> WRITE_ENDPOINTS = List.of(
      Endpoint.V1_CREATE_TABLE,
      Endpoint.V1_UPDATE_TABLE,
      Endpoint.V1_DELETE_TABLE,
      Endpoint.V1_RENAME_TABLE,
      Endpoint.V1_REGISTER_TABLE);

  private static final List<Endpoint> DEFAULT_CATALOG_ENDPOINTS =
      Stream.concat(READ_ENDPOINTS.stream(), WRITE_ENDPOINTS.stream()).toList();

  private final String catalogName;
  private final RoutingHiveCatalog catalog;
  private final RESTCatalogAdapter adapter;
  private final WriteRouteGate writeGate;
  private final List<Endpoint> servedEndpoints;

  public IcebergRestService(
      String catalogName,
      ThriftHiveMetastore.Iface delegate,
      CatalogNameTranslation translationOrNull,
      String defaultCatalogName,
      Function<String, String> catalogForExternalDb) {
    this.catalogName = Objects.requireNonNull(catalogName, "catalogName");
    Objects.requireNonNull(delegate, "delegate");
    Objects.requireNonNull(defaultCatalogName, "defaultCatalogName");
    Objects.requireNonNull(catalogForExternalDb, "catalogForExternalDb");
    IMetaStoreClient client = RoutingMetaStoreClient.create(delegate, translationOrNull);
    this.catalog = new RoutingHiveCatalog(client, new Configuration());
    this.catalog.initialize(catalogName, Map.of(CatalogProperties.URI, UNUSED_URI));
    this.adapter = new RESTCatalogAdapter(catalog);
    // A name-translated (non-default) service only ever exposes its own databases, so its own
    // local namespace values must be mapped back to this catalog's federated external name
    // before going through the shared resolver - which then always answers with this same
    // (non-default) catalog, and the gate below refuses every write for it. The default
    // catalog's own service has no translation, so its namespace values are already federated
    // external names and go through the shared resolver unchanged.
    Function<String, String> catalogForNamespace = translationOrNull == null
        ? catalogForExternalDb
        : localDb -> catalogForExternalDb.apply(translationOrNull.toExternal(localDb));
    this.writeGate = new WriteRouteGate(defaultCatalogName, catalogForNamespace);
    this.servedEndpoints = catalogName.equals(defaultCatalogName) ? DEFAULT_CATALOG_ENDPOINTS : READ_ENDPOINTS;
  }

  public String catalogName() {
    return catalogName;
  }

  WriteRouteGate writeGate() {
    return writeGate;
  }

  /**
   * Returns the GET /v1/config response that Iceberg clients use for discovery.
   * Setting overrides.prefix locks the client to this service's catalog so all
   * subsequent /v1/{prefix}/... requests land here.
   */
  public ConfigResponse loadConfig() {
    return ConfigResponse.builder()
        .withOverride("prefix", catalogName)
        .withEndpoints(servedEndpoints)
        .build();
  }

  public <T extends RESTResponse> T dispatch(
      RESTCatalogAdapter.Route route,
      Map<String, String> vars,
      Object body,
      Class<T> responseType) {
    return adapter.handleRequest(route, vars, body, responseType);
  }

  @Override
  public void close() throws IOException {
    catalog.close();
  }
}
