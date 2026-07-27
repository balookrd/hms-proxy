package io.github.mmalykhin.hmsproxy.restcatalog;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
 */
public final class IcebergRestService implements AutoCloseable {
  private static final String UNUSED_URI = "thrift://hms-proxy-loopback:0";

  /**
   * Routes this read-only front door actually serves. Kept in sync by hand with the
   * dispatch table in {@link IcebergHttpHandler}: every entry here must answer for
   * real, and every route the handler serves must be listed here so REST clients
   * (which use this list for discovery) do not learn about write support we don't
   * have, or fail to learn about a read route we do have, one request at a time.
   */
  private static final List<Endpoint> SERVED_ENDPOINTS = List.of(
      Endpoint.V1_LIST_NAMESPACES,
      Endpoint.V1_LOAD_NAMESPACE,
      Endpoint.V1_NAMESPACE_EXISTS,
      Endpoint.V1_LIST_TABLES,
      Endpoint.V1_LOAD_TABLE,
      Endpoint.V1_TABLE_EXISTS,
      Endpoint.V1_LIST_VIEWS,
      Endpoint.V1_LOAD_VIEW,
      Endpoint.V1_VIEW_EXISTS);

  private final String catalogName;
  private final RoutingHiveCatalog catalog;
  private final RESTCatalogAdapter adapter;

  public IcebergRestService(
      String catalogName,
      ThriftHiveMetastore.Iface delegate,
      CatalogNameTranslation translationOrNull) {
    this.catalogName = Objects.requireNonNull(catalogName, "catalogName");
    Objects.requireNonNull(delegate, "delegate");
    IMetaStoreClient client = RoutingMetaStoreClient.create(delegate, translationOrNull);
    this.catalog = new RoutingHiveCatalog(client, new Configuration());
    this.catalog.initialize(catalogName, Map.of(CatalogProperties.URI, UNUSED_URI));
    this.adapter = new RESTCatalogAdapter(catalog);
  }

  public String catalogName() {
    return catalogName;
  }

  /**
   * Returns the GET /v1/config response that Iceberg clients use for discovery.
   * Setting overrides.prefix locks the client to this service's catalog so all
   * subsequent /v1/{prefix}/... requests land here.
   */
  public ConfigResponse loadConfig() {
    return ConfigResponse.builder()
        .withOverride("prefix", catalogName)
        .withEndpoints(SERVED_ENDPOINTS)
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
