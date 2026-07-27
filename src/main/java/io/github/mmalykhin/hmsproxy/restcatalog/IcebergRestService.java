package io.github.mmalykhin.hmsproxy.restcatalog;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.apache.iceberg.rest.RESTCatalogAdapter.HTTPMethod;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;

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
        .build();
  }

  public <T extends RESTResponse> T dispatch(
      HTTPMethod method,
      String relativePath,
      Map<String, String> queryParams,
      Object body,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return adapter.execute(method, relativePath, queryParams, body, responseType, headers, errorHandler);
  }

  @Override
  public void close() throws IOException {
    catalog.close();
  }
}
