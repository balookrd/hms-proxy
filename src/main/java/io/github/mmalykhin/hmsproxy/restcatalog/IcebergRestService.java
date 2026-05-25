package io.github.mmalykhin.hmsproxy.restcatalog;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
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
 * single shared RoutingHiveCatalog. The MVP supports the proxy's default catalog
 * only; multi-catalog support requires namespace prefix rewriting on both
 * request and response paths (planned for a follow-up step).
 */
public final class IcebergRestService implements AutoCloseable {
  private static final String UNUSED_URI = "thrift://hms-proxy-loopback:0";

  private final ProxyConfig config;
  private final RoutingHiveCatalog catalog;
  private final RESTCatalogAdapter adapter;

  public IcebergRestService(ProxyConfig config, ThriftHiveMetastore.Iface delegate) {
    this.config = Objects.requireNonNull(config, "config");
    Objects.requireNonNull(delegate, "delegate");
    IMetaStoreClient client = RoutingMetaStoreClient.create(delegate);
    this.catalog = new RoutingHiveCatalog(client, new Configuration());
    this.catalog.initialize(
        config.defaultCatalog(),
        Map.of(CatalogProperties.URI, UNUSED_URI));
    this.adapter = new RESTCatalogAdapter(catalog);
  }

  /** True when the prefix segment in the request URL maps to a known catalog we can serve. */
  public boolean supportsPrefix(String prefix) {
    return prefix != null && prefix.equals(config.defaultCatalog());
  }

  /**
   * Returns the GET /v1/config response that Iceberg clients use for discovery.
   * Setting overrides.prefix locks the client to the proxy's default catalog
   * so all subsequent /v1/{prefix}/... requests land here.
   */
  public ConfigResponse loadConfig() {
    return ConfigResponse.builder()
        .withOverride("prefix", config.defaultCatalog())
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
