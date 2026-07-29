package io.github.mmalykhin.hmsproxy.routing;

import static io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreProxyTestSupport.catalogConfig;
import static io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreProxyTestSupport.newBackend;
import static io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreProxyTestSupport.newBackendRuntime;
import static io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreProxyTestSupport.newSession;

import io.github.mmalykhin.hmsproxy.backend.ApacheBackendAdapter;
import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityMode;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;
import io.github.mmalykhin.hmsproxy.federation.FederationLayer;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.junit.Assert;
import org.junit.Test;

/**
 * A Hive client's own {@code alter_table} must not roll an Iceberg table's pointer back.
 *
 * <p>The stand caught this as real data loss (TEST-MATRIX section I, "I4 in detail"): a
 * HiveServer2 {@code INSERT} opens with an {@code alter_table_with_environment_context} carrying
 * {@code alterTableOpType=DROPPROPS} and the {@code Table} object it snapshotted when the query
 * was compiled. A metastore applies those parameters wholesale, {@code metadata_location}
 * included, so a REST commit that landed in between is erased - and none of it happens under the
 * Iceberg lock, so nothing serializes the two. The proxy is the only place both paths meet.
 */
public class RoutingMetaStoreProxyIcebergPointerGuardTest {
  private static final String CURRENT = "hdfs://nn/warehouse/sales/events/metadata/00006-current.metadata.json";
  private static final String STALE = "hdfs://nn/warehouse/sales/events/metadata/00005-stale.metadata.json";

  @Test
  public void staleAlterTableKeepsTheMetadataLocationTheMetastoreCurrentlyHolds() throws Throwable {
    AtomicReference<Table> forwarded = new AtomicReference<>();
    ThriftHiveMetastore.Iface handler = newProxy(forwarded);

    Table stale = icebergTable(STALE, null);
    stale.getParameters().put("numRows", "17");
    Method alter = ThriftHiveMetastore.Iface.class.getMethod(
        "alter_table_with_environment_context", String.class, String.class, Table.class, EnvironmentContext.class);

    handler.getClass();
    invoke(handler, alter, "sales", "events", stale, dropPropsContext());

    Assert.assertNotNull("the alter never reached the backend", forwarded.get());
    Assert.assertEquals("a stale alter_table must not move the Iceberg pointer backwards",
        CURRENT, forwarded.get().getParameters().get("metadata_location"));
    Assert.assertEquals("everything the client actually meant to change must still go through",
        "17", forwarded.get().getParameters().get("numRows"));
  }

  @Test
  public void icebergCommitThatBuildsOnTheCurrentPointerIsLeftAlone() throws Throwable {
    AtomicReference<Table> forwarded = new AtomicReference<>();
    ThriftHiveMetastore.Iface handler = newProxy(forwarded);

    // What an Iceberg commit sends: a new metadata file, with the pointer it read as the base.
    String next = "hdfs://nn/warehouse/sales/events/metadata/00007-next.metadata.json";
    Table commit = icebergTable(next, CURRENT);
    Method alter = ThriftHiveMetastore.Iface.class.getMethod(
        "alter_table_with_environment_context", String.class, String.class, Table.class, EnvironmentContext.class);

    invoke(handler, alter, "sales", "events", commit, null);

    Assert.assertEquals("a commit built on the current pointer must pass through untouched",
        next, forwarded.get().getParameters().get("metadata_location"));
  }

  private static Object invoke(ThriftHiveMetastore.Iface handler, Method method, Object... args) throws Throwable {
    return ((RoutingMetaStoreProxy) java.lang.reflect.Proxy.getInvocationHandler(handler))
        .invoke(null, method, args);
  }

  private static ThriftHiveMetastore.Iface newProxy(AtomicReference<Table> forwarded) throws Exception {
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1",
            catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), newSession((proxy, method, args) -> {
          switch (method.getName()) {
            case "get_table":
              return icebergTable(CURRENT, null);
            case "alter_table_with_environment_context":
              forwarded.set((Table) args[2]);
              return null;
            default:
              throw new UnsupportedOperationException(method.getName());
          }
        })));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", backend);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy proxy =
        new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    return (ThriftHiveMetastore.Iface) java.lang.reflect.Proxy.newProxyInstance(
        ThriftHiveMetastore.Iface.class.getClassLoader(),
        new Class<?>[] {ThriftHiveMetastore.Iface.class},
        proxy);
  }

  private static Table icebergTable(String metadataLocation, String previousMetadataLocation) {
    Table table = new Table();
    table.setDbName("sales");
    table.setTableName("events");
    Map<String, String> parameters = new HashMap<>();
    parameters.put("table_type", "ICEBERG");
    parameters.put("metadata_location", metadataLocation);
    if (previousMetadataLocation != null) {
      parameters.put("previous_metadata_location", previousMetadataLocation);
    }
    table.setParameters(parameters);
    return table;
  }

  private static EnvironmentContext dropPropsContext() {
    EnvironmentContext context = new EnvironmentContext();
    context.setProperties(Map.of("DO_NOT_UPDATE_STATS", "true", "alterTableOpType", "DROPPROPS"));
    return context;
  }
}
