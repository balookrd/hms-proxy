package io.github.mmalykhin.hmsproxy.restcatalog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.restcatalog.RestCatalogConfig;
import io.github.mmalykhin.hmsproxy.config.restcatalog.RestCatalogPurgeMode;
import io.github.mmalykhin.hmsproxy.config.routing.BackendConfig;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class IcebergRestServicesTest {
  @Test
  public void registryServesEveryConfiguredCatalog() throws Exception {
    RecordingThriftIface recording = new RecordingThriftIface();
    // Registry wiring only; the write gate itself is covered by WriteRouteGateTest and
    // IcebergRestEndpointIntegrationTest, so any resolver matching this fixture's separator works.
    Function<String, String> catalogForExternalDb =
        externalDbName -> externalDbName != null && externalDbName.startsWith("apache.") ? "apache" : "hdp";
    try (IcebergRestServices services =
        IcebergRestServices.open(buildTwoCatalogConfig(), recording.iface, catalogForExternalDb)) {
      assertEquals("hdp", services.defaultPrefix());
      assertNotNull(services.serviceFor("hdp"));
      assertNotNull(services.serviceFor("apache"));
      assertNull(services.serviceFor("nope"));
      assertEquals("hdp", services.byWarehouse(null).catalogName());
      assertEquals("apache", services.byWarehouse("apache").catalogName());
      assertNull(services.byWarehouse("nope"));
    }
  }

  @Test
  public void theRestCatalogGetsHiveEngineDescriptorsWithoutMutatingTheBackendConfiguration()
      throws Exception {
    RecordingThriftIface recording = new RecordingThriftIface();
    Configuration backendConf = new Configuration(false);

    // The positive half - that the per-catalog Configuration actually passed to
    // IcebergRestService carries iceberg.engine.hive.enabled=true - has no accessor in this
    // file's fixtures to assert against; it is covered by the stand run in Task 2 instead.
    try (IcebergRestServices services = IcebergRestServices.open(
        buildTwoCatalogConfig(), recording.iface, externalDbName -> "hdp", catalog -> backendConf)) {
      assertNotNull(services.serviceFor("hdp"));
    }

    assertNull("the backend's shared Configuration must not be written to",
        backendConf.get("iceberg.engine.hive.enabled"));
  }

  private static ProxyConfig buildTwoCatalogConfig() {
    return ProxyConfig.builder()
        .server(new ServerConfig("hms-proxy-test", "127.0.0.1", 9083, 1, 4))
        .catalogDbSeparator(".")
        .defaultCatalog("hdp")
        .catalogs(Map.of(
            "hdp", new CatalogConfig(
                "hdp",
                null,
                null,
                false,
                CatalogAccessMode.READ_WRITE,
                List.of(),
                null,
                null,
                Map.of("hive.metastore.uris", "thrift://hms-test:9083")),
            "apache", new CatalogConfig(
                "apache",
                null,
                null,
                false,
                CatalogAccessMode.READ_WRITE,
                List.of(),
                null,
                null,
                Map.of("hive.metastore.uris", "thrift://hms-test:9084"))))
        .backend(new BackendConfig(Map.of()))
        .restCatalog(new RestCatalogConfig(
            true, "127.0.0.1", 0, 1, 4, null, null, RestCatalogPurgeMode.ALLOW, List.of(), true))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();
  }
}
