package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.AbstractBackendAdapter;
import io.github.mmalykhin.hmsproxy.backend.ApacheBackendAdapter;
import io.github.mmalykhin.hmsproxy.backend.BackendAdapter;
import io.github.mmalykhin.hmsproxy.backend.BackendInvocationSession;
import io.github.mmalykhin.hmsproxy.backend.BackendRuntime;
import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.backend.IsolatedInvocationBridge;
import io.github.mmalykhin.hmsproxy.backend.IsolatedMetastoreClient;
import io.github.mmalykhin.hmsproxy.backend.MetastoreApiClassLoader;
import io.github.mmalykhin.hmsproxy.compatibility.MetastoreCompatibility;
import io.github.mmalykhin.hmsproxy.config.server.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.observability.ProxyObservability;
import io.github.mmalykhin.hmsproxy.observability.ProxyRuntimeState;
import io.github.mmalykhin.hmsproxy.security.ClientRequestContext;
import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import io.github.mmalykhin.hmsproxy.federation.FederationLayer;
import io.github.mmalykhin.hmsproxy.security.FrontDoorSecurity;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.HeartbeatRequest;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import io.github.mmalykhin.hmsproxy.config.routing.AdaptiveTimeoutConfig;
import io.github.mmalykhin.hmsproxy.config.routing.BackendConfig;
import io.github.mmalykhin.hmsproxy.config.routing.BackendStatePollingConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogExposureMode;
import io.github.mmalykhin.hmsproxy.config.routing.CircuitBreakerConfig;
import io.github.mmalykhin.hmsproxy.config.compatibility.CompatibilityConfig;
import io.github.mmalykhin.hmsproxy.config.routing.DegradedRoutingPolicy;
import io.github.mmalykhin.hmsproxy.config.catalog.ExternalTableDropPurgeMode;
import io.github.mmalykhin.hmsproxy.config.catalog.ExternalTableLocationRewriteMode;
import io.github.mmalykhin.hmsproxy.config.federation.FederationConfig;
import io.github.mmalykhin.hmsproxy.config.server.FrontendProfile;
import io.github.mmalykhin.hmsproxy.config.routing.HedgedReadConfig;
import io.github.mmalykhin.hmsproxy.config.routing.LatencyRoutingConfig;
import io.github.mmalykhin.hmsproxy.config.management.ManagementConfig;
import io.github.mmalykhin.hmsproxy.config.ratelimit.RateLimitConfig;
import io.github.mmalykhin.hmsproxy.config.ratelimit.RateLimitPolicyConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityMode;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreMode;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreZooKeeperConfig;
import io.github.mmalykhin.hmsproxy.config.ddlguard.TransactionalDdlGuardConfig;
import io.github.mmalykhin.hmsproxy.config.ddlguard.TransactionalDdlGuardMode;
import io.github.mmalykhin.hmsproxy.config.catalog.ViewTextRewriteMode;
import static io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreProxyTestSupport.*;

public class RoutingMetaStoreProxyAccessControlTest {
  @Test
  public void readOnlyCatalogBlocksWriteOperations() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    RoutingMetaStoreProxy handler = accessModeHandler(
        CatalogAccessMode.READ_ONLY,
        List.of(),
        backendCalls,
        new AtomicReference<>());
    Table table = table("catalog1__sales", "events", Map.of());
    table.setTableType("MANAGED_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);

    MetaException error = Assert.assertThrows(MetaException.class, () -> handler.invoke(null, method, new Object[] {table}));

    Assert.assertTrue(error.getMessage().contains("READ_ONLY"));
    Assert.assertEquals(0, backendCalls.get());
  }

  @Test
  public void readOnlyCatalogBlocksRefreshPrivileges() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    RoutingMetaStoreProxy handler = accessModeHandler(
        CatalogAccessMode.READ_ONLY,
        List.of(),
        backendCalls,
        new AtomicReference<>());
    Method method = ThriftHiveMetastore.Iface.class.getMethod(
        "refresh_privileges",
        org.apache.hadoop.hive.metastore.api.HiveObjectRef.class,
        String.class,
        org.apache.hadoop.hive.metastore.api.GrantRevokePrivilegeRequest.class);

    MetaException error = Assert.assertThrows(
        MetaException.class,
        () -> handler.invoke(null, method, new Object[] {
            new org.apache.hadoop.hive.metastore.api.HiveObjectRef(),
            "admin",
            new org.apache.hadoop.hive.metastore.api.GrantRevokePrivilegeRequest()}));

    Assert.assertTrue(error.getMessage().contains("READ_ONLY"));
    Assert.assertEquals(0, backendCalls.get());
  }

  @Test
  public void whitelistCatalogAllowsWritesForWhitelistedDatabases() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    AtomicReference<Table> capturedTable = new AtomicReference<>();
    RoutingMetaStoreProxy handler = accessModeHandler(
        CatalogAccessMode.READ_WRITE_DB_WHITELIST,
        List.of("sales"),
        backendCalls,
        capturedTable);
    Table table = table("catalog1__sales", "events", Map.of());
    table.setTableType("MANAGED_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);

    handler.invoke(null, method, new Object[] {table});

    Assert.assertEquals(1, backendCalls.get());
    Assert.assertEquals("sales", capturedTable.get().getDbName());
  }

  @Test
  public void whitelistCatalogBlocksWritesForNonWhitelistedDatabases() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    RoutingMetaStoreProxy handler = accessModeHandler(
        CatalogAccessMode.READ_WRITE_DB_WHITELIST,
        List.of("sales"),
        backendCalls,
        new AtomicReference<>());
    Table table = table("catalog1__finance", "events", Map.of());
    table.setTableType("MANAGED_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);

    MetaException error = Assert.assertThrows(MetaException.class, () -> handler.invoke(null, method, new Object[] {table}));

    Assert.assertTrue(error.getMessage().contains("not allowed"));
    Assert.assertEquals(0, backendCalls.get());
  }

  @Test
  public void transactionalDdlGuardDecidesPerRequestWhenAThreadServesSeveralClients() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    RoutingMetaStoreProxy handler = guardedHandler(backendCalls, new AtomicReference<>(), "10.20.0.0/16");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);

    // Same worker thread, guarded client first.
    String previousRemoteAddress = ClientRequestContext.setRemoteAddress("10.20.1.15");
    try {
      Assert.assertThrows(
          MetaException.class,
          () -> handler.invoke(null, method, new Object[] {transactionalTable()}));
      Assert.assertEquals(0, backendCalls.get());
    } finally {
      ClientRequestContext.restoreRemoteAddress(previousRemoteAddress);
    }

    // The next client on that thread is outside the guarded CIDR and must not inherit the verdict.
    previousRemoteAddress = ClientRequestContext.setRemoteAddress("192.168.10.5");
    try {
      handler.invoke(null, method, new Object[] {transactionalTable()});

      Assert.assertEquals(1, backendCalls.get());
    } finally {
      ClientRequestContext.restoreRemoteAddress(previousRemoteAddress);
    }
  }

  private static Table transactionalTable() {
    Table table = table("catalog1__sales", "events", Map.of("transactional", "true"));
    table.setTableType("MANAGED_TABLE");
    return table;
  }

  @Test
  public void getAllDatabasesFiltersHiddenDatabasesByExposurePolicy() throws Throwable {
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of(
            "catalog1",
            catalogConfigWithExposure(
                "catalog1",
                "c1",
                null,
                null,
                CatalogExposureMode.DENY_BY_DEFAULT,
                List.of("sales", "finance"),
                Map.of(),
                Map.of("hive.metastore.uris", "thrift://one"))))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    BackendInvocationSession session = newSession((proxy, method, args) -> {
      if ("get_all_databases".equals(method.getName())) {
        return List.of("sales", "hidden", "Finance");
      }
      throw new UnsupportedOperationException(method.getName());
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("get_all_databases");

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) handler.invoke(null, method, new Object[0]);

    Assert.assertEquals(List.of("sales", "Finance"), result);
  }

  @Test
  public void getTableRejectsHiddenTableByExposurePolicyWithoutBackendCall() throws Throwable {
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of(
            "catalog1",
            catalogConfigWithExposure(
                "catalog1",
                "c1",
                null,
                null,
                CatalogExposureMode.DENY_BY_DEFAULT,
                List.of(),
                Map.of("sales", List.of("orders")),
                Map.of("hive.metastore.uris", "thrift://one"))))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    AtomicInteger backendCalls = new AtomicInteger();
    BackendInvocationSession session = newSession((proxy, method, args) -> {
      backendCalls.incrementAndGet();
      return table("sales", "secret", Map.of());
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("get_table", String.class, String.class);

    NoSuchObjectException error = Assert.assertThrows(
        NoSuchObjectException.class,
        () -> handler.invoke(null, method, new Object[] {"sales", "secret"}));

    Assert.assertTrue(error.getMessage().contains("not exposed"));
    Assert.assertEquals(0, backendCalls.get());
  }

  @Test
  public void getAllTablesFiltersHiddenTablesByExposurePolicyCaseInsensitively() throws Throwable {
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of(
            "catalog1",
            catalogConfigWithExposure(
                "catalog1",
                "c1",
                null,
                null,
                CatalogExposureMode.DENY_BY_DEFAULT,
                List.of(),
                Map.of("sales", List.of("orders")),
                Map.of("hive.metastore.uris", "thrift://one"))))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    BackendInvocationSession session = newSession((proxy, method, args) -> {
      if ("get_all_tables".equals(method.getName())) {
        return List.of("Orders", "secret");
      }
      throw new UnsupportedOperationException(method.getName());
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("get_all_tables", String.class);

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) handler.invoke(null, method, new Object[] {"sales"});

    Assert.assertEquals(List.of("Orders"), result);
  }

  @Test
  public void getTableMetaFiltersHiddenTablesByExposurePolicy() throws Throwable {
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of(
            "catalog1",
            catalogConfigWithExposure(
                "catalog1",
                "c1",
                null,
                null,
                CatalogExposureMode.DENY_BY_DEFAULT,
                List.of(),
                Map.of("sales", List.of("orders")),
                Map.of("hive.metastore.uris", "thrift://one"))))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    BackendInvocationSession session = newSession((proxy, method, args) -> {
      if ("get_table_meta".equals(method.getName())) {
        return List.of(
            new TableMeta("sales", "Orders", "MANAGED_TABLE"),
            new TableMeta("sales", "secret", "MANAGED_TABLE"));
      }
      throw new UnsupportedOperationException(method.getName());
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Method method =
        ThriftHiveMetastore.Iface.class.getMethod("get_table_meta", String.class, String.class, List.class);

    @SuppressWarnings("unchecked")
    List<TableMeta> result =
        (List<TableMeta>) handler.invoke(null, method, new Object[] {"sales", "*", List.of()});

    Assert.assertEquals(1, result.size());
    Assert.assertEquals("Orders", result.get(0).getTableName());
  }

}
