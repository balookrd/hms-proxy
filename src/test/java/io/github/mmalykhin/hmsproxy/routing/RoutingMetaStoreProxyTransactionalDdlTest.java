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

public class RoutingMetaStoreProxyTransactionalDdlTest {
  @Test
  public void transactionalDdlGuardBlocksCreateTableForMatchingClientAddress() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    RoutingMetaStoreProxy handler = guardedHandler(backendCalls, new AtomicReference<>(), "10.20.0.0/16");
    Table table = table("catalog1__sales", "events", Map.of("transactional", "true"));
    table.setTableType("MANAGED_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);
    String previousRemoteAddress = ClientRequestContext.setRemoteAddress("10.20.1.15");
    try {
      MetaException error = Assert.assertThrows(MetaException.class, () -> handler.invoke(null, method, new Object[] {table}));

      Assert.assertTrue(error.getMessage().contains("create_table"));
      Assert.assertEquals(0, backendCalls.get());
    } finally {
      ClientRequestContext.restoreRemoteAddress(previousRemoteAddress);
    }
  }

  @Test
  public void transactionalDdlGuardAllowsCreateTableForNonMatchingClientAddress() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    AtomicReference<Table> capturedTable = new AtomicReference<>();
    RoutingMetaStoreProxy handler = guardedHandler(backendCalls, capturedTable, "10.20.0.0/16");
    Table table = table("catalog1__sales", "events", Map.of("transactional", "true"));
    table.setTableType("MANAGED_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);
    String previousRemoteAddress = ClientRequestContext.setRemoteAddress("192.168.10.5");
    try {
      handler.invoke(null, method, new Object[] {table});

      Assert.assertEquals(1, backendCalls.get());
      Assert.assertEquals("sales", capturedTable.get().getDbName());
    } finally {
      ClientRequestContext.restoreRemoteAddress(previousRemoteAddress);
    }
  }

  @Test
  public void transactionalDdlGuardBlocksCreateTableWithEnvironmentContext() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    RoutingMetaStoreProxy handler = guardedHandler(backendCalls, new AtomicReference<>(), "10.20.0.0/16");
    Table table = table("catalog1__sales", "events", Map.of("transactional", "true"));
    table.setTableType("MANAGED_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod(
        "create_table_with_environment_context", Table.class, EnvironmentContext.class);
    String previousRemoteAddress = ClientRequestContext.setRemoteAddress("10.20.1.15");
    try {
      MetaException error = Assert.assertThrows(
          MetaException.class,
          () -> handler.invoke(null, method, new Object[] {table, new EnvironmentContext()}));

      Assert.assertTrue(error.getMessage().contains("create_table_with_environment_context"));
      Assert.assertEquals(0, backendCalls.get());
    } finally {
      ClientRequestContext.restoreRemoteAddress(previousRemoteAddress);
    }
  }

  @Test
  public void transactionalDdlGuardBlocksCreateTableWithConstraints() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    RoutingMetaStoreProxy handler = guardedHandler(backendCalls, new AtomicReference<>(), "10.20.0.0/16");
    Table table = table("catalog1__sales", "events", Map.of("transactional", "true"));
    table.setTableType("MANAGED_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod(
        "create_table_with_constraints",
        Table.class, List.class, List.class, List.class, List.class, List.class, List.class);
    String previousRemoteAddress = ClientRequestContext.setRemoteAddress("10.20.1.15");
    try {
      MetaException error = Assert.assertThrows(
          MetaException.class,
          () -> handler.invoke(null, method, new Object[] {
              table, List.of(), List.of(), List.of(), List.of(), List.of(), List.of()}));

      Assert.assertTrue(error.getMessage().contains("create_table_with_constraints"));
      Assert.assertEquals(0, backendCalls.get());
    } finally {
      ClientRequestContext.restoreRemoteAddress(previousRemoteAddress);
    }
  }

  @Test
  public void transactionalDdlRewriteAppliesToCreateTableWithEnvironmentContext() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    AtomicReference<Table> capturedTable = new AtomicReference<>();
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))))
        .compatibility(new CompatibilityConfig(false))
        .transactionalDdlGuard(new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.REWRITE_TRANSACTIONAL_TO_EXTERNAL, List.of()))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    BackendInvocationSession session = newSession((proxy, method, args) -> {
      backendCalls.incrementAndGet();
      if (args != null) {
        for (Object argument : args) {
          if (argument instanceof Table table) {
            capturedTable.set(table);
          }
        }
      }
      return null;
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Table table = table("catalog1__sales", "events", Map.of("transactional", "true"));
    table.setTableType("MANAGED_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod(
        "create_table_with_environment_context", Table.class, EnvironmentContext.class);

    handler.invoke(null, method, new Object[] {table, new EnvironmentContext()});

    Assert.assertEquals(1, backendCalls.get());
    Assert.assertEquals("EXTERNAL_TABLE", capturedTable.get().getTableType());
    Assert.assertEquals("TRUE", capturedTable.get().getParameters().get("EXTERNAL"));
    Assert.assertFalse(capturedTable.get().getParameters().containsKey("transactional"));
  }

  @Test
  public void transactionalDdlGuardBlocksAlterTableWhenTransactionalPropertiesArePresent() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    RoutingMetaStoreProxy handler = guardedHandler(backendCalls, new AtomicReference<>(), "10.10.10.10");
    Table table = table("catalog1__sales", "events", Map.of("transactional_properties", "insert_only"));
    table.setTableType("MANAGED_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod(
        "alter_table_with_environment_context",
        String.class,
        String.class,
        Table.class,
        EnvironmentContext.class);
    String previousRemoteAddress = ClientRequestContext.setRemoteAddress("10.10.10.10");
    try {
      MetaException error = Assert.assertThrows(
          MetaException.class,
          () -> handler.invoke(null, method, new Object[] {"catalog1__sales", "events", table, new EnvironmentContext()}));

      Assert.assertTrue(error.getMessage().contains("alter_table_with_environment_context"));
      Assert.assertEquals(0, backendCalls.get());
    } finally {
      ClientRequestContext.restoreRemoteAddress(previousRemoteAddress);
    }
  }

  @Test
  public void transactionalDdlGuardWithoutAddressListAppliesToAllClients() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))))
        .compatibility(new CompatibilityConfig(false))
        .transactionalDdlGuard(new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.REJECT_TRANSACTIONAL, List.of()))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    BackendInvocationSession session = newSession((proxy, method, args) -> {
      backendCalls.incrementAndGet();
      return null;
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Table table = table("catalog1__sales", "events", Map.of("transactional", "true"));
    table.setTableType("MANAGED_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);

    MetaException error = Assert.assertThrows(MetaException.class, () -> handler.invoke(null, method, new Object[] {table}));

    Assert.assertTrue(error.getMessage().contains("create_table"));
    Assert.assertEquals(0, backendCalls.get());
  }

  @Test
  public void transactionalDdlRewriteChangesManagedTransactionalTableToExternal() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    AtomicReference<Table> capturedTable = new AtomicReference<>();
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))))
        .compatibility(new CompatibilityConfig(false))
        .transactionalDdlGuard(new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.REWRITE_TRANSACTIONAL_TO_EXTERNAL, List.of("10.20.0.0/16")))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    BackendInvocationSession session = newSession((proxy, method, args) -> {
      backendCalls.incrementAndGet();
      if (args != null) {
        for (Object argument : args) {
          if (argument instanceof Table table) {
            capturedTable.set(table);
          }
        }
      }
      return null;
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Table table = table("catalog1__sales", "events", Map.of(
        "transactional", "true",
        "transactional_properties", "insert_only",
        "owner", "etl"));
    table.setTableType("MANAGED_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);
    String previousRemoteAddress = ClientRequestContext.setRemoteAddress("10.20.1.15");
    try {
      handler.invoke(null, method, new Object[] {table});

      Assert.assertEquals(1, backendCalls.get());
      Assert.assertEquals("EXTERNAL_TABLE", capturedTable.get().getTableType());
      Assert.assertEquals("TRUE", capturedTable.get().getParameters().get("EXTERNAL"));
      Assert.assertEquals("true", capturedTable.get().getParameters().get("external.table.purge"));
      Assert.assertEquals("etl", capturedTable.get().getParameters().get("owner"));
      Assert.assertFalse(capturedTable.get().getParameters().containsKey("transactional"));
      Assert.assertFalse(capturedTable.get().getParameters().containsKey("transactional_properties"));
    } finally {
      ClientRequestContext.restoreRemoteAddress(previousRemoteAddress);
    }
  }

  @Test
  public void transactionalDdlRewriteWithoutAddressListAppliesToAllClients() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    AtomicReference<Table> capturedTable = new AtomicReference<>();
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))))
        .compatibility(new CompatibilityConfig(false))
        .transactionalDdlGuard(new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.REWRITE_TRANSACTIONAL_TO_EXTERNAL, List.of()))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    BackendInvocationSession session = newSession((proxy, method, args) -> {
      backendCalls.incrementAndGet();
      if (args != null) {
        for (Object argument : args) {
          if (argument instanceof Table table) {
            capturedTable.set(table);
          }
        }
      }
      return null;
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Table table = table("catalog1__sales", "events", Map.of("transactional", "true"));
    table.setTableType("MANAGED_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);

    handler.invoke(null, method, new Object[] {table});

    Assert.assertEquals(1, backendCalls.get());
    Assert.assertEquals("EXTERNAL_TABLE", capturedTable.get().getTableType());
  }

  @Test
  public void transactionalDdlRejectDoesNotApplyToExternalTables() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    AtomicReference<Table> capturedTable = new AtomicReference<>();
    RoutingMetaStoreProxy handler = guardedHandler(backendCalls, capturedTable, "10.20.0.0/16");
    Table table = table("catalog1__sales", "events", Map.of("transactional", "true"));
    table.setTableType("EXTERNAL_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);
    String previousRemoteAddress = ClientRequestContext.setRemoteAddress("10.20.1.15");
    try {
      handler.invoke(null, method, new Object[] {table});

      Assert.assertEquals(1, backendCalls.get());
      Assert.assertEquals("EXTERNAL_TABLE", capturedTable.get().getTableType());
      Assert.assertEquals("true", capturedTable.get().getParameters().get("transactional"));
    } finally {
      ClientRequestContext.restoreRemoteAddress(previousRemoteAddress);
    }
  }

  @Test
  public void transactionalDdlRewriteDoesNotApplyToExternalTables() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    AtomicReference<Table> capturedTable = new AtomicReference<>();
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))))
        .compatibility(new CompatibilityConfig(false))
        .transactionalDdlGuard(new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.REWRITE_TRANSACTIONAL_TO_EXTERNAL, List.of("10.20.0.0/16")))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    BackendInvocationSession session = newSession((proxy, method, args) -> {
      backendCalls.incrementAndGet();
      if (args != null) {
        for (Object argument : args) {
          if (argument instanceof Table currentTable) {
            capturedTable.set(currentTable);
          }
        }
      }
      return null;
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Table table = table("catalog1__sales", "events", Map.of("transactional", "true"));
    table.setTableType("EXTERNAL_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);
    String previousRemoteAddress = ClientRequestContext.setRemoteAddress("10.20.1.15");
    try {
      handler.invoke(null, method, new Object[] {table});

      Assert.assertEquals(1, backendCalls.get());
      Assert.assertEquals("EXTERNAL_TABLE", capturedTable.get().getTableType());
      Assert.assertEquals("true", capturedTable.get().getParameters().get("transactional"));
      Assert.assertFalse(capturedTable.get().getParameters().containsKey("external.table.purge"));
    } finally {
      ClientRequestContext.restoreRemoteAddress(previousRemoteAddress);
    }
  }

  @Test
  public void transactionalDdlRewriteToNonTransactionalStripsTransactionalParams() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    AtomicReference<Table> capturedTable = new AtomicReference<>();
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))))
        .compatibility(new CompatibilityConfig(false))
        .transactionalDdlGuard(new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.REWRITE_TO_NON_TRANSACTIONAL, List.of()))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    BackendInvocationSession session = newSession((proxy, method, args) -> {
      backendCalls.incrementAndGet();
      if (args != null) {
        for (Object argument : args) {
          if (argument instanceof Table table) {
            capturedTable.set(table);
          }
        }
      }
      return null;
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Table table = table("catalog1__sales", "events", Map.of(
        "transactional", "true",
        "transactional_properties", "insert_only",
        "owner", "etl"));
    table.setTableType("MANAGED_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);

    handler.invoke(null, method, new Object[] {table});

    Assert.assertEquals(1, backendCalls.get());
    Assert.assertEquals("MANAGED_TABLE", capturedTable.get().getTableType());
    Assert.assertFalse(capturedTable.get().getParameters().containsKey("transactional"));
    Assert.assertFalse(capturedTable.get().getParameters().containsKey("transactional_properties"));
    Assert.assertEquals("etl", capturedTable.get().getParameters().get("owner"));
    Assert.assertFalse(capturedTable.get().getParameters().containsKey("EXTERNAL"));
    Assert.assertFalse(capturedTable.get().getParameters().containsKey("external.table.purge"));
  }

  @Test
  public void transactionalDdlRewriteToNonTransactionalDoesNotApplyToNonTransactionalManagedTables() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    AtomicReference<Table> capturedTable = new AtomicReference<>();
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))))
        .compatibility(new CompatibilityConfig(false))
        .transactionalDdlGuard(new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.REWRITE_TO_NON_TRANSACTIONAL, List.of()))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    BackendInvocationSession session = newSession((proxy, method, args) -> {
      backendCalls.incrementAndGet();
      if (args != null) {
        for (Object argument : args) {
          if (argument instanceof Table table) {
            capturedTable.set(table);
          }
        }
      }
      return null;
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Table table = table("catalog1__sales", "events", Map.of("owner", "etl"));
    table.setTableType("MANAGED_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);

    handler.invoke(null, method, new Object[] {table});

    Assert.assertEquals(1, backendCalls.get());
    Assert.assertEquals("MANAGED_TABLE", capturedTable.get().getTableType());
    Assert.assertEquals("etl", capturedTable.get().getParameters().get("owner"));
  }

  @Test
  public void transactionalDdlRewriteToNonTransactionalDoesNotApplyToExternalTables() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    AtomicReference<Table> capturedTable = new AtomicReference<>();
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))))
        .compatibility(new CompatibilityConfig(false))
        .transactionalDdlGuard(new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.REWRITE_TO_NON_TRANSACTIONAL, List.of()))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    BackendInvocationSession session = newSession((proxy, method, args) -> {
      backendCalls.incrementAndGet();
      if (args != null) {
        for (Object argument : args) {
          if (argument instanceof Table table) {
            capturedTable.set(table);
          }
        }
      }
      return null;
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Table table = table("catalog1__sales", "events", Map.of("transactional", "true"));
    table.setTableType("EXTERNAL_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);

    handler.invoke(null, method, new Object[] {table});

    Assert.assertEquals(1, backendCalls.get());
    Assert.assertEquals("EXTERNAL_TABLE", capturedTable.get().getTableType());
    Assert.assertEquals("true", capturedTable.get().getParameters().get("transactional"));
  }

  @Test
  public void transactionalDdlRewriteManagedToExternalAppliesToAllManagedTables() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    AtomicReference<Table> capturedTable = new AtomicReference<>();
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))))
        .compatibility(new CompatibilityConfig(false))
        .transactionalDdlGuard(new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.REWRITE_MANAGED_TO_EXTERNAL, List.of()))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    BackendInvocationSession session = newSession((proxy, method, args) -> {
      backendCalls.incrementAndGet();
      if (args != null) {
        for (Object argument : args) {
          if (argument instanceof Table table) {
            capturedTable.set(table);
          }
        }
      }
      return null;
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Table table = table("catalog1__sales", "events", Map.of("owner", "etl"));
    table.setTableType("MANAGED_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);

    handler.invoke(null, method, new Object[] {table});

    Assert.assertEquals(1, backendCalls.get());
    Assert.assertEquals("EXTERNAL_TABLE", capturedTable.get().getTableType());
    Assert.assertEquals("TRUE", capturedTable.get().getParameters().get("EXTERNAL"));
    Assert.assertEquals("true", capturedTable.get().getParameters().get("external.table.purge"));
    Assert.assertEquals("etl", capturedTable.get().getParameters().get("owner"));
  }

  @Test
  public void transactionalDdlRewriteManagedToExternalAlsoAppliesToTransactionalManagedTables() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    AtomicReference<Table> capturedTable = new AtomicReference<>();
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))))
        .compatibility(new CompatibilityConfig(false))
        .transactionalDdlGuard(new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.REWRITE_MANAGED_TO_EXTERNAL, List.of()))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    BackendInvocationSession session = newSession((proxy, method, args) -> {
      backendCalls.incrementAndGet();
      if (args != null) {
        for (Object argument : args) {
          if (argument instanceof Table table) {
            capturedTable.set(table);
          }
        }
      }
      return null;
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Table table = table("catalog1__sales", "events", Map.of(
        "transactional", "true",
        "transactional_properties", "insert_only",
        "owner", "etl"));
    table.setTableType("MANAGED_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);

    handler.invoke(null, method, new Object[] {table});

    Assert.assertEquals(1, backendCalls.get());
    Assert.assertEquals("EXTERNAL_TABLE", capturedTable.get().getTableType());
    Assert.assertEquals("TRUE", capturedTable.get().getParameters().get("EXTERNAL"));
    Assert.assertEquals("true", capturedTable.get().getParameters().get("external.table.purge"));
    Assert.assertFalse(capturedTable.get().getParameters().containsKey("transactional"));
    Assert.assertFalse(capturedTable.get().getParameters().containsKey("transactional_properties"));
    Assert.assertEquals("etl", capturedTable.get().getParameters().get("owner"));
  }

  @Test
  public void transactionalDdlRewriteManagedToExternalDoesNotApplyToExternalTables() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    AtomicReference<Table> capturedTable = new AtomicReference<>();
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))))
        .compatibility(new CompatibilityConfig(false))
        .transactionalDdlGuard(new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.REWRITE_MANAGED_TO_EXTERNAL, List.of()))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    BackendInvocationSession session = newSession((proxy, method, args) -> {
      backendCalls.incrementAndGet();
      if (args != null) {
        for (Object argument : args) {
          if (argument instanceof Table table) {
            capturedTable.set(table);
          }
        }
      }
      return null;
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Table table = table("catalog1__sales", "events", Map.of("owner", "etl"));
    table.setTableType("EXTERNAL_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);

    handler.invoke(null, method, new Object[] {table});

    Assert.assertEquals(1, backendCalls.get());
    Assert.assertEquals("EXTERNAL_TABLE", capturedTable.get().getTableType());
    Assert.assertFalse(capturedTable.get().getParameters().containsKey("external.table.purge"));
  }

}
