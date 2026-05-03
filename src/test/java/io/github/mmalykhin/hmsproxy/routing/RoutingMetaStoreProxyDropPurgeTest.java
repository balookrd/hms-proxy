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

public class RoutingMetaStoreProxyDropPurgeTest {
  @Test
  public void dropTablePurgesExternalTableAfterSuccessfulBackendDrop() throws Throwable {
    ProxyConfig config = dropPurgeConfig();
    List<String> events = new ArrayList<>();
    RecordingExternalTableDropPurger purger = new RecordingExternalTableDropPurger(events);
    purger.preparedRequest = Optional.of(new ExternalTableDropPurger.PurgeRequest("hdfs://ns-dev3/tmp/external/events"));

    BackendInvocationSession session = newSession((proxy, method, args) -> {
      events.add(method.getName());
      if ("get_table".equals(method.getName())) {
        return externalTable("sales", "events", "hdfs://ns-dev3/tmp/external/events", true);
      }
      if ("drop_table".equals(method.getName())) {
        return null;
      }
      throw new UnsupportedOperationException(method.getName());
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreProxy handler =
        new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null, new ProxyObservability(config), purger);
    Method method = thriftMethod("drop_table");

    handler.invoke(null, method, dropTableArguments(method, "catalog1__sales", "events"));

    Assert.assertEquals(List.of("get_table", "prepare", "drop_table", "purge"), events);
    Assert.assertEquals("sales", purger.preparedTable.get().getDbName());
    Assert.assertEquals("events", purger.preparedTable.get().getTableName());
  }

  @Test
  public void dropTableWithEnvironmentContextPurgesExternalTableAfterSuccessfulBackendDrop() throws Throwable {
    ProxyConfig config = dropPurgeConfig();
    List<String> events = new ArrayList<>();
    RecordingExternalTableDropPurger purger = new RecordingExternalTableDropPurger(events);
    purger.preparedRequest = Optional.of(new ExternalTableDropPurger.PurgeRequest("hdfs://ns-dev3/tmp/external/events"));

    BackendInvocationSession session = newSession((proxy, method, args) -> {
      events.add(method.getName());
      if ("get_table".equals(method.getName())) {
        return externalTable("sales", "events", "hdfs://ns-dev3/tmp/external/events", true);
      }
      if ("drop_table_with_environment_context".equals(method.getName())) {
        return null;
      }
      throw new UnsupportedOperationException(method.getName());
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreProxy handler =
        new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null, new ProxyObservability(config), purger);
    Method method = thriftMethod("drop_table_with_environment_context");

    handler.invoke(null, method, dropTableArguments(method, "catalog1__sales", "events"));

    Assert.assertEquals(List.of("get_table", "prepare", "drop_table_with_environment_context", "purge"), events);
  }

  @Test
  public void dropTableIgnoresBestEffortPurgeFailureAfterSuccessfulBackendDrop() throws Throwable {
    ProxyConfig config = dropPurgeConfig();
    List<String> events = new ArrayList<>();
    RecordingExternalTableDropPurger purger = new RecordingExternalTableDropPurger(events);
    purger.preparedRequest = Optional.of(new ExternalTableDropPurger.PurgeRequest("hdfs://ns-dev3/tmp/external/events"));
    purger.purgeFailure = new java.io.IOException("simulated purge failure");

    BackendInvocationSession session = newSession((proxy, method, args) -> {
      events.add(method.getName());
      if ("get_table".equals(method.getName())) {
        return externalTable("sales", "events", "hdfs://ns-dev3/tmp/external/events", true);
      }
      if ("drop_table".equals(method.getName())) {
        return null;
      }
      throw new UnsupportedOperationException(method.getName());
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreProxy handler =
        new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null, new ProxyObservability(config), purger);
    Method method = thriftMethod("drop_table");

    handler.invoke(null, method, dropTableArguments(method, "catalog1__sales", "events"));

    Assert.assertEquals(List.of("get_table", "prepare", "drop_table", "purge"), events);
    Assert.assertEquals(1, purger.purgeCalls.get());
  }

}
