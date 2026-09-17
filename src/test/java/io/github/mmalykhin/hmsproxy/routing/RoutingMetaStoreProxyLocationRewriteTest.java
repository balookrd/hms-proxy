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
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.AddPartitionsRequest;
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

public class RoutingMetaStoreProxyLocationRewriteTest {
  @Test
  public void externalTableLocationRewriteQualifiesUnqualifiedLocationForNonDefaultCatalog() throws Throwable {
    AtomicReference<Table> capturedTable = new AtomicReference<>();
    RoutingMetaStoreProxy handler = locationRewriteHandler(
        ExternalTableLocationRewriteMode.QUALIFY_UNQUALIFIED,
        null,
        capturedTable);
    Table table = table("catalog2__sales", "events", Map.of("EXTERNAL", "TRUE"));
    table.setTableType("EXTERNAL_TABLE");
    table.setSd(storageDescriptor("/tmp/external/events"));
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);

    handler.invoke(null, method, new Object[] {table});

    Assert.assertEquals("sales", capturedTable.get().getDbName());
    Assert.assertEquals("hdfs://ns-catalog2/tmp/external/events", capturedTable.get().getSd().getLocation());
  }

  @Test
  public void externalTableLocationRewriteMovesFrontendDefaultFsToTargetCatalog() throws Throwable {
    AtomicReference<Table> capturedTable = new AtomicReference<>();
    RoutingMetaStoreProxy handler = locationRewriteHandler(
        ExternalTableLocationRewriteMode.REWRITE_IF_SOURCE_DEFAULT_FS,
        "hdfs://ns-frontend",
        capturedTable);
    Table table = table("catalog2__sales", "events", Map.of("EXTERNAL", "TRUE"));
    table.setTableType("EXTERNAL_TABLE");
    table.setSd(storageDescriptor("hdfs://ns-frontend/tmp/external/events"));
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);

    handler.invoke(null, method, new Object[] {table});

    Assert.assertEquals("hdfs://ns-catalog2/tmp/external/events", capturedTable.get().getSd().getLocation());
  }

  @Test
  public void externalTableLocationRewriteLeavesExplicitForeignHdfsLocationUntouched() throws Throwable {
    AtomicReference<Table> capturedTable = new AtomicReference<>();
    RoutingMetaStoreProxy handler = locationRewriteHandler(
        ExternalTableLocationRewriteMode.REWRITE_IF_SOURCE_DEFAULT_FS,
        "hdfs://ns-frontend",
        capturedTable);
    Table table = table("catalog2__sales", "events", Map.of("EXTERNAL", "TRUE"));
    table.setTableType("EXTERNAL_TABLE");
    table.setSd(storageDescriptor("hdfs://ns-shared/tmp/external/events"));
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);

    handler.invoke(null, method, new Object[] {table});

    Assert.assertEquals("hdfs://ns-shared/tmp/external/events", capturedTable.get().getSd().getLocation());
  }

  @Test
  public void alterPartitionsReqQualifiesUnqualifiedLocation() throws Throwable {
    AtomicReference<Object[]> captured = new AtomicReference<>();
    RoutingMetaStoreProxy handler = locationRewriteHandlerWithCapturedArgs(
        ExternalTableLocationRewriteMode.QUALIFY_UNQUALIFIED,
        null,
        captured);

    Partition p = partition("catalog2__sales", "events", List.of("2026-09-17"), "/tmp/external/events/p1");
    TestAlterPartitionsRequest req = new TestAlterPartitionsRequest("catalog2__sales", "events", List.of(p));

    handler.alter_partitions_req(req);

    Assert.assertNotNull(captured.get());
    List<?> parts = captured.get().length == 1
        ? (List<?>) ThriftReflectionCache.invokeGetter(captured.get()[0], "getPartitions")
        : (List<?>) captured.get()[2];
    Assert.assertNotNull(parts);
    Assert.assertEquals(1, parts.size());
    Partition routedP = (Partition) parts.get(0);
    Assert.assertEquals("sales", routedP.getDbName());
    Assert.assertEquals("hdfs://ns-catalog2/tmp/external/events/p1", routedP.getSd().getLocation());
  }

  @Test
  public void alterPartitionsReqRewritesSourceDefaultFsToTarget() throws Throwable {
    AtomicReference<Object[]> captured = new AtomicReference<>();
    RoutingMetaStoreProxy handler = locationRewriteHandlerWithCapturedArgs(
        ExternalTableLocationRewriteMode.REWRITE_IF_SOURCE_DEFAULT_FS,
        "hdfs://ns-frontend",
        captured);

    Partition p = partition("catalog2__sales", "events", List.of("2026-09-17"), "hdfs://ns-frontend/tmp/external/events/p1");
    TestAlterPartitionsRequest req = new TestAlterPartitionsRequest("catalog2__sales", "events", List.of(p));

    handler.alter_partitions_req(req);

    Assert.assertNotNull(captured.get());
    List<?> parts = captured.get().length == 1
        ? (List<?>) ThriftReflectionCache.invokeGetter(captured.get()[0], "getPartitions")
        : (List<?>) captured.get()[2];
    Assert.assertNotNull(parts);
    Partition routedP = (Partition) parts.get(0);
    Assert.assertEquals("hdfs://ns-catalog2/tmp/external/events/p1", routedP.getSd().getLocation());
  }

  @Test
  public void addPartitionsReqQualifiesUnqualifiedLocation() throws Throwable {
    AtomicReference<Object[]> captured = new AtomicReference<>();
    RoutingMetaStoreProxy handler = locationRewriteHandlerWithCapturedArgs(
        ExternalTableLocationRewriteMode.QUALIFY_UNQUALIFIED,
        null,
        captured);

    AddPartitionsRequest req = new AddPartitionsRequest();
    req.setDbName("catalog2__sales");
    req.setTblName("events");
    req.setParts(List.of(partition("catalog2__sales", "events", List.of("2026-09-17"), "/tmp/external/events/p2")));

    handler.add_partitions_req(req);

    Assert.assertNotNull(captured.get());
    Object routedReq = captured.get()[0];
    AddPartitionsRequest routed = (AddPartitionsRequest) routedReq;
    Assert.assertEquals("sales", routed.getDbName());
    Assert.assertEquals("hdfs://ns-catalog2/tmp/external/events/p2", routed.getParts().get(0).getSd().getLocation());
  }

  @Test
  public void positionalAlterPartitionQualifiesUnqualifiedLocation() throws Throwable {
    AtomicReference<Object[]> captured = new AtomicReference<>();
    RoutingMetaStoreProxy handler = locationRewriteHandlerWithCapturedArgs(
        ExternalTableLocationRewriteMode.QUALIFY_UNQUALIFIED,
        null,
        captured);

    Partition p = partition("catalog2__sales", "events", List.of("2026-09-17"), "/tmp/external/events/p1");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("alter_partition", String.class, String.class, Partition.class);

    handler.invoke(null, method, new Object[]{"catalog2__sales", "events", p});

    Assert.assertNotNull(captured.get());
    Partition routedP = (Partition) captured.get()[2];
    Assert.assertEquals("sales", routedP.getDbName());
    Assert.assertEquals("hdfs://ns-catalog2/tmp/external/events/p1", routedP.getSd().getLocation());
  }

  @Test
  public void positionalAlterPartitionsRewritesSourceDefaultFs() throws Throwable {
    AtomicReference<Object[]> captured = new AtomicReference<>();
    RoutingMetaStoreProxy handler = locationRewriteHandlerWithCapturedArgs(
        ExternalTableLocationRewriteMode.REWRITE_IF_SOURCE_DEFAULT_FS,
        "hdfs://ns-frontend",
        captured);

    Partition p = partition("catalog2__sales", "events", List.of("2026-09-17"), "hdfs://ns-frontend/tmp/external/events/p1");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("alter_partitions", String.class, String.class, List.class);

    handler.invoke(null, method, new Object[]{"catalog2__sales", "events", List.of(p)});

    Assert.assertNotNull(captured.get());
    List<?> routedList = (List<?>) captured.get()[2];
    Partition routedP = (Partition) routedList.get(0);
    Assert.assertEquals("sales", routedP.getDbName());
    Assert.assertEquals("hdfs://ns-catalog2/tmp/external/events/p1", routedP.getSd().getLocation());
  }

  @Test
  public void renamePartitionRewritesPartitionLocation() throws Throwable {
    AtomicReference<Object[]> captured = new AtomicReference<>();
    RoutingMetaStoreProxy handler = locationRewriteHandlerWithCapturedArgs(
        ExternalTableLocationRewriteMode.QUALIFY_UNQUALIFIED,
        null,
        captured);

    Partition p = partition("catalog2__sales", "events", List.of("2026-09-17"), "/tmp/external/events/p_renamed");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("rename_partition", String.class, String.class, List.class, Partition.class);

    handler.invoke(null, method, new Object[]{"catalog2__sales", "events", List.of("2026-09-17"), p});

    Assert.assertNotNull(captured.get());
    Partition routedP = (Partition) captured.get()[3];
    Assert.assertEquals("sales", routedP.getDbName());
    Assert.assertEquals("hdfs://ns-catalog2/tmp/external/events/p_renamed", routedP.getSd().getLocation());
  }

  private static RoutingMetaStoreProxy locationRewriteHandlerWithCapturedArgs(
      ExternalTableLocationRewriteMode mode,
      String sourceDefaultFs,
      AtomicReference<Object[]> capturedArgs
  ) throws Exception {
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of(
            "catalog1",
            catalogConfig(
                "catalog1", "c1", null, null,
                Map.of("hive.metastore.uris", "thrift://one", "fs.defaultFS", "hdfs://ns-catalog1")),
            "catalog2",
            catalogConfig(
                "catalog2", "c2", null, null,
                Map.of("hive.metastore.uris", "thrift://two", "fs.defaultFS", "hdfs://ns-catalog2"))))
        .compatibility(new CompatibilityConfig(false))
        .federation(new FederationConfig(false, ViewTextRewriteMode.DISABLED, false, mode, sourceDefaultFs))
        .transactionalDdlGuard(new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.DISABLED, List.of()))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    BackendInvocationSession backend1Session = newSession((proxy, method, args) -> null);
    BackendInvocationSession backend2Session = newSession((proxy, method, args) -> {
      if (args != null) {
        capturedArgs.set(args);
      }
      return null;
    });
    CatalogBackend backend1 = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), backend1Session));
    CatalogBackend backend2 = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog2"), backend2Session));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", backend1);
    backends.put("catalog2", backend2);
    CatalogRouter router = new CatalogRouter(config, backends);
    return new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
  }

  private static Partition partition(String db, String table, List<String> values, String location) {
    Partition part = new Partition();
    part.setDbName(db);
    part.setTableName(table);
    part.setValues(values);
    part.setSd(storageDescriptor(location));
    return part;
  }

  public static class TestAlterPartitionsRequest {
    private String dbName;
    private String tableName;
    private List<Partition> partitions;

    public TestAlterPartitionsRequest(String dbName, String tableName, List<Partition> partitions) {
      this.dbName = dbName;
      this.tableName = tableName;
      this.partitions = partitions;
    }

    public String getDbName() { return dbName; }
    public void setDbName(String dbName) { this.dbName = dbName; }
    public String getTableName() { return tableName; }
    public List<Partition> getPartitions() { return partitions; }
    public void setPartitions(List<Partition> partitions) { this.partitions = partitions; }
  }

}
