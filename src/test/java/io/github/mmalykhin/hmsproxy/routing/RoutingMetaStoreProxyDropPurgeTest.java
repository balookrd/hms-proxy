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
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.security.UserGroupInformation;
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
    List<String> events = Collections.synchronizedList(new ArrayList<>());
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
    handler.close();

    Assert.assertEquals(List.of("get_table", "prepare", "drop_table", "purge"), events);
    Assert.assertEquals("sales", purger.preparedTable.get().getDbName());
    Assert.assertEquals("events", purger.preparedTable.get().getTableName());
  }

  @Test
  public void dropTableWithEnvironmentContextPurgesExternalTableAfterSuccessfulBackendDrop() throws Throwable {
    ProxyConfig config = dropPurgeConfig();
    List<String> events = Collections.synchronizedList(new ArrayList<>());
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
    handler.close();

    Assert.assertEquals(List.of("get_table", "prepare", "drop_table_with_environment_context", "purge"), events);
  }

  @Test
  public void dropTableIgnoresBestEffortPurgeFailureAfterSuccessfulBackendDrop() throws Throwable {
    ProxyConfig config = dropPurgeConfig();
    List<String> events = Collections.synchronizedList(new ArrayList<>());
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
    handler.close();

    Assert.assertEquals(List.of("get_table", "prepare", "drop_table", "purge"), events);
    Assert.assertEquals(1, purger.purgeCalls.get());
  }

  @Test
  public void dropTableRunsPurgeOffTheRequestThread() throws Throwable {
    ProxyConfig config = dropPurgeConfig();
    List<String> events = Collections.synchronizedList(new ArrayList<>());
    RecordingExternalTableDropPurger purger = new RecordingExternalTableDropPurger(events);
    purger.preparedRequest = Optional.of(new ExternalTableDropPurger.PurgeRequest("hdfs://ns-dev3/tmp/external/events"));
    CountDownLatch purgeStarted = new CountDownLatch(1);
    CountDownLatch releasePurge = new CountDownLatch(1);
    AtomicBoolean purgeFinished = new AtomicBoolean();
    purger.purgeAction = () -> {
      purgeStarted.countDown();
      awaitQuietly(releasePurge);
      purgeFinished.set(true);
    };

    RoutingMetaStoreProxy handler = dropPurgeHandler(config, events, purger);
    Method method = thriftMethod("drop_table");

    handler.invoke(null, method, dropTableArguments(method, "catalog1__sales", "events"));

    Assert.assertTrue("purge did not start in the background", purgeStarted.await(10, TimeUnit.SECONDS));
    Assert.assertFalse("drop_table returned only after the purge completed", purgeFinished.get());
    Assert.assertNotEquals(Thread.currentThread().getName(), purger.purgeThreadName.get());

    releasePurge.countDown();
    handler.close();
    Assert.assertTrue(purgeFinished.get());
    Assert.assertEquals(1, purger.purgeCalls.get());
  }

  @Test
  public void closeWaitsForPendingBackgroundPurge() throws Throwable {
    ProxyConfig config = dropPurgeConfig();
    List<String> events = Collections.synchronizedList(new ArrayList<>());
    RecordingExternalTableDropPurger purger = new RecordingExternalTableDropPurger(events);
    purger.preparedRequest = Optional.of(new ExternalTableDropPurger.PurgeRequest("hdfs://ns-dev3/tmp/external/events"));
    CountDownLatch purgeStarted = new CountDownLatch(1);
    AtomicBoolean purgeFinished = new AtomicBoolean();
    purger.purgeAction = () -> {
      purgeStarted.countDown();
      sleepQuietly(300L);
      purgeFinished.set(true);
    };

    RoutingMetaStoreProxy handler = dropPurgeHandler(config, events, purger);
    Method method = thriftMethod("drop_table");

    handler.invoke(null, method, dropTableArguments(method, "catalog1__sales", "events"));
    Assert.assertTrue(purgeStarted.await(10, TimeUnit.SECONDS));
    handler.close();

    Assert.assertTrue("close() returned before the pending purge finished", purgeFinished.get());
    Assert.assertTrue(
        "purge did not run on a named background worker: " + purger.purgeThreadName.get(),
        purger.purgeThreadName.get().startsWith("hms-proxy-drop-purge-"));
    Assert.assertEquals(List.of("get_table", "prepare", "drop_table", "purge"), events);
  }

  @Test
  public void kerberizedPurgeReusesSingleKeytabLogin() throws Exception {
    assumeHadoopFileSystemUsable();
    java.nio.file.Path root = Files.createTempDirectory("hms-proxy-drop-purge");
    ProxyConfig config = kerberizedDropPurgeConfig(root);
    AtomicInteger logins = new AtomicInteger();
    AtomicInteger relogins = new AtomicInteger();
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("hms-proxy-purger");
    KeytabUgiProvider ugiProvider = new KeytabUgiProvider(
        (principal, keytab) -> {
          logins.incrementAndGet();
          return ugi;
        },
        loggedIn -> relogins.incrementAndGet());
    FileSystemExternalTableDropPurger purger = new FileSystemExternalTableDropPurger(config, ugiProvider);
    CatalogBackend backend = kerberizedBackend(config);

    java.nio.file.Path first = Files.createDirectories(root.resolve("first/data"));
    java.nio.file.Path second = Files.createDirectories(root.resolve("second/data"));
    purger.purge(backend, new ExternalTableDropPurger.PurgeRequest("file:" + root.resolve("first")));
    purger.purge(backend, new ExternalTableDropPurger.PurgeRequest("file:" + root.resolve("second")));

    Assert.assertEquals("keytab login must happen once and be reused", 1, logins.get());
    Assert.assertEquals("cached login must be refreshed before every reuse", 1, relogins.get());
    Assert.assertFalse(Files.exists(first));
    Assert.assertFalse(Files.exists(second));
  }

  @Test
  public void kerberizedPurgeKeepsCachedFileSystemUsable() throws Exception {
    assumeHadoopFileSystemUsable();
    java.nio.file.Path root = Files.createTempDirectory("hms-proxy-drop-purge");
    ProxyConfig config = kerberizedDropPurgeConfig(root);
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("hms-proxy-purger");
    KeytabUgiProvider ugiProvider = new KeytabUgiProvider((principal, keytab) -> ugi, loggedIn -> { });
    FileSystemExternalTableDropPurger purger = new FileSystemExternalTableDropPurger(config, ugiProvider);
    CatalogBackend backend = kerberizedBackend(config);
    Files.createDirectories(root.resolve("first/data"));

    FileSystem cached = ugi.doAs((PrivilegedExceptionAction<FileSystem>) () ->
        FileSystem.get(URI.create("file:///"), backend.hiveConf()));
    purger.purge(backend, new ExternalTableDropPurger.PurgeRequest("file:" + root.resolve("first")));

    // A shared UGI implies a shared entry in Hadoop's static FileSystem cache: closing it after a
    // purge would break concurrent purges instead of freeing anything.
    Assert.assertTrue(cached.exists(new org.apache.hadoop.fs.Path("file:" + root)));
    FileSystem afterPurge = ugi.doAs((PrivilegedExceptionAction<FileSystem>) () ->
        FileSystem.get(URI.create("file:///"), backend.hiveConf()));
    Assert.assertSame(cached, afterPurge);
  }

  @Test
  public void prepareRejectsLocationOutsideAllowedPrefixes() throws Exception {
    assumeHadoopFileSystemUsable();
    ProxyConfig config = localAllowlistDropPurgeConfig();
    FileSystemExternalTableDropPurger purger = new FileSystemExternalTableDropPurger(config);
    CatalogBackend backend = simpleBackend(config);

    Optional<ExternalTableDropPurger.PurgeRequest> request = purger.prepare(
        backend, externalTable("sales", "events", "file:/tmp/hms-proxy-denied/events", true));

    Assert.assertTrue(request.isEmpty());
  }

  @Test
  public void prepareAcceptsLocationInsideAllowedPrefixes() throws Exception {
    assumeHadoopFileSystemUsable();
    ProxyConfig config = localAllowlistDropPurgeConfig();
    FileSystemExternalTableDropPurger purger = new FileSystemExternalTableDropPurger(config);
    CatalogBackend backend = simpleBackend(config);

    Optional<ExternalTableDropPurger.PurgeRequest> request = purger.prepare(
        backend, externalTable("sales", "events", "file:/tmp/hms-proxy-allowed/events", true));

    Assert.assertEquals(
        Optional.of(new ExternalTableDropPurger.PurgeRequest("file:/tmp/hms-proxy-allowed/events")),
        request);
  }

  @Test
  public void prepareRejectsPurgeWhenAllowedPrefixesAreNotConfigured() throws Exception {
    ProxyConfig config = dropPurgeConfigWithoutAllowlist();
    FileSystemExternalTableDropPurger purger = new FileSystemExternalTableDropPurger(config);
    CatalogBackend backend = simpleBackend(config);

    Optional<ExternalTableDropPurger.PurgeRequest> request = purger.prepare(
        backend, externalTable("sales", "events", "hdfs://ns-dev3/tmp/external/events", true));

    Assert.assertTrue(request.isEmpty());
  }

  private static RoutingMetaStoreProxy dropPurgeHandler(
      ProxyConfig config,
      List<String> events,
      ExternalTableDropPurger purger
  ) throws Exception {
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
    return new RoutingMetaStoreProxy(
        config, router, new FederationLayer(config, router), null, new ProxyObservability(config), purger);
  }

  private static CatalogBackend simpleBackend(ProxyConfig config) throws Exception {
    return newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), newSession()));
  }

  private static CatalogBackend kerberizedBackend(ProxyConfig config) throws Exception {
    return simpleBackend(config);
  }

  private static ProxyConfig kerberizedDropPurgeConfig(java.nio.file.Path allowedRoot) {
    return dropPurgeConfigBuilder(Map.of(
        "hive.metastore.uris", "thrift://one",
        "hadoop.security.authentication", "kerberos",
        FileSystemExternalTableDropPurger.ALLOWED_PREFIXES_CONF_KEY, "file:" + allowedRoot + "/"))
        .security(new SecurityConfig(
            SecurityMode.KERBEROS, "hms/_HOST@EXAMPLE.COM", null, "/etc/security/keytabs/hms.keytab",
            null, false, Map.of()))
        .build();
  }

  private static ProxyConfig localAllowlistDropPurgeConfig() {
    return dropPurgeConfigBuilder(Map.of(
        "hive.metastore.uris", "thrift://one",
        FileSystemExternalTableDropPurger.ALLOWED_PREFIXES_CONF_KEY, "file:/tmp/hms-proxy-allowed/"))
        .build();
  }

  private static ProxyConfig dropPurgeConfigWithoutAllowlist() {
    return dropPurgeConfigBuilder(Map.of("hive.metastore.uris", "thrift://one")).build();
  }

  private static ProxyConfig.Builder dropPurgeConfigBuilder(Map<String, String> hiveConf) {
    return ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of(
            "catalog1",
            catalogConfig("catalog1", "c1", MetastoreRuntimeProfile.APACHE_3_1_3, null, hiveConf)))
        .compatibility(new CompatibilityConfig(false))
        .federation(new FederationConfig(
            false,
            ViewTextRewriteMode.DISABLED,
            false,
            ExternalTableLocationRewriteMode.DISABLED,
            null,
            ExternalTableDropPurgeMode.BEST_EFFORT))
        .transactionalDdlGuard(new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.DISABLED, List.of()))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory());
  }

  /**
   * Hadoop's UGI still calls the JDK Subject APIs that JDK 24+ refuses to serve, so tests that
   * touch a real FileSystem only run on the supported Java 17 runtime.
   */
  private static void assumeHadoopFileSystemUsable() {
    try {
      UserGroupInformation.getCurrentUser();
    } catch (Throwable unsupported) {
      Assume.assumeNoException(
          "Hadoop UGI is unusable on this JVM (JDK 24+ removed the Subject API it relies on)",
          unsupported);
    }
  }

  private static void awaitQuietly(CountDownLatch latch) {
    try {
      latch.await(10L, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static void sleepQuietly(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
