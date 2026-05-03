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

public class RoutingMetaStoreProxyAuthorizationGuardsTest {
  @Test
  public void setMetaConfWithoutCatalogContextIsRejectedInMultiCatalogMode() throws Throwable {
    RoutingMetaStoreProxy handler =
        new RoutingMetaStoreProxy(CUSTOM_SEPARATOR_CONFIG, CUSTOM_SEPARATOR_ROUTER, new FederationLayer(CUSTOM_SEPARATOR_CONFIG, CUSTOM_SEPARATOR_ROUTER), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("setMetaConf", String.class, String.class);

    MetaException error = Assert.assertThrows(
        MetaException.class,
        () -> handler.invoke(null, method, new Object[] {"metastore.thrift.uris", "thrift://override"}));

    Assert.assertTrue(error.getMessage().contains("requires explicit namespace ownership"));
  }

  @Test
  public void grantRoleWithoutCatalogContextIsRejectedInMultiCatalogMode() throws Throwable {
    RoutingMetaStoreProxy handler =
        new RoutingMetaStoreProxy(CUSTOM_SEPARATOR_CONFIG, CUSTOM_SEPARATOR_ROUTER, new FederationLayer(CUSTOM_SEPARATOR_CONFIG, CUSTOM_SEPARATOR_ROUTER), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod(
        "grant_role",
        String.class,
        String.class,
        PrincipalType.class,
        String.class,
        PrincipalType.class,
        boolean.class);

    MetaException error = Assert.assertThrows(
        MetaException.class,
        () -> handler.invoke(
            null,
            method,
            new Object[] {"admin_role", "alice", PrincipalType.USER, "hive", PrincipalType.USER, false}));

    Assert.assertTrue(error.getMessage().contains("requires explicit namespace ownership"));
  }

  @Test
  public void revokeRoleWithoutCatalogContextIsRejectedInMultiCatalogMode() throws Throwable {
    RoutingMetaStoreProxy handler =
        new RoutingMetaStoreProxy(CUSTOM_SEPARATOR_CONFIG, CUSTOM_SEPARATOR_ROUTER, new FederationLayer(CUSTOM_SEPARATOR_CONFIG, CUSTOM_SEPARATOR_ROUTER), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod(
        "revoke_role",
        String.class,
        String.class,
        PrincipalType.class);

    MetaException error = Assert.assertThrows(
        MetaException.class,
        () -> handler.invoke(null, method, new Object[] {"admin_role", "alice", PrincipalType.USER}));

    Assert.assertTrue(error.getMessage().contains("requires explicit namespace ownership"));
  }

  @Test
  public void catalogManagementRpcsAreRejected() throws Throwable {
    RoutingMetaStoreProxy handler =
        new RoutingMetaStoreProxy(CUSTOM_SEPARATOR_CONFIG, CUSTOM_SEPARATOR_ROUTER, new FederationLayer(CUSTOM_SEPARATOR_CONFIG, CUSTOM_SEPARATOR_ROUTER), null);

    assertCatalogManagementRejected(handler, "create_catalog");
    assertCatalogManagementRejected(handler, "alter_catalog");
    assertCatalogManagementRejected(handler, "drop_catalog");
  }

  @Test
  public void addTokenWithoutKerberosFrontDoorIsRejected() throws Throwable {
    RoutingMetaStoreProxy handler =
        new RoutingMetaStoreProxy(CUSTOM_SEPARATOR_CONFIG, CUSTOM_SEPARATOR_ROUTER, new FederationLayer(CUSTOM_SEPARATOR_CONFIG, CUSTOM_SEPARATOR_ROUTER), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("add_token", String.class, String.class);

    MetaException error = Assert.assertThrows(
        MetaException.class,
        () -> handler.invoke(null, method, new Object[] {"token-id", "payload"}));

    Assert.assertTrue(error.getMessage().contains("Delegation tokens require Kerberos/SASL"));
  }

  @Test
  public void tokenIdentifierListingWithoutKerberosFrontDoorIsRejected() throws Throwable {
    RoutingMetaStoreProxy handler =
        new RoutingMetaStoreProxy(CUSTOM_SEPARATOR_CONFIG, CUSTOM_SEPARATOR_ROUTER, new FederationLayer(CUSTOM_SEPARATOR_CONFIG, CUSTOM_SEPARATOR_ROUTER), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("get_all_token_identifiers");

    MetaException error = Assert.assertThrows(
        MetaException.class,
        () -> handler.invoke(null, method, new Object[0]));

    Assert.assertTrue(error.getMessage().contains("Delegation tokens require Kerberos/SASL"));
  }

  @Test
  public void servicePrincipalTrafficDoesNotTriggerBackendImpersonation() {
    SecurityConfig security = new SecurityConfig(
        SecurityMode.KERBEROS,
        "hive/hd-hdp-31-08.dmp.vimpelcom.ru@BEE.VIMPELCOM.RU",
        "proxy-client/hd-hdp-31-08.dmp.vimpelcom.ru@BEE.VIMPELCOM.RU",
        "/etc/security/keytabs/hive.service.keytab",
        "/etc/security/keytabs/hive.client.keytab",
        true,
        java.util.Map.of());

    Assert.assertTrue(RoutingMetaStoreProxy.isServicePrincipalUser("hive", security));
    Assert.assertFalse(RoutingMetaStoreProxy.isServicePrincipalUser("alice", security));
    Assert.assertFalse(RoutingMetaStoreProxy.isServicePrincipalUser("proxy-client", security));
  }

  @Test
  public void delegationTokenAuthorizationMessageMentionsFrontDoorProxyUserKeys() {
    String message = FrontDoorSecurity.delegationTokenAuthorizationMessage(
        "algaraev",
        "algaraev",
        "hive/hd-hdp-31-08.dmp.vimpelcom.ru@BEE.VIMPELCOM.RU",
        "10.0.0.8");

    Assert.assertTrue(message.contains("owner 'algaraev'"));
    Assert.assertTrue(message.contains("authenticated as 'hive/hd-hdp-31-08.dmp.vimpelcom.ru@BEE.VIMPELCOM.RU'"));
    Assert.assertTrue(message.contains("10.0.0.8"));
    Assert.assertTrue(message.contains("security.front-door-conf.hadoop.proxyuser.hive.hosts"));
    Assert.assertTrue(message.contains("security.front-door-conf.hadoop.proxyuser.hive.groups"));
  }

  @Test
  public void catalogBackendCloseQuietlySuppressesSocketClosedFailures() {
    CatalogBackend.closeQuietly(() -> {
      throw new SocketException("Socket closed");
    }, "test metastore client");
  }

}
