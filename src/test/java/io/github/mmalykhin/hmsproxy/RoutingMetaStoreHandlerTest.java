package io.github.mmalykhin.hmsproxy;

import java.net.SocketException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Test;

public class RoutingMetaStoreHandlerTest {
  private static final ProxyConfig CUSTOM_SEPARATOR_CONFIG = new ProxyConfig(
      new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
      new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
      "__",
      "catalog1",
      Map.of(
          "catalog1", new ProxyConfig.CatalogConfig("catalog1", "c1", "file:///c1", false, Map.of("hive.metastore.uris", "thrift://one")),
          "catalog2", new ProxyConfig.CatalogConfig("catalog2", "c2", "file:///c2", false, Map.of("hive.metastore.uris", "thrift://two"))));

  private static CatalogRouter routerFor(ProxyConfig config) {
    Map<String, CatalogBackend> backends = new LinkedHashMap<>();
    for (String name : config.catalogs().keySet()) {
      backends.put(name, null);
    }
    return new CatalogRouter(config, backends);
  }

  @Test
  public void onlyExplicitCompatibilityMethodsUseDefaultBackendPath() {
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("set_ugi"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("flushCache"));
  }

  @Test
  public void currentNotificationEventIdUsesDefaultBackendCompatibilityPath() {
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_current_notificationEventId"));
  }

  @Test
  public void explicitlyListedOperationalMethodsUseDefaultBackendCompatibilityPath() {
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_all_resource_plans"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("show_compact"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("show_locks"));
  }

  @Test
  public void notificationMethodsHaveCompatibilityFallbacks() {
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_current_notificationEventId"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_next_notification"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_notification_events_count"));
  }

  @Test
  public void refreshPrivilegesUsesContextRoutingButHasCompatibilityFallback() {
    Assert.assertFalse(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("refresh_privileges"));
  }

  @Test
  public void compatibilityFallbackAppliesToBackendMetaAndTransportFailures() {
    Assert.assertTrue(RoutingMetaStoreHandler.shouldUseCompatibilityFallback(
        "get_next_notification", new MetaException("not allowed")));
    Assert.assertTrue(RoutingMetaStoreHandler.shouldUseCompatibilityFallback(
        "refresh_privileges", new TTransportException()));
    Assert.assertTrue(RoutingMetaStoreHandler.shouldUseCompatibilityFallback(
        "show_compact", new TApplicationException("unsupported")));
  }

  @Test
  public void nonCompatibilityMethodsDoNotSilentlyFallback() {
    Assert.assertFalse(RoutingMetaStoreHandler.shouldUseCompatibilityFallback(
        "create_role", new MetaException("boom")));
    Assert.assertFalse(RoutingMetaStoreHandler.shouldUseCompatibilityFallback(
        "get_delegation_token", new MetaException("boom")));
  }

  @Test
  public void serviceReadMethodsUseDefaultBackendCompatibilityPath() {
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_open_txns"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_open_txns_info"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("show_locks"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("show_compact"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_active_resource_plan"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_runtime_stats"));
  }

  @Test
  public void removedPrefixBasedMethodsNoLongerUseDefaultBackendPath() {
    Assert.assertFalse(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_all_functions"));
    Assert.assertFalse(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_role_names"));
    Assert.assertFalse(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_all_token_identifiers"));
    Assert.assertFalse(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_master_keys"));
    Assert.assertFalse(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("list_roles"));
  }

  @Test
  public void unrelatedGlobalMethodStillRequiresExplicitHandling() {
    Assert.assertFalse(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("create_role"));
  }

  @Test
  public void servicePrincipalTrafficDoesNotTriggerBackendImpersonation() {
    ProxyConfig.SecurityConfig security = new ProxyConfig.SecurityConfig(
        ProxyConfig.SecurityMode.KERBEROS,
        "hive/hd-hdp-31-08.dmp.vimpelcom.ru@BEE.VIMPELCOM.RU",
        "proxy-client/hd-hdp-31-08.dmp.vimpelcom.ru@BEE.VIMPELCOM.RU",
        "/etc/security/keytabs/hive.service.keytab",
        "/etc/security/keytabs/hive.client.keytab",
        true,
        java.util.Map.of());

    Assert.assertTrue(RoutingMetaStoreHandler.isServicePrincipalUser("hive", security));
    Assert.assertFalse(RoutingMetaStoreHandler.isServicePrincipalUser("alice", security));
    Assert.assertFalse(RoutingMetaStoreHandler.isServicePrincipalUser("proxy-client", security));
  }

  @Test
  public void catalogBackendCloseQuietlySuppressesSocketClosedFailures() {
    CatalogBackend.closeQuietly(() -> {
      throw new SocketException("Socket closed");
    }, "test metastore client");
  }

  @Test
  public void getConfigValueCompatibilityMapsShortMetastoreAlias() {
    Assert.assertEquals("300", MetastoreCompatibility.compatibleConfigValue(
        "batch.retrieve.max",
        "50",
        java.util.Map.of("metastore.batch.retrieve.max", "300")).orElse(null));
  }

  @Test
  public void getConfigValueCompatibilityUsesClientDefaultWhenProxyConfigMissing() {
    Assert.assertEquals("50", MetastoreCompatibility.compatibleConfigValue(
        "batch.retrieve.max",
        "50",
        java.util.Map.of()).orElse(null));
  }

  @Test
  public void getConfigValueCompatibilityDoesNotInterceptCanonicalMetastoreKeys() {
    Assert.assertFalse(MetastoreCompatibility.compatibleConfigValue(
        "metastore.batch.retrieve.max",
        "50",
        java.util.Map.of()).isPresent());
  }

  @Test
  public void explicitDefaultCatalogLeavesUnprefixedDatabaseNameUntouched() throws Exception {
    RoutingMetaStoreHandler handler =
        new RoutingMetaStoreHandler(CUSTOM_SEPARATOR_CONFIG, routerFor(CUSTOM_SEPARATOR_CONFIG), null);
    java.lang.reflect.Method method =
        RoutingMetaStoreHandler.class.getDeclaredMethod("resolveRequestNamespace", String.class, String.class);
    method.setAccessible(true);

    CatalogRouter.ResolvedNamespace namespace =
        (CatalogRouter.ResolvedNamespace) method.invoke(handler, "catalog1", "sales");

    Assert.assertEquals("catalog1", namespace.catalogName());
    Assert.assertEquals("sales", namespace.backendDbName());
    Assert.assertEquals("sales", namespace.externalDbName());
  }

  @Test
  public void explicitCatalogPrefixStillRoutesUsingThatPrefix() throws Exception {
    RoutingMetaStoreHandler handler =
        new RoutingMetaStoreHandler(CUSTOM_SEPARATOR_CONFIG, routerFor(CUSTOM_SEPARATOR_CONFIG), null);
    java.lang.reflect.Method method =
        RoutingMetaStoreHandler.class.getDeclaredMethod("resolveRequestNamespace", String.class, String.class);
    method.setAccessible(true);

    CatalogRouter.ResolvedNamespace namespace =
        (CatalogRouter.ResolvedNamespace) method.invoke(handler, "catalog1", "catalog1__sales");

    Assert.assertEquals("catalog1", namespace.catalogName());
    Assert.assertEquals("sales", namespace.backendDbName());
    Assert.assertEquals("catalog1__sales", namespace.externalDbName());
  }

}
