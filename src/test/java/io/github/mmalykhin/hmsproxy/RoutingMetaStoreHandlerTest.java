package io.github.mmalykhin.hmsproxy;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Test;

public class RoutingMetaStoreHandlerTest {
  @Test
  public void getAllFunctionsUsesDefaultBackendCompatibilityPath() {
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_all_functions"));
  }

  @Test
  public void currentNotificationEventIdUsesDefaultBackendCompatibilityPath() {
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_current_notificationEventId"));
  }

  @Test
  public void globalReadOnlyMethodsUseDefaultBackendCompatibilityPath() {
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_all_resource_plans"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("show_compact"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("list_roles"));
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
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_role_names"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_all_token_identifiers"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_master_keys"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_open_txns"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("show_locks"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("show_compact"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_active_resource_plan"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_runtime_stats"));
  }

  @Test
  public void unrelatedGlobalMethodStillRequiresExplicitHandling() {
    Assert.assertFalse(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("create_role"));
  }
}
