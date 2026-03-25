package io.github.mmalykhin.hmsproxy;

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
  public void unrelatedGlobalMethodStillRequiresExplicitHandling() {
    Assert.assertFalse(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("create_role"));
  }
}
