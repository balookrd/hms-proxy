package io.github.mmalykhin.hmsproxy;

import org.junit.Assert;
import org.junit.Test;

public class RoutingMetaStoreHandlerTest {
  @Test
  public void getAllFunctionsUsesDefaultBackendCompatibilityPath() {
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_all_functions"));
  }

  @Test
  public void unrelatedGlobalMethodStillRequiresExplicitHandling() {
    Assert.assertFalse(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_all_tables"));
  }
}
