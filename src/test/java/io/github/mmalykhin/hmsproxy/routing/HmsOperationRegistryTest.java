package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.config.HmsOperationClass;
import org.junit.Assert;
import org.junit.Test;

public class HmsOperationRegistryTest {
  @Test
  public void metadataReadsCarryNamespaceShapeAndReadFilters() {
    HmsOperationRegistry.OperationMetadata operation = HmsOperationRegistry.describe("get_table");

    Assert.assertEquals(HmsOperationClass.METADATA_READ, operation.operationClass());
    Assert.assertEquals(HmsOperationRegistry.NamespaceStrategy.DB_STRING_ARG0, operation.namespaceStrategy());
    Assert.assertEquals(HmsOperationRegistry.TableExposureMode.TABLE_ARG1, operation.tableExposureMode());
    Assert.assertEquals(HmsOperationRegistry.ReadResultFilterKind.SINGLE_TABLE, operation.readResultFilterKind());
    Assert.assertFalse(operation.mutating());
    Assert.assertTrue(operation.trace());
  }

  @Test
  public void globalWritesAreModeledSeparatelyFromMetadataWrites() {
    HmsOperationRegistry.OperationMetadata operation = HmsOperationRegistry.describe("setMetaConf");

    Assert.assertEquals(HmsOperationClass.SERVICE_GLOBAL_WRITE, operation.operationClass());
    Assert.assertEquals(HmsOperationRegistry.NamespaceStrategy.NONE, operation.namespaceStrategy());
    Assert.assertTrue(operation.mutating());
  }

  @Test
  public void acidLifecycleMethodsKeepDefaultBackendPolicy() {
    HmsOperationRegistry.OperationMetadata operation = HmsOperationRegistry.describe("open_txns");

    Assert.assertEquals(HmsOperationClass.ACID_ID_BOUND_LIFECYCLE, operation.operationClass());
    Assert.assertEquals(
        DefaultBackendRoutingPolicy.Policy.TXN_AND_LOCK_LIFECYCLE,
        operation.defaultBackendPolicy());
    Assert.assertEquals(HmsOperationRegistry.NamespaceStrategy.NONE, operation.namespaceStrategy());
    Assert.assertTrue(operation.trace());
  }

  @Test
  public void compatibilityOnlyMethodsCanStillBeMutating() {
    HmsOperationRegistry.OperationMetadata operation = HmsOperationRegistry.describe("add_write_notification_log");

    Assert.assertEquals(HmsOperationClass.COMPATIBILITY_ONLY_RPC, operation.operationClass());
    Assert.assertTrue(operation.mutating());
    Assert.assertTrue(operation.trace());
  }

  @Test
  public void adminMethodsStayOutOfNamespaceRouting() {
    HmsOperationRegistry.OperationMetadata operation = HmsOperationRegistry.describe("get_catalogs");

    Assert.assertEquals(HmsOperationClass.ADMIN_INTROSPECTION, operation.operationClass());
    Assert.assertEquals(HmsOperationRegistry.NamespaceStrategy.NONE, operation.namespaceStrategy());
    Assert.assertFalse(operation.mutating());
  }
}
