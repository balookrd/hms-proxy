package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.config.routing.DefaultBackendRoutingPolicy;
import io.github.mmalykhin.hmsproxy.config.operation.HmsOperationClass;
import io.github.mmalykhin.hmsproxy.config.operation.HmsOperationPolicy;
import org.junit.Assert;
import org.junit.Test;
import io.github.mmalykhin.hmsproxy.config.catalog.NamespaceStrategy;
import io.github.mmalykhin.hmsproxy.config.operation.OperationMetadata;
import io.github.mmalykhin.hmsproxy.config.catalog.ReadResultFilterKind;
import io.github.mmalykhin.hmsproxy.config.catalog.TableExposureMode;

public class HmsOperationPolicyTest {
  @Test
  public void metadataReadsCarryNamespaceShapeAndReadFilters() {
    OperationMetadata operation = HmsOperationPolicy.describe("get_table");

    Assert.assertEquals(HmsOperationClass.METADATA_READ, operation.operationClass());
    Assert.assertEquals(NamespaceStrategy.DB_STRING_ARG0, operation.namespaceStrategy());
    Assert.assertEquals(TableExposureMode.TABLE_ARG1, operation.tableExposureMode());
    Assert.assertEquals(ReadResultFilterKind.SINGLE_TABLE, operation.readResultFilterKind());
    Assert.assertFalse(operation.mutating());
    Assert.assertTrue(operation.trace());
  }

  @Test
  public void globalWritesAreModeledSeparatelyFromMetadataWrites() {
    OperationMetadata operation = HmsOperationPolicy.describe("setMetaConf");

    Assert.assertEquals(HmsOperationClass.SERVICE_GLOBAL_WRITE, operation.operationClass());
    Assert.assertEquals(NamespaceStrategy.NONE, operation.namespaceStrategy());
    Assert.assertTrue(operation.mutating());
  }

  @Test
  public void acidLifecycleMethodsKeepDefaultBackendPolicy() {
    OperationMetadata operation = HmsOperationPolicy.describe("open_txns");

    Assert.assertEquals(HmsOperationClass.ACID_ID_BOUND_LIFECYCLE, operation.operationClass());
    Assert.assertEquals(
        DefaultBackendRoutingPolicy.Policy.TXN_AND_LOCK_LIFECYCLE,
        operation.defaultBackendPolicy());
    Assert.assertEquals(NamespaceStrategy.NONE, operation.namespaceStrategy());
    Assert.assertTrue(operation.trace());
  }

  @Test
  public void compatibilityOnlyMethodsCanStillBeMutating() {
    OperationMetadata operation = HmsOperationPolicy.describe("add_write_notification_log");

    Assert.assertEquals(HmsOperationClass.COMPATIBILITY_ONLY_RPC, operation.operationClass());
    Assert.assertTrue(operation.mutating());
    Assert.assertTrue(operation.trace());
  }

  @Test
  public void adminMethodsStayOutOfNamespaceRouting() {
    OperationMetadata operation = HmsOperationPolicy.describe("get_catalogs");

    Assert.assertEquals(HmsOperationClass.ADMIN_INTROSPECTION, operation.operationClass());
    Assert.assertEquals(NamespaceStrategy.NONE, operation.namespaceStrategy());
    Assert.assertFalse(operation.mutating());
  }
}
