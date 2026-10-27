package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.config.routing.DefaultBackendRoutingPolicy;
import io.github.mmalykhin.hmsproxy.config.operation.HmsOperationClass;
import io.github.mmalykhin.hmsproxy.config.operation.HmsOperationPolicy;
import java.lang.reflect.Method;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
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
  public void getValidWriteIdsExtractsFromArgsWithDefaultBackendFallback() {
    OperationMetadata operation = HmsOperationPolicy.describe("get_valid_write_ids");

    Assert.assertEquals(HmsOperationClass.ACID_NAMESPACE_BOUND_WRITE, operation.operationClass());
    Assert.assertEquals(
        DefaultBackendRoutingPolicy.Policy.TXN_AND_LOCK_LIFECYCLE,
        operation.defaultBackendPolicy());
    Assert.assertEquals(NamespaceStrategy.EXTRACT_FROM_ARGS, operation.namespaceStrategy());
    Assert.assertFalse(operation.mutating());
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

  @Test
  public void namespacelessPartitionHelpersUseAdminIntrospectionAndDefaultBackend() {
    for (String method : java.util.List.of("partition_name_has_valid_characters", "partition_name_to_spec", "partition_name_to_vals")) {
      OperationMetadata operation = HmsOperationPolicy.describe(method);
      Assert.assertEquals(HmsOperationClass.ADMIN_INTROSPECTION, operation.operationClass());
      Assert.assertEquals(DefaultBackendRoutingPolicy.Policy.NAMESPACELESS_VALIDATION, operation.defaultBackendPolicy());
      Assert.assertEquals(NamespaceStrategy.NONE, operation.namespaceStrategy());
      Assert.assertFalse(operation.mutating());
    }
  }

  @Test
  public void refreshPrivilegesIsAMetadataWrite() {
    OperationMetadata operation = HmsOperationPolicy.describe("refresh_privileges");

    Assert.assertEquals(HmsOperationClass.METADATA_WRITE, operation.operationClass());
    Assert.assertTrue(operation.mutating());
  }

  @Test
  public void materializationRebuildLockLifecycleIsMutating() {
    Assert.assertTrue(HmsOperationPolicy.describe("get_lock_materialization_rebuild").mutating());
    Assert.assertTrue(HmsOperationPolicy.describe("heartbeat_lock_materialization_rebuild").mutating());
  }

  @Test
  public void checkLockIsMutatingLikeItsLifecycleSiblings() {
    OperationMetadata operation = HmsOperationPolicy.describe("check_lock");

    Assert.assertEquals(HmsOperationClass.ACID_ID_BOUND_LIFECYCLE, operation.operationClass());
    Assert.assertTrue(operation.mutating());
  }

  @Test
  public void replicationSchemaAndFileMetadataWritesAreMutating() {
    Assert.assertTrue(HmsOperationPolicy.describe("cm_recycle").mutating());
    Assert.assertTrue(HmsOperationPolicy.describe("map_schema_version_to_serde").mutating());
    Assert.assertTrue(HmsOperationPolicy.describe("put_file_metadata").mutating());
    Assert.assertTrue(HmsOperationPolicy.describe("clear_file_metadata").mutating());
    Assert.assertTrue(HmsOperationPolicy.describe("cache_file_metadata").mutating());
  }

  @Test
  public void fileMetadataAndPrivilegeReadsStayNonMutating() {
    Assert.assertFalse(HmsOperationPolicy.describe("get_file_metadata").mutating());
    Assert.assertFalse(HmsOperationPolicy.describe("get_file_metadata_by_expr").mutating());
    Assert.assertFalse(HmsOperationPolicy.describe("list_privileges").mutating());
    Assert.assertFalse(HmsOperationPolicy.describe("get_privilege_set").mutating());
  }

  @Test
  public void onlyCallsThatChangeNoStateMayBeReplayedAfterALostReply() {
    Assert.assertTrue(HmsOperationPolicy.replayableAfterLostReply("get_table"));
    Assert.assertTrue(HmsOperationPolicy.replayableAfterLostReply("get_all_databases"));
    Assert.assertTrue(HmsOperationPolicy.replayableAfterLostReply("show_locks"));

    for (String write : new String[] {
        "create_table", "add_partition", "alter_table_with_environment_context", "drop_table",
        "open_txns", "commit_txn", "abort_txns", "lock", "heartbeat", "allocate_table_write_ids"}) {
      Assert.assertFalse(write, HmsOperationPolicy.replayableAfterLostReply(write));
    }
    // Not flagged mutating, but each call has its own side effect.
    for (String sideEffect : new String[] {
        "get_delegation_token", "renew_delegation_token", "cancel_delegation_token", "shutdown", "reinitialize"}) {
      Assert.assertFalse(sideEffect, HmsOperationPolicy.replayableAfterLostReply(sideEffect));
    }
    Assert.assertFalse(
        "a name the registry does not know is not replayed on a guess",
        HmsOperationPolicy.replayableAfterLostReply("frobnicate_tables"));
  }

  @Test
  public void noMutatingMetastoreMethodIsReplayable() {
    for (Method method : ThriftHiveMetastore.Iface.class.getMethods()) {
      if (HmsOperationPolicy.describe(method.getName()).mutating()) {
        Assert.assertFalse(method.getName(), HmsOperationPolicy.replayableAfterLostReply(method.getName()));
      }
    }
  }
}
