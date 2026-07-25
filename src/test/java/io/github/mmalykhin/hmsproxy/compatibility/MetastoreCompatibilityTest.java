package io.github.mmalykhin.hmsproxy.compatibility;

import io.github.mmalykhin.hmsproxy.config.routing.DefaultBackendRoutingPolicy;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Test;

public class MetastoreCompatibilityTest {
  private static final List<String> DATA_SEMANTIC_FALLBACK_METHODS = List.of(
      "get_open_txns",
      "get_open_txns_info",
      "show_locks",
      "show_compact",
      "get_privilege_set",
      "list_privileges",
      "get_role_names",
      "get_principals_in_role",
      "get_role_grants_for_principal",
      "refresh_privileges",
      "get_all_token_identifiers",
      "get_master_keys",
      "get_current_notificationEventId",
      "get_next_notification",
      "get_notification_events_count");
  private static final List<String> OPTIONAL_SERVICE_FALLBACK_METHODS = List.of(
      "get_runtime_stats",
      "get_active_resource_plan",
      "get_all_resource_plans");

  @Test
  public void delegationTokenMethodsAreHandledLocally() {
    Assert.assertTrue(MetastoreCompatibility.handlesLocally("get_delegation_token"));
    Assert.assertTrue(MetastoreCompatibility.handlesLocally("renew_delegation_token"));
    Assert.assertTrue(MetastoreCompatibility.handlesLocally("cancel_delegation_token"));
    Assert.assertTrue(MetastoreCompatibility.handlesLocally("add_token"));
    Assert.assertTrue(MetastoreCompatibility.handlesLocally("get_token"));
    Assert.assertTrue(MetastoreCompatibility.handlesLocally("remove_token"));
    Assert.assertTrue(MetastoreCompatibility.handlesLocally("get_all_token_identifiers"));
    Assert.assertTrue(MetastoreCompatibility.handlesLocally("add_master_key"));
    Assert.assertTrue(MetastoreCompatibility.handlesLocally("update_master_key"));
    Assert.assertTrue(MetastoreCompatibility.handlesLocally("remove_master_key"));
    Assert.assertTrue(MetastoreCompatibility.handlesLocally("get_master_keys"));
    Assert.assertFalse(MetastoreCompatibility.handlesLocally("set_ugi"));
  }

  @Test
  public void localDelegationTokenHandlingRequiresKerberosFrontDoor() {
    assertRequiresFrontDoorSecurity("get_delegation_token", new Object[] {"alice", "hive"});
    assertRequiresFrontDoorSecurity("renew_delegation_token", new Object[] {"token"});
    assertRequiresFrontDoorSecurity("cancel_delegation_token", new Object[] {"token"});
    assertRequiresFrontDoorSecurity("add_master_key", new Object[] {"key"});
    assertRequiresFrontDoorSecurity("get_master_keys", new Object[0]);
  }

  @Test
  public void compatibilityNoLongerOwnsDefaultBackendRoutingPolicy() {
    Assert.assertEquals(
        DefaultBackendRoutingPolicy.Policy.NAMESPACELESS_VALIDATION,
        DefaultBackendRoutingPolicy.policyFor("partition_name_has_valid_characters").orElse(null));
    Assert.assertEquals(
        DefaultBackendRoutingPolicy.Policy.TXN_AND_LOCK_LIFECYCLE,
        DefaultBackendRoutingPolicy.policyFor("open_txns").orElse(null));
    Assert.assertEquals(
        DefaultBackendRoutingPolicy.Policy.SESSION_COMPATIBILITY,
        DefaultBackendRoutingPolicy.policyFor("flushCache").orElse(null));
    Assert.assertTrue(DefaultBackendRoutingPolicy.policyFor("create_role").isEmpty());
  }

  @Test
  public void dataSemanticFallbacksOnlyApplyWhenTheBackendHasNoSuchMethod() {
    for (String method : DATA_SEMANTIC_FALLBACK_METHODS) {
      Assert.assertTrue(
          method + " must fall back when the backend does not implement it",
          MetastoreCompatibility.shouldUseFallback(
              method, new TApplicationException(TApplicationException.UNKNOWN_METHOD, "unsupported")));
      Assert.assertTrue(
          method + " must fall back when the loaded runtime has no such method",
          MetastoreCompatibility.shouldUseFallback(method, new NoSuchMethodException(method)));

      assertNoFallback(method, new TApplicationException(TApplicationException.INTERNAL_ERROR, "boom"));
      assertNoFallback(method, new TApplicationException(TApplicationException.MISSING_RESULT, "no result"));
      assertNoFallback(method, new TApplicationException(TApplicationException.INVALID_MESSAGE_TYPE, "desync"));
      assertNoFallback(method, new TApplicationException("unsupported"));
      assertNoFallback(method, new TTransportException("backend closed the connection"));
      assertNoFallback(method, new MetaException("backend catalog1 is unavailable"));
    }
  }

  @Test
  public void openTxnsNeverReportsAnEmptyTransactionListOnATransientFailure() {
    Assert.assertTrue(MetastoreCompatibility.fallback(
        "get_open_txns",
        new TApplicationException(TApplicationException.UNKNOWN_METHOD, "unsupported")).isPresent());
    Assert.assertTrue(MetastoreCompatibility.fallback(
        "get_open_txns",
        new TApplicationException(TApplicationException.INTERNAL_ERROR, "boom")).isEmpty());
    Assert.assertTrue(MetastoreCompatibility.fallback(
        "get_open_txns", new TTransportException("connection reset")).isEmpty());
  }

  @Test
  public void optionalServiceReadsStillFallBackOnAnyBackendFailure() {
    for (String method : OPTIONAL_SERVICE_FALLBACK_METHODS) {
      Assert.assertTrue(method, MetastoreCompatibility.shouldUseFallback(
          method, new TApplicationException(TApplicationException.UNKNOWN_METHOD, "unsupported")));
      Assert.assertTrue(method, MetastoreCompatibility.shouldUseFallback(
          method, new TApplicationException(TApplicationException.INTERNAL_ERROR, "boom")));
      Assert.assertTrue(method, MetastoreCompatibility.shouldUseFallback(
          method, new TTransportException("connection reset")));
      Assert.assertTrue(method, MetastoreCompatibility.shouldUseFallback(
          method, new MetaException("backend catalog1 is unavailable")));
      Assert.assertTrue(method, MetastoreCompatibility.fallback(method, new MetaException("boom")).isPresent());
    }
  }

  @Test
  public void methodsWithoutFallbackNeverFallBack() {
    Assert.assertFalse(MetastoreCompatibility.shouldUseFallback(
        "create_table", new TApplicationException(TApplicationException.UNKNOWN_METHOD, "unsupported")));
    Assert.assertFalse(MetastoreCompatibility.shouldUseFallback("create_table", new MetaException("boom")));
  }

  private static void assertNoFallback(String methodName, Throwable cause) {
    Assert.assertFalse(
        methodName + " must not answer with synthetic data for " + cause,
        MetastoreCompatibility.shouldUseFallback(methodName, cause));
    Assert.assertTrue(
        methodName + " must not answer with synthetic data for " + cause,
        MetastoreCompatibility.fallback(methodName, cause).isEmpty());
  }

  private static void assertRequiresFrontDoorSecurity(String methodName, Object[] args) {
    try {
      MetastoreCompatibility.handleLocally(methodName, args, null);
      Assert.fail("Expected MetaException for missing front door security");
    } catch (MetaException e) {
      Assert.assertTrue(e.getMessage().contains("Delegation tokens require Kerberos/SASL"));
    } catch (Exception e) {
      Assert.fail("Expected MetaException, got: " + e);
    }
  }
}
