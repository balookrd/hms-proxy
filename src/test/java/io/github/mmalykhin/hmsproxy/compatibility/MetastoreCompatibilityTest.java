package io.github.mmalykhin.hmsproxy.compatibility;

import io.github.mmalykhin.hmsproxy.routing.DefaultBackendRoutingPolicy;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.junit.Assert;
import org.junit.Test;

public class MetastoreCompatibilityTest {
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
