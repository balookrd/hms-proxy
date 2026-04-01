package io.github.mmalykhin.hmsproxy.compatibility;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.junit.Assert;
import org.junit.Test;

public class MetastoreCompatibilityTest {
  @Test
  public void delegationTokenMethodsAreHandledLocally() {
    Assert.assertTrue(MetastoreCompatibility.handlesLocally("get_delegation_token"));
    Assert.assertTrue(MetastoreCompatibility.handlesLocally("renew_delegation_token"));
    Assert.assertTrue(MetastoreCompatibility.handlesLocally("cancel_delegation_token"));
    Assert.assertFalse(MetastoreCompatibility.handlesLocally("set_ugi"));
  }

  @Test
  public void localDelegationTokenHandlingRequiresKerberosFrontDoor() {
    assertRequiresFrontDoorSecurity("get_delegation_token", new Object[] {"alice", "hive"});
    assertRequiresFrontDoorSecurity("renew_delegation_token", new Object[] {"token"});
    assertRequiresFrontDoorSecurity("cancel_delegation_token", new Object[] {"token"});
  }

  @Test
  public void defaultBackendRoutingPoliciesExplainNamespaceFreeCompatibilityPaths() {
    Assert.assertEquals(
        MetastoreCompatibility.DefaultBackendRoutingPolicy.NAMESPACELESS_VALIDATION,
        MetastoreCompatibility.defaultBackendRoutingPolicy("partition_name_has_valid_characters").orElse(null));
    Assert.assertEquals(
        MetastoreCompatibility.DefaultBackendRoutingPolicy.TXN_AND_LOCK_LIFECYCLE,
        MetastoreCompatibility.defaultBackendRoutingPolicy("open_txns").orElse(null));
    Assert.assertEquals(
        MetastoreCompatibility.DefaultBackendRoutingPolicy.SESSION_COMPATIBILITY,
        MetastoreCompatibility.defaultBackendRoutingPolicy("flushCache").orElse(null));
    Assert.assertTrue(MetastoreCompatibility.defaultBackendRoutingPolicy("create_role").isEmpty());
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
