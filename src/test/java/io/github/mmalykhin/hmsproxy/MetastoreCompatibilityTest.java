package io.github.mmalykhin.hmsproxy;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesResult;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TApplicationException;
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
  public void downgradesGetTableReqToLegacyGetTableCall() throws Throwable {
    GetTableRequest request = new GetTableRequest("sales", "events");
    request.setCatName("hive");
    AtomicReference<String> invokedMethod = new AtomicReference<>();

    Object downgraded = MetastoreCompatibility.downgradeRequest(
        "get_table_req",
        new Object[] {request},
        (methodName, parameterTypes, args) -> {
          invokedMethod.set(methodName);
          Assert.assertArrayEquals(new Class<?>[] {String.class, String.class}, parameterTypes);
          Assert.assertArrayEquals(new Object[] {"sales", "events"}, args);
          Table table = new Table();
          table.setDbName("sales");
          table.setTableName("events");
          return table;
        },
        new TApplicationException(TApplicationException.UNKNOWN_METHOD, "Invalid method name: 'get_table_req'"))
        .orElseThrow();

    Assert.assertEquals("get_table", invokedMethod.get());
    Assert.assertTrue(downgraded instanceof GetTableResult);
    Assert.assertEquals("sales", ((GetTableResult) downgraded).getTable().getDbName());
    Assert.assertEquals("events", ((GetTableResult) downgraded).getTable().getTableName());
  }

  @Test
  public void downgradesGetTableObjectsByNameReqToLegacyCall() throws Throwable {
    GetTablesRequest request = new GetTablesRequest("sales");
    request.setTblNames(List.of("events", "orders"));
    AtomicReference<String> invokedMethod = new AtomicReference<>();

    Object downgraded = MetastoreCompatibility.downgradeRequest(
        "get_table_objects_by_name_req",
        new Object[] {request},
        (methodName, parameterTypes, args) -> {
          invokedMethod.set(methodName);
          Assert.assertArrayEquals(new Class<?>[] {String.class, List.class}, parameterTypes);
          Assert.assertArrayEquals(new Object[] {"sales", List.of("events", "orders")}, args);
          Table first = new Table();
          first.setDbName("sales");
          first.setTableName("events");
          Table second = new Table();
          second.setDbName("sales");
          second.setTableName("orders");
          return List.of(first, second);
        },
        new TApplicationException(TApplicationException.UNKNOWN_METHOD,
            "Invalid method name: 'get_table_objects_by_name_req'"))
        .orElseThrow();

    Assert.assertEquals("get_table_objects_by_name", invokedMethod.get());
    Assert.assertTrue(downgraded instanceof GetTablesResult);
    Assert.assertEquals(2, ((GetTablesResult) downgraded).getTablesSize());
  }

  @Test
  public void doesNotDowngradeNonApplicationFailures() throws Throwable {
    GetTableRequest request = new GetTableRequest("sales", "events");

    Assert.assertTrue(MetastoreCompatibility.downgradeRequest(
        "get_table_req",
        new Object[] {request},
        (methodName, parameterTypes, args) -> null,
        new MetaException("backend failure")).isEmpty());
  }

  @Test
  public void detectsLegacyBackendProfileFromHortonworksVersion() {
    Assert.assertEquals(
        MetastoreCompatibility.BackendProfile.HORTONWORKS_3_1_0_LEGACY_REQUESTS,
        MetastoreCompatibility.backendProfile("3.1.0.3.1.0.0-78"));
    Assert.assertTrue(MetastoreCompatibility.usesLegacyRequestApi(
        MetastoreCompatibility.backendProfile("3.1.0.3.1.0.0-78")));
  }

  @Test
  public void detectsModernBackendProfileFromApacheVersion() {
    Assert.assertEquals(
        MetastoreCompatibility.BackendProfile.MODERN_REQUESTS,
        MetastoreCompatibility.backendProfile("3.1.3"));
    Assert.assertFalse(MetastoreCompatibility.usesLegacyRequestApi(
        MetastoreCompatibility.backendProfile("3.1.3")));
  }

  @Test
  public void resolvesRuntimeProfileFromBackendVersion() {
    Assert.assertEquals(
        MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78,
        MetastoreRuntimeProfileResolver.forBackendVersion("3.1.0.3.1.0.0-78"));
    Assert.assertEquals(
        MetastoreRuntimeProfile.APACHE_3_1_3,
        MetastoreRuntimeProfileResolver.forBackendVersion("3.1.3"));
    Assert.assertEquals(
        MetastoreRuntimeProfile.APACHE_3_1_3,
        MetastoreRuntimeProfileResolver.forBackendVersion(null));
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
