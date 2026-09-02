package io.github.mmalykhin.hmsproxy.security.ranger;

import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.security.CatalogRangerConfig;
import io.github.mmalykhin.hmsproxy.config.security.RangerConfig;
import java.util.List;
import java.util.Map;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.Assert;
import org.junit.Test;

public class RangerMetadataAuthorizerTest {

  @Test
  public void testNoOpAuthorizerAllowsAll() {
    NoOpMetadataAuthorizer authorizer = NoOpMetadataAuthorizer.INSTANCE;
    ImpersonationContext user = new ImpersonationContext("alice", List.of("group1"));

    Assert.assertTrue(authorizer.isDatabaseAllowed("cat1", "db1", user));
    Assert.assertTrue(authorizer.isTableAllowed("cat1", "db1", "tbl1", user));

    List<String> dbs = authorizer.filterDatabases("cat1", List.of("db1", "db2"), user);
    Assert.assertEquals(List.of("db1", "db2"), dbs);

    List<String> tbls = authorizer.filterTables("cat1", "db1", List.of("t1", "t2"), user);
    Assert.assertEquals(List.of("t1", "t2"), tbls);

    authorizer.close();
  }

  @Test
  public void testRangerAuthorizerWithInjectedPolicies() {
    CatalogRangerConfig rangerConfig = new CatalogRangerConfig(
        true, null, "test_hive_svc", "hive", "hms-proxy", null, 30000L, 5000, 5000, null, null, null, false);
    RangerConfig globalRanger = new RangerConfig(
        true, rangerConfig, Map.of("cat1", rangerConfig));

    CatalogConfig catConfig = new CatalogConfig(
        "cat1", "cat1 desc", "file:///tmp/cat1", false,
        io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode.READ_WRITE,
        List.of(), io.github.mmalykhin.hmsproxy.config.catalog.CatalogExposureMode.ALLOW_ALL,
        List.of(), Map.of(),
        io.github.mmalykhin.hmsproxy.config.server.MetastoreRuntimeProfile.APACHE_3_1_3,
        null, Map.of(), 5000L, 10, 60000L, 10, 10, 60000L, rangerConfig);

    ServicePolicies servicePolicies = buildServicePolicies("test_hive_svc");

    RangerMetadataAuthorizer authorizer = new RangerMetadataAuthorizer(globalRanger, Map.of("cat1", catConfig)) {
      @Override
      protected RangerBasePlugin createPlugin(String catalogName, CatalogRangerConfig config) {
        RangerBasePlugin plugin = super.createPlugin(catalogName, config);
        plugin.setPolicies(servicePolicies);
        return plugin;
      }
    };

    ImpersonationContext alice = new ImpersonationContext("alice", List.of("sales_grp"));
    ImpersonationContext bob = new ImpersonationContext("bob", List.of("finance_grp"));
    ImpersonationContext eve = new ImpersonationContext("eve", List.of("other_grp"));

    // Database authorization
    Assert.assertTrue(authorizer.isDatabaseAllowed("cat1", "sales", alice));
    Assert.assertFalse(authorizer.isDatabaseAllowed("cat1", "finance", alice));

    Assert.assertFalse(authorizer.isDatabaseAllowed("cat1", "sales", bob));
    Assert.assertTrue(authorizer.isDatabaseAllowed("cat1", "finance", bob));

    Assert.assertFalse(authorizer.isDatabaseAllowed("cat1", "sales", eve));
    Assert.assertFalse(authorizer.isDatabaseAllowed("cat1", "finance", eve));

    // Database listing filtering
    List<String> allDbs = List.of("sales", "finance", "secret_db");
    Assert.assertEquals(List.of("sales"), authorizer.filterDatabases("cat1", allDbs, alice));
    Assert.assertEquals(List.of("finance"), authorizer.filterDatabases("cat1", allDbs, bob));
    Assert.assertEquals(List.of(), authorizer.filterDatabases("cat1", allDbs, eve));

    // Table authorization
    Assert.assertTrue(authorizer.isTableAllowed("cat1", "sales", "orders", alice));
    Assert.assertTrue(authorizer.isTableAllowed("cat1", "sales", "customers", alice));
    Assert.assertFalse(authorizer.isTableAllowed("cat1", "finance", "reports", alice));

    Assert.assertTrue(authorizer.isTableAllowed("cat1", "finance", "reports", bob));
    Assert.assertFalse(authorizer.isTableAllowed("cat1", "finance", "salaries", bob)); // only reports allowed

    // Table listing filtering
    List<String> financeTables = List.of("reports", "salaries");
    Assert.assertEquals(List.of("reports"), authorizer.filterTables("cat1", "finance", financeTables, bob));
    Assert.assertEquals(List.of(), authorizer.filterTables("cat1", "finance", financeTables, eve));

    authorizer.close();
  }

  private ServicePolicies buildServicePolicies(String serviceName) {
    ServicePolicies sp = new ServicePolicies();
    sp.setServiceName(serviceName);
    sp.setServiceId(1L);

    RangerServiceDef sd = new RangerServiceDef();
    sd.setName("hive");
    sd.setId(1L);

    RangerServiceDef.RangerResourceDef dbRes = new RangerServiceDef.RangerResourceDef();
    dbRes.setItemId(1L);
    dbRes.setName("database");
    dbRes.setLevel(10);

    RangerServiceDef.RangerResourceDef tblRes = new RangerServiceDef.RangerResourceDef();
    tblRes.setItemId(2L);
    tblRes.setName("table");
    tblRes.setLevel(20);
    tblRes.setParent("database");

    sd.setResources(List.of(dbRes, tblRes));

    RangerServiceDef.RangerAccessTypeDef selectAcc = new RangerServiceDef.RangerAccessTypeDef(1L, "select", "select", null, null);
    RangerServiceDef.RangerAccessTypeDef readAcc = new RangerServiceDef.RangerAccessTypeDef(2L, "read", "read", null, null);
    RangerServiceDef.RangerAccessTypeDef useAcc = new RangerServiceDef.RangerAccessTypeDef(3L, "use", "use", null, null);

    sd.setAccessTypes(List.of(selectAcc, readAcc, useAcc));
    sp.setServiceDef(sd);

    // Policy 1: alice -> database: sales, table: *
    RangerPolicy p1 = new RangerPolicy();
    p1.setId(1L);
    p1.setService(serviceName);
    p1.setName("sales_policy");
    p1.setResources(Map.of(
        "database", new RangerPolicy.RangerPolicyResource("sales"),
        "table", new RangerPolicy.RangerPolicyResource("*")
    ));
    RangerPolicy.RangerPolicyItem item1 = new RangerPolicy.RangerPolicyItem();
    item1.setUsers(List.of("alice"));
    item1.setAccesses(List.of(new RangerPolicy.RangerPolicyItemAccess("select", true)));
    p1.setPolicyItems(List.of(item1));

    // Policy 2: bob -> database: finance, table: reports
    RangerPolicy p2 = new RangerPolicy();
    p2.setId(2L);
    p2.setService(serviceName);
    p2.setName("finance_policy");
    p2.setResources(Map.of(
        "database", new RangerPolicy.RangerPolicyResource("finance"),
        "table", new RangerPolicy.RangerPolicyResource("reports")
    ));
    RangerPolicy.RangerPolicyItem item2 = new RangerPolicy.RangerPolicyItem();
    item2.setUsers(List.of("bob"));
    item2.setAccesses(List.of(new RangerPolicy.RangerPolicyItemAccess("select", true)));
    p2.setPolicyItems(List.of(item2));

    sp.setPolicies(List.of(p1, p2));
    return sp;
  }
}
