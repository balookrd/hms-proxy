package io.github.mmalykhin.hmsproxy.security.ranger;

import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.security.CatalogRangerConfig;
import io.github.mmalykhin.hmsproxy.config.security.RangerConfig;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
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

    io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics metrics =
        new io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics();

    RangerMetadataAuthorizer authorizer = new RangerMetadataAuthorizer(globalRanger, Map.of("cat1", catConfig), metrics) {
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

    // AD Group-based authorization tests (Policy 3 grants database 'analytics' to group 'analysts'):
    // Case 1: user with mixed-case AD group "Analysts"
    ImpersonationContext dan = new ImpersonationContext("dan", List.of("Analysts"));
    Assert.assertTrue(authorizer.isDatabaseAllowed("cat1", "analytics", dan));
    Assert.assertTrue(authorizer.isTableAllowed("cat1", "analytics", "metrics", dan));
    Assert.assertFalse(authorizer.isDatabaseAllowed("cat1", "sales", dan));

    // Case 2: user with LDAP DN group "CN=analysts,OU=Groups,DC=corp,DC=example,DC=com"
    ImpersonationContext erin = new ImpersonationContext("erin", List.of("CN=analysts,OU=Groups,DC=corp,DC=example,DC=com"));
    Assert.assertTrue(authorizer.isDatabaseAllowed("cat1", "analytics", erin));
    Assert.assertTrue(authorizer.isTableAllowed("cat1", "analytics", "metrics", erin));

    // Case 3: user with other group is denied
    ImpersonationContext frank = new ImpersonationContext("frank", List.of("other_group"));
    Assert.assertFalse(authorizer.isDatabaseAllowed("cat1", "analytics", frank));

    String rendered = metrics.render();
    Assert.assertTrue(rendered.contains(
        "hms_proxy_ranger_evaluations_total{catalog=\"cat1\",resource_type=\"database\",access_type=\"select\",result=\"allowed\"}"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_ranger_evaluations_total{catalog=\"cat1\",resource_type=\"database\",access_type=\"select\",result=\"denied\"}"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_ranger_evaluations_total{catalog=\"cat1\",resource_type=\"table\",access_type=\"select\",result=\"allowed\"}"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_ranger_filtered_objects_total{catalog=\"cat1\",resource_type=\"database\"}"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_ranger_filtered_objects_total{catalog=\"cat1\",resource_type=\"table\"}"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_ranger_plugin_info{catalog=\"cat1\",service_name=\"test_hive_svc\",service_type=\"hive\",app_id=\"hms-proxy\"} 1.0"));

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

    // Policy 3: group 'analysts' -> database: analytics, table: *
    RangerPolicy p3 = new RangerPolicy();
    p3.setId(3L);
    p3.setService(serviceName);
    p3.setName("analysts_group_policy");
    p3.setResources(Map.of(
        "database", new RangerPolicy.RangerPolicyResource("analytics"),
        "table", new RangerPolicy.RangerPolicyResource("*")
    ));
    RangerPolicy.RangerPolicyItem item3 = new RangerPolicy.RangerPolicyItem();
    item3.setGroups(List.of("analysts"));
    item3.setAccesses(List.of(new RangerPolicy.RangerPolicyItemAccess("select", true)));
    p3.setPolicyItems(List.of(item3));

    sp.setPolicies(List.of(p1, p2, p3));
    return sp;
  }

  @Test
  public void testConcurrentModificationExceptionRetrySuccess() {
    CatalogRangerConfig rangerConfig = new CatalogRangerConfig(
        true, null, "cme_svc", "hive", "hms-proxy", null, 30000L, 5000, 5000, null, null, null, false);
    RangerConfig globalRanger = new RangerConfig(true, rangerConfig, Map.of("cat1", rangerConfig));
    CatalogConfig catConfig = new CatalogConfig(
        "cat1", "cat1 desc", "file:///tmp/cat1", false,
        io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode.READ_WRITE,
        List.of(), io.github.mmalykhin.hmsproxy.config.catalog.CatalogExposureMode.ALLOW_ALL,
        List.of(), Map.of(),
        io.github.mmalykhin.hmsproxy.config.server.MetastoreRuntimeProfile.APACHE_3_1_3,
        null, Map.of(), 5000L, 10, 60000L, 10, 10, 60000L, rangerConfig);

    AtomicInteger callCount = new AtomicInteger();
    RangerMetadataAuthorizer authorizer = new RangerMetadataAuthorizer(globalRanger, Map.of("cat1", catConfig)) {
      @Override
      protected RangerAccessResult evaluateAccess(RangerBasePlugin plugin, RangerAccessRequest request) {
        int attempt = callCount.incrementAndGet();
        if (attempt == 1) {
          throw new ConcurrentModificationException("Simulated CME in Ranger policy engine");
        }
        RangerAccessResult res = new RangerAccessResult(0, "cme_svc", null, request);
        res.setIsAllowed(true);
        return res;
      }
    };

    ImpersonationContext user = new ImpersonationContext("alice", List.of());
    boolean allowed = authorizer.isDatabaseAllowed("cat1", "test_db", user);
    Assert.assertTrue("Should be allowed after successful retry", allowed);
    Assert.assertEquals("Should have called evaluateAccess twice (initial + retry)", 2, callCount.get());
  }

  @Test
  public void testConcurrentModificationExceptionSucceedsAfterTwoRetries() {
    CatalogRangerConfig rangerConfig = new CatalogRangerConfig(
        true, null, "cme_svc2", "hive", "hms-proxy", null, 30000L, 5000, 5000, null, null, null, false);
    RangerConfig globalRanger = new RangerConfig(true, rangerConfig, Map.of("cat1", rangerConfig));
    CatalogConfig catConfig = testCatalogConfig(rangerConfig);

    AtomicInteger callCount = new AtomicInteger();
    RangerMetadataAuthorizer authorizer = new RangerMetadataAuthorizer(globalRanger, Map.of("cat1", catConfig)) {
      @Override
      protected RangerAccessResult evaluateAccess(RangerBasePlugin plugin, RangerAccessRequest request) {
        if (callCount.incrementAndGet() < 3) {
          throw new ConcurrentModificationException("Transient CME failure in Ranger");
        }
        RangerAccessResult res = new RangerAccessResult(0, "cme_svc2", null, request);
        res.setIsAllowed(true);
        return res;
      }
    };

    ImpersonationContext user = new ImpersonationContext("alice", List.of());
    boolean allowed = authorizer.isDatabaseAllowed("cat1", "test_db", user);
    Assert.assertTrue("Should succeed on 3rd attempt (after 2 retries)", allowed);
    Assert.assertEquals(3, callCount.get());
    authorizer.close();
  }

  @Test
  public void testConcurrentModificationExceptionFailsClosedAfterExhaustingRetries() {
    CatalogRangerConfig rangerConfig = new CatalogRangerConfig(
        true, null, "cme_svc3", "hive", "hms-proxy", null, 30000L, 5000, 5000, null, null, null, false);
    RangerConfig globalRanger = new RangerConfig(true, rangerConfig, Map.of("cat1", rangerConfig));
    CatalogConfig catConfig = testCatalogConfig(rangerConfig);

    AtomicInteger callCount = new AtomicInteger();
    RangerMetadataAuthorizer authorizer = new RangerMetadataAuthorizer(globalRanger, Map.of("cat1", catConfig)) {
      @Override
      protected RangerAccessResult evaluateAccess(RangerBasePlugin plugin, RangerAccessRequest request) {
        callCount.incrementAndGet();
        throw new ConcurrentModificationException("Persistent CME failure in Ranger");
      }
    };

    ImpersonationContext user = new ImpersonationContext("alice", List.of());
    boolean allowed = authorizer.isDatabaseAllowed("cat1", "test_db", user);
    Assert.assertFalse("Must fail closed when all retries are exhausted", allowed);
    Assert.assertEquals("Should attempt 1 initial + 2 retries = 3 total attempts per access type (3*3=9)", 9, callCount.get());
    authorizer.close();
  }

  @Test
  public void testUnexpectedExceptionMissingImpersonationAndClosedAuthorizerFailClosed() {
    CatalogRangerConfig rangerConfig = new CatalogRangerConfig(
        true, null, "fail_closed_svc", "hive", "hms-proxy", null, 30000L, 5000, 5000, null, null, null, false);
    RangerConfig globalRanger = new RangerConfig(true, rangerConfig, Map.of("cat1", rangerConfig));
    CatalogConfig catConfig = testCatalogConfig(rangerConfig);

    RangerMetadataAuthorizer authorizer = new RangerMetadataAuthorizer(globalRanger, Map.of("cat1", catConfig)) {
      @Override
      protected RangerAccessResult evaluateAccess(RangerBasePlugin plugin, RangerAccessRequest request) {
        throw new IllegalStateException("Unexpected SDK NPE/failure");
      }
    };

    Assert.assertFalse(authorizer.isDatabaseAllowed("cat1", "sales", null));
    Assert.assertFalse(authorizer.isDatabaseAllowed("cat1", "sales", new ImpersonationContext(" ", List.of())));
    Assert.assertFalse(authorizer.isTableAllowed("cat1", "sales", "orders", null));
    Assert.assertEquals(List.of(), authorizer.filterDatabases("cat1", List.of("sales", "finance"), null));
    Assert.assertEquals(List.of(), authorizer.filterTables("cat1", "sales", List.of("orders"), null));

    ImpersonationContext alice = new ImpersonationContext("alice", List.of());
    Assert.assertFalse(authorizer.isDatabaseAllowed("cat1", "sales", alice));
    Assert.assertFalse(authorizer.isTableAllowed("cat1", "sales", "orders", alice));

    authorizer.close();
    Assert.assertFalse(authorizer.isDatabaseAllowed("cat1", "sales", alice));
    Assert.assertFalse(authorizer.isTableAllowed("cat1", "sales", "orders", alice));
    Assert.assertEquals(List.of(), authorizer.filterDatabases("cat1", List.of("sales"), alice));
    Assert.assertEquals(List.of(), authorizer.filterTables("cat1", "sales", List.of("orders"), alice));
  }

  private static CatalogConfig testCatalogConfig(CatalogRangerConfig rangerConfig) {
    return new CatalogConfig(
        "cat1", "cat1 desc", "file:///tmp/cat1", false,
        io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode.READ_WRITE,
        List.of(), io.github.mmalykhin.hmsproxy.config.catalog.CatalogExposureMode.ALLOW_ALL,
        List.of(), Map.of(),
        io.github.mmalykhin.hmsproxy.config.server.MetastoreRuntimeProfile.APACHE_3_1_3,
        null, Map.of(), 5000L, 10, 60000L, 10, 10, 60000L, rangerConfig);
  }

  @Test
  public void testRolesDisabledConfigurationUsesNoOpClient() {
    CatalogRangerConfig rangerConfig = new CatalogRangerConfig(
        true, null, "roles_off_svc", "hive", "hms-proxy", null, 30000L, 5000, 5000, null, null, null, false, false);
    RangerConfig globalRanger = new RangerConfig(true, rangerConfig, Map.of("cat1", rangerConfig));
    CatalogConfig catConfig = new CatalogConfig(
        "cat1", "cat1 desc", "file:///tmp/cat1", false,
        io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode.READ_WRITE,
        List.of(), io.github.mmalykhin.hmsproxy.config.catalog.CatalogExposureMode.ALLOW_ALL,
        List.of(), Map.of(),
        io.github.mmalykhin.hmsproxy.config.server.MetastoreRuntimeProfile.APACHE_3_1_3,
        null, Map.of(), 5000L, 10, 60000L, 10, 10, 60000L, rangerConfig);

    RangerMetadataAuthorizer authorizer = new RangerMetadataAuthorizer(globalRanger, Map.of("cat1", catConfig)) {
      @Override
      protected RangerBasePlugin createPlugin(String catalogName, CatalogRangerConfig config) {
        RangerBasePlugin plugin = super.createPlugin(catalogName, config);
        String policySourceImpl = plugin.getConfig().get("ranger.plugin.hive.policy.source.impl");
        Assert.assertEquals(NoOpRolesRangerAdminClient.class.getName(), policySourceImpl);
        return plugin;
      }
    };
    Assert.assertNotNull(authorizer);
    authorizer.close();
  }

  @Test
  public void testPropertiesOverrideXmlConfigAndFallbackRestUrl() throws Exception {
    java.nio.file.Path configDir = java.nio.file.Files.createTempDirectory("ranger-cfg-test");
    try {
      java.nio.file.Files.writeString(configDir.resolve("ranger-hive-security.xml"), """
          <configuration>
            <property>
              <name>ranger.plugin.hive.policy.rest.url</name>
              <value>http://xml-ranger:6080</value>
            </property>
          </configuration>
          """);
      CatalogRangerConfig inheritedXmlConfig = new CatalogRangerConfig(
          true, null, "xml_svc", "hive", "hms-proxy", null, 30000L, 5000, 5000, null, null,
          configDir.toString(), false, false);
      RangerConfig inheritedGlobal = new RangerConfig(true, inheritedXmlConfig, Map.of("cat1", inheritedXmlConfig));
      try (RangerMetadataAuthorizer authorizer =
          new RangerMetadataAuthorizer(inheritedGlobal, Map.of("cat1", testCatalogConfig(inheritedXmlConfig))) {
            @Override
            protected RangerBasePlugin createPlugin(String catalogName, CatalogRangerConfig config) {
              RangerBasePlugin plugin = super.createPlugin(catalogName, config);
              Assert.assertEquals("http://xml-ranger:6080",
                  plugin.getConfig().get("ranger.plugin.hive.policy.rest.url"));
              return plugin;
            }
          }) {
        Assert.assertNotNull(authorizer);
      }

      CatalogRangerConfig overrideConfig = new CatalogRangerConfig(
          true, "http://props-ranger:6180", "props_svc", "hive", "hms-proxy", null, 30000L, 5000, 5000,
          null, null, configDir.toString(), false, false);
      RangerConfig overrideGlobal = new RangerConfig(true, overrideConfig, Map.of("cat1", overrideConfig));
      try (RangerMetadataAuthorizer authorizer =
          new RangerMetadataAuthorizer(overrideGlobal, Map.of("cat1", testCatalogConfig(overrideConfig))) {
            @Override
            protected RangerBasePlugin createPlugin(String catalogName, CatalogRangerConfig config) {
              RangerBasePlugin plugin = super.createPlugin(catalogName, config);
              Assert.assertEquals("http://props-ranger:6180",
                  plugin.getConfig().get("ranger.plugin.hive.policy.rest.url"));
              return plugin;
            }
          }) {
        Assert.assertNotNull(authorizer);
      }
    } finally {
      java.nio.file.Files.deleteIfExists(configDir.resolve("ranger-hive-security.xml"));
      java.nio.file.Files.deleteIfExists(configDir);
    }
  }

  @Test
  public void testAuditEnabledAuditsFinalResultOnceAndExtractsRfc4514Cn() {
    CatalogRangerConfig rangerConfig = new CatalogRangerConfig(
        true, null, "audit_svc", "hive", "hms-proxy", null, 30000L, 5000, 5000, null, null, null,
        true, false);
    RangerConfig globalRanger = new RangerConfig(true, rangerConfig, Map.of("cat1", rangerConfig));
    CatalogConfig catConfig = testCatalogConfig(rangerConfig);

    java.util.List<String> auditedTypes = new java.util.ArrayList<>();
    try (RangerMetadataAuthorizer authorizer =
        new RangerMetadataAuthorizer(globalRanger, Map.of("cat1", catConfig)) {
          @Override
          protected RangerBasePlugin createPlugin(String catalogName, CatalogRangerConfig config) {
            RangerBasePlugin plugin = super.createPlugin(catalogName, config);
            Assert.assertNotNull("auditEnabled=true should configure a result processor",
                plugin.getResultProcessor());
            plugin.setResultProcessor(new org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor() {
              @Override
              public void processResult(RangerAccessResult result) {
                auditedTypes.add(result.getAccessRequest().getAccessType() + ":" + result.getIsAllowed());
              }

              @Override
              public void processResults(java.util.Collection<RangerAccessResult> results) {}
            });
            return plugin;
          }

          @Override
          protected RangerAccessResult evaluateAccess(RangerBasePlugin plugin, RangerAccessRequest request) {
            RangerAccessResult res = new RangerAccessResult(0, "audit_svc", null, request);
            res.setIsAllowed("read".equals(request.getAccessType()));
            return res;
          }
        }) {
      Assert.assertTrue(authorizer.isDatabaseAllowed("cat1", "sales",
          new ImpersonationContext("alice", List.of())));
      Assert.assertEquals("Fallback select->read must emit only one final audit event",
          List.of("read:true"), auditedTypes);
      // Idempotent close
      authorizer.close();
      authorizer.close();
    }

    Assert.assertEquals("Sales, EMEA",
        RangerMetadataAuthorizer.extractFirstCn("  cn = Sales\\, EMEA , OU=Groups "));
    Assert.assertEquals("team",
        RangerMetadataAuthorizer.extractFirstCn("CN=team+gid=42,OU=Groups"));
    Assert.assertNull(RangerMetadataAuthorizer.extractFirstCn("OU=Groups,CN=analysts"));
  }
}
