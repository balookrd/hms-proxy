package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityMode;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

public class CatalogRouterPatternTest {
  private static final ProxyConfig CONFIG = ProxyConfig.builder()
      .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
      .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
      .catalogDbSeparator("__")
      .defaultCatalog("catalog1")
      .catalogs(Map.of(
          "catalog1", new CatalogConfig(
              "catalog1", "c1", "file:///c1", false, CatalogAccessMode.READ_WRITE, java.util.List.of(), null, null,
              Map.of("hive.metastore.uris", "thrift://one")),
          "catalog2", new CatalogConfig(
              "catalog2", "c2", "file:///c2", false, CatalogAccessMode.READ_WRITE, java.util.List.of(), null, null,
              Map.of("hive.metastore.uris", "thrift://two"))))
      .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
      .build();

  private static CatalogRouter router() {
    Map<String, CatalogBackend> backends = new LinkedHashMap<>();
    for (String name : CONFIG.catalogs().keySet()) {
      backends.put(name, null);
    }
    return new CatalogRouter(CONFIG, backends);
  }

  @Test
  public void backendPatternForRemoteCatalogTranslatesWildcardPrefixes() {
    CatalogRouter router = router();

    // Partial catalog prefix followed by wildcard matches any database in catalog2
    Assert.assertEquals(Optional.of("*"), router.backendDatabasePattern("catalog2", "catalog2*"));
    Assert.assertEquals(Optional.of("*"), router.backendDatabasePattern("catalog2", "catalog2_*"));
    Assert.assertEquals(Optional.of("*"), router.backendDatabasePattern("catalog2", "catalog2%"));
    Assert.assertEquals(Optional.of("*"), router.backendDatabasePattern("catalog2", "catalog2_%"));
    Assert.assertEquals(Optional.of("*"), router.backendDatabasePattern("catalog2", "cat*"));

    // Full separator prefix stripped down to remainder
    Assert.assertEquals(Optional.of("*"), router.backendDatabasePattern("catalog2", "catalog2__*"));
    Assert.assertEquals(Optional.of("*"), router.backendDatabasePattern("catalog2", "catalog2..*"));
    Assert.assertEquals(Optional.of("sales*"), router.backendDatabasePattern("catalog2", "catalog2__sales*"));
    Assert.assertEquals(Optional.of("sales*"), router.backendDatabasePattern("catalog2", "catalog2..sales*"));
    Assert.assertEquals(Optional.of("sales%"), router.backendDatabasePattern("catalog2", "catalog2__sales%"));

    // General wildcards preserved
    Assert.assertEquals(Optional.of("*"), router.backendDatabasePattern("catalog2", "*"));
    Assert.assertEquals(Optional.of(".*"), router.backendDatabasePattern("catalog2", ".*"));
    Assert.assertEquals(Optional.of("%"), router.backendDatabasePattern("catalog2", "%"));
    Assert.assertEquals(Optional.of("*sales*"), router.backendDatabasePattern("catalog2", "*sales*"));

    // Hive 3 transport framing with @cat#
    Assert.assertEquals(Optional.of("*"), router.backendDatabasePattern("catalog2", "@hive#"));
    Assert.assertEquals(Optional.of("*"), router.backendDatabasePattern("catalog2", "@hive#*"));
    Assert.assertEquals(Optional.of("*"), router.backendDatabasePattern("catalog2", "@hive#catalog2*"));
    Assert.assertEquals(Optional.of("*"), router.backendDatabasePattern("catalog2", "@hive#catalog2_*"));
    Assert.assertEquals(Optional.of("*"), router.backendDatabasePattern("catalog2", "@hive#catalog2__*"));
    Assert.assertEquals(Optional.of("sales*"), router.backendDatabasePattern("catalog2", "@hive#catalog2__sales*"));
    Assert.assertEquals(Optional.of("*sales*"), router.backendDatabasePattern("catalog2", "@hive#*sales*"));
    Assert.assertEquals(Optional.of("%sales%"), router.backendDatabasePattern("catalog2", "@hive#%sales%"));

    // Unmatched pattern returns empty for catalog2
    Assert.assertEquals(Optional.empty(), router.backendDatabasePattern("catalog2", "sales*"));
    Assert.assertEquals(Optional.empty(), router.backendDatabasePattern("catalog2", "catalog1__*"));
    Assert.assertEquals(Optional.empty(), router.backendDatabasePattern("catalog2", "other*"));
    Assert.assertEquals(Optional.empty(), router.backendDatabasePattern("catalog2", "@hive#sales*"));
    Assert.assertEquals(Optional.empty(), router.backendDatabasePattern("catalog2", "@hive#catalog1__*"));
  }

  @Test
  public void backendPatternForDefaultCatalogPreservesLocalPatterns() {
    CatalogRouter router = router();

    Assert.assertEquals(Optional.of("sales*"), router.backendDatabasePattern("catalog1", "sales*"));
    Assert.assertEquals(Optional.of("catalog2*"), router.backendDatabasePattern("catalog1", "catalog2*"));
    Assert.assertEquals(Optional.of("*"), router.backendDatabasePattern("catalog1", "*"));

    // Strict prefix for another catalog is skipped on default
    Assert.assertEquals(Optional.empty(), router.backendDatabasePattern("catalog1", "catalog2__sales*"));
    Assert.assertEquals(Optional.empty(), router.backendDatabasePattern("catalog1", "catalog2..sales*"));
  }

  @Test
  public void matchesHivePatternHandlesHiveAndSqlWildcards() {
    // Glob asterisk
    Assert.assertTrue(CatalogRouter.matchesHivePattern("catalog2__sales", "catalog2*"));
    Assert.assertTrue(CatalogRouter.matchesHivePattern("catalog2__sales", "catalog2_*"));
    Assert.assertTrue(CatalogRouter.matchesHivePattern("catalog2__sales", "catalog2__*"));
    Assert.assertFalse(CatalogRouter.matchesHivePattern("catalog2_backup", "catalog2__*"));

    // SQL percent
    Assert.assertTrue(CatalogRouter.matchesHivePattern("catalog2__sales", "catalog2%"));
    Assert.assertTrue(CatalogRouter.matchesHivePattern("catalog2__sales", "catalog2_%"));

    // Case insensitivity
    Assert.assertTrue(CatalogRouter.matchesHivePattern("CATALOG2__SALES", "catalog2*"));
    Assert.assertTrue(CatalogRouter.matchesHivePattern("catalog2__sales", "CATALOG2*"));

    // Pipe alternation
    Assert.assertTrue(CatalogRouter.matchesHivePattern("catalog1__db", "catalog1*|catalog2*"));
    Assert.assertTrue(CatalogRouter.matchesHivePattern("catalog2__db", "catalog1*|catalog2*"));
    Assert.assertFalse(CatalogRouter.matchesHivePattern("catalog3__db", "catalog1*|catalog2*"));

    // Substring wildcards
    Assert.assertTrue(CatalogRouter.matchesHivePattern("catalog2__sales_daily", "*sales*"));
    Assert.assertFalse(CatalogRouter.matchesHivePattern("catalog2__dwh", "*sales*"));
  }

  @Test
  public void normalizeDatabasePatternStripsHiveFraming() {
    CatalogRouter router = router();

    Assert.assertEquals("*", router.normalizeDatabasePattern(null));
    Assert.assertEquals("*", router.normalizeDatabasePattern(""));
    Assert.assertEquals("*", router.normalizeDatabasePattern("   "));
    Assert.assertEquals("*", router.normalizeDatabasePattern("*"));
    Assert.assertEquals("*", router.normalizeDatabasePattern("@hive#"));
    Assert.assertEquals("*", router.normalizeDatabasePattern("@hive#*"));
    Assert.assertEquals("catalog2*", router.normalizeDatabasePattern("@hive#catalog2*"));
    Assert.assertEquals("catalog2__*", router.normalizeDatabasePattern("@hive#catalog2__*"));
    Assert.assertEquals("catalog2_%", router.normalizeDatabasePattern("@hive#catalog2_%"));
    Assert.assertEquals("sales*", router.normalizeDatabasePattern("@hive#sales*"));
    Assert.assertEquals("catalog2__sales*", router.normalizeDatabasePattern("spark_catalog.catalog2__sales*"));
  }

  @Test
  public void canMatchRemoteCatalogsEvaluatesWildcardsAndCatalogFraming() {
    CatalogRouter router = router();

    Assert.assertTrue(router.canMatchRemoteCatalogs(null));
    Assert.assertTrue(router.canMatchRemoteCatalogs(""));
    Assert.assertTrue(router.canMatchRemoteCatalogs("*"));
    Assert.assertTrue(router.canMatchRemoteCatalogs("@hive#"));
    Assert.assertTrue(router.canMatchRemoteCatalogs("@hive#*"));
    Assert.assertTrue(router.canMatchRemoteCatalogs("@hive#catalog2*"));
    Assert.assertTrue(router.canMatchRemoteCatalogs("@hive#catalog2_%"));
    Assert.assertTrue(router.canMatchRemoteCatalogs("@hive#catalog2__*"));
    Assert.assertTrue(router.canMatchRemoteCatalogs("catalog2*"));
    Assert.assertTrue(router.canMatchRemoteCatalogs("catalog2__*"));

    Assert.assertFalse(router.canMatchRemoteCatalogs("@hive#sales*"));
    Assert.assertFalse(router.canMatchRemoteCatalogs("sales*"));
    Assert.assertFalse(router.canMatchRemoteCatalogs("catalog1__*"));
  }
}
