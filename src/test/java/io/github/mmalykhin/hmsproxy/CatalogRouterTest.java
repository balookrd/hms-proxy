package io.github.mmalykhin.hmsproxy;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.junit.Assert;
import org.junit.Test;

public class CatalogRouterTest {
  private static final ProxyConfig TWO_CATALOG_CONFIG = new ProxyConfig(
      new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
      new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null),
      "catalog1",
      Map.of(
          "catalog1", new ProxyConfig.CatalogConfig("catalog1", "c1", "file:///c1", Map.of("hive.metastore.uris", "thrift://one")),
          "catalog2", new ProxyConfig.CatalogConfig("catalog2", "c2", "file:///c2", Map.of("hive.metastore.uris", "thrift://two"))));

  private static final ProxyConfig ONE_CATALOG_CONFIG = new ProxyConfig(
      new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
      new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null),
      "catalog1",
      Map.of("catalog1", new ProxyConfig.CatalogConfig("catalog1", "c1", "file:///c1", Map.of("hive.metastore.uris", "thrift://one"))));

  private static CatalogRouter routerFor(ProxyConfig config) {
    Map<String, CatalogBackend> backends = new LinkedHashMap<>();
    for (String name : config.catalogs().keySet()) {
      backends.put(name, null);
    }
    return new CatalogRouter(config, backends);
  }

  @Test
  public void resolvesLegacyCatalogPrefixedDbName() throws Exception {
    CatalogRouter router = routerFor(TWO_CATALOG_CONFIG);

    CatalogRouter.ResolvedNamespace namespace = router.resolveDatabase("catalog2.sales");

    Assert.assertEquals("catalog2", namespace.catalogName());
    Assert.assertEquals("sales", namespace.backendDbName());
    Assert.assertEquals("catalog2.sales", namespace.externalDbName());
  }

  @Test
  public void resolvesDatabaseInSingleCatalogWithoutPrefix() throws Exception {
    CatalogRouter router = routerFor(ONE_CATALOG_CONFIG);

    CatalogRouter.ResolvedNamespace namespace = router.resolveDatabase("sales");

    Assert.assertEquals("catalog1", namespace.catalogName());
    Assert.assertEquals("sales", namespace.backendDbName());
    Assert.assertEquals("catalog1.sales", namespace.externalDbName());
  }

  @Test
  public void rejectsUnprefixedDatabaseInMultiCatalog() {
    CatalogRouter router = routerFor(TWO_CATALOG_CONFIG);

    try {
      router.resolveDatabase("sales");
      Assert.fail("Expected MetaException for unqualified db name in multi-catalog mode");
    } catch (MetaException e) {
      Assert.assertTrue(e.getMessage().contains("catalog-qualified"));
    }
  }

  @Test
  public void resolvePatternReturnsPresentForKnownCatalogPrefix() {
    CatalogRouter router = routerFor(TWO_CATALOG_CONFIG);

    Optional<CatalogRouter.ResolvedNamespace> result = router.resolvePattern("catalog2.*");

    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("catalog2", result.get().catalogName());
    Assert.assertEquals("*", result.get().backendDbName());
  }

  @Test
  public void resolvePatternReturnsEmptyForUnknownPrefix() {
    CatalogRouter router = routerFor(TWO_CATALOG_CONFIG);

    Optional<CatalogRouter.ResolvedNamespace> result = router.resolvePattern("unknown.*");

    Assert.assertFalse(result.isPresent());
  }

  @Test
  public void resolvePatternReturnsEmptyForNullOrBlank() {
    CatalogRouter router = routerFor(TWO_CATALOG_CONFIG);

    Assert.assertFalse(router.resolvePattern(null).isPresent());
    Assert.assertFalse(router.resolvePattern("").isPresent());
    Assert.assertFalse(router.resolvePattern("  ").isPresent());
  }

  @Test
  public void externalDatabaseNameCombinesCatalogAndDb() {
    CatalogRouter router = routerFor(ONE_CATALOG_CONFIG);

    Assert.assertEquals("catalog1.sales", router.externalDatabaseName("catalog1", "sales"));
    Assert.assertEquals("catalog1", router.externalDatabaseName("catalog1", ""));
    Assert.assertEquals("catalog1", router.externalDatabaseName("catalog1", null));
  }

  @Test
  public void singleCatalogReturnsTrueOnlyForOneCatalog() {
    Assert.assertTrue(routerFor(ONE_CATALOG_CONFIG).singleCatalog());
    Assert.assertFalse(routerFor(TWO_CATALOG_CONFIG).singleCatalog());
  }

  @Test
  public void requireBackendThrowsForUnknownCatalog() {
    CatalogRouter router = routerFor(ONE_CATALOG_CONFIG);

    try {
      router.requireBackend("nonexistent");
      Assert.fail("Expected IllegalArgumentException for unknown catalog");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("nonexistent"));
    }
  }
}
