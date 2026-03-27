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
      new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
      ".",
      "catalog1",
      Map.of(
          "catalog1", new ProxyConfig.CatalogConfig("catalog1", "c1", "file:///c1", false, Map.of("hive.metastore.uris", "thrift://one")),
          "catalog2", new ProxyConfig.CatalogConfig("catalog2", "c2", "file:///c2", false, Map.of("hive.metastore.uris", "thrift://two"))));

  private static final ProxyConfig ONE_CATALOG_CONFIG = new ProxyConfig(
      new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
      new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
      ".",
      "catalog1",
      Map.of("catalog1", new ProxyConfig.CatalogConfig("catalog1", "c1", "file:///c1", false, Map.of("hive.metastore.uris", "thrift://one"))));

  private static final ProxyConfig CUSTOM_SEPARATOR_CONFIG = new ProxyConfig(
      new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
      new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
      "__",
      "catalog1",
      Map.of(
          "catalog1", new ProxyConfig.CatalogConfig("catalog1", "c1", "file:///c1", false, Map.of("hive.metastore.uris", "thrift://one")),
          "catalog2", new ProxyConfig.CatalogConfig("catalog2", "c2", "file:///c2", false, Map.of("hive.metastore.uris", "thrift://two"))));

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
    Assert.assertEquals("sales", namespace.externalDbName());
  }

  @Test
  public void resolvesUnprefixedDatabaseToDefaultCatalogInMultiCatalog() throws Exception {
    CatalogRouter router = routerFor(TWO_CATALOG_CONFIG);

    CatalogRouter.ResolvedNamespace namespace = router.resolveDatabase("sales");

    Assert.assertEquals("catalog1", namespace.catalogName());
    Assert.assertEquals("sales", namespace.backendDbName());
    Assert.assertEquals("sales", namespace.externalDbName());
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

    Assert.assertEquals("sales", router.externalDatabaseName("catalog1", "sales"));
    Assert.assertEquals("", router.externalDatabaseName("catalog1", ""));
    Assert.assertNull(router.externalDatabaseName("catalog1", null));
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

  @Test
  public void resolvesDatabaseUsingConfiguredSeparator() throws Exception {
    CatalogRouter router = routerFor(CUSTOM_SEPARATOR_CONFIG);

    CatalogRouter.ResolvedNamespace namespace = router.resolveDatabase("catalog2__sales");

    Assert.assertEquals("catalog2", namespace.catalogName());
    Assert.assertEquals("sales", namespace.backendDbName());
    Assert.assertEquals("catalog2__sales", namespace.externalDbName());
  }

  @Test
  public void externalDatabaseNameUsesConfiguredSeparator() {
    CatalogRouter router = routerFor(CUSTOM_SEPARATOR_CONFIG);

    Assert.assertEquals("sales", router.externalDatabaseName("catalog1", "sales"));
    Assert.assertEquals("catalog2__sales", router.externalDatabaseName("catalog2", "sales"));
  }

  @Test
  public void resolveCatalogIfKnownReturnsPresentOnlyForConfiguredCatalogs() {
    CatalogRouter router = routerFor(TWO_CATALOG_CONFIG);

    Assert.assertTrue(router.resolveCatalogIfKnown("catalog2", "sales").isPresent());
    Assert.assertFalse(router.resolveCatalogIfKnown("hive", "sales").isPresent());
  }

  @Test
  public void resolvesHivePrefixedExternalDatabaseNameForCompatibility() throws Exception {
    CatalogRouter router = routerFor(CUSTOM_SEPARATOR_CONFIG);

    CatalogRouter.ResolvedNamespace namespace = router.resolveDatabase("hive.catalog2__default");

    Assert.assertEquals("catalog2", namespace.catalogName());
    Assert.assertEquals("default", namespace.backendDbName());
    Assert.assertEquals("catalog2__default", namespace.externalDbName());
  }

  @Test
  public void resolvesHivePrefixedDefaultDatabaseToDefaultCatalog() throws Exception {
    CatalogRouter router = routerFor(CUSTOM_SEPARATOR_CONFIG);

    CatalogRouter.ResolvedNamespace namespace = router.resolveDatabase("hive.default");

    Assert.assertEquals("catalog1", namespace.catalogName());
    Assert.assertEquals("default", namespace.backendDbName());
    Assert.assertEquals("default", namespace.externalDbName());
  }

  @Test
  public void resolvesDoublePrefixedExternalDatabaseNameForCompatibility() throws Exception {
    CatalogRouter router = routerFor(CUSTOM_SEPARATOR_CONFIG);

    CatalogRouter.ResolvedNamespace namespace = router.resolveDatabase("catalog2.catalog2__default");

    Assert.assertEquals("catalog2", namespace.catalogName());
    Assert.assertEquals("default", namespace.backendDbName());
    Assert.assertEquals("catalog2__default", namespace.externalDbName());
  }

  @Test
  public void resolvesAtCatalogPrefixedExternalDatabaseNameForCompatibility() throws Exception {
    CatalogRouter router = routerFor(CUSTOM_SEPARATOR_CONFIG);

    CatalogRouter.ResolvedNamespace namespace = router.resolveDatabase("@hive#catalog2__default");

    Assert.assertEquals("catalog2", namespace.catalogName());
    Assert.assertEquals("default", namespace.backendDbName());
    Assert.assertEquals("catalog2__default", namespace.externalDbName());
  }

  @Test
  public void resolvesAtCatalogPrefixedDefaultDatabaseToDefaultCatalog() throws Exception {
    CatalogRouter router = routerFor(CUSTOM_SEPARATOR_CONFIG);

    CatalogRouter.ResolvedNamespace namespace = router.resolveDatabase("@hive#default");

    Assert.assertEquals("catalog1", namespace.catalogName());
    Assert.assertEquals("default", namespace.backendDbName());
    Assert.assertEquals("default", namespace.externalDbName());
  }

  @Test
  public void resolvesDefaultCatalogPrefixedDatabaseNameAsLiteralName() throws Exception {
    CatalogRouter router = routerFor(CUSTOM_SEPARATOR_CONFIG);

    CatalogRouter.ResolvedNamespace namespace = router.resolveDatabase("catalog1__sales");

    Assert.assertEquals("catalog1", namespace.catalogName());
    Assert.assertEquals("sales", namespace.backendDbName());
    Assert.assertEquals("catalog1__sales", namespace.externalDbName());
  }

  @Test
  public void resolvesHivePrefixedDefaultCatalogPrefixedDatabaseNameForCompatibility() throws Exception {
    CatalogRouter router = routerFor(CUSTOM_SEPARATOR_CONFIG);

    CatalogRouter.ResolvedNamespace namespace = router.resolveDatabase("hive.catalog1__default");

    Assert.assertEquals("catalog1", namespace.catalogName());
    Assert.assertEquals("default", namespace.backendDbName());
    Assert.assertEquals("catalog1__default", namespace.externalDbName());
  }

  @Test
  public void resolvePatternRoutesDefaultCatalogPrefixToMatchingCatalog() {
    CatalogRouter router = routerFor(CUSTOM_SEPARATOR_CONFIG);

    Optional<CatalogRouter.ResolvedNamespace> result = router.resolvePattern("catalog1__*");

    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("catalog1", result.get().catalogName());
    Assert.assertEquals("*", result.get().backendDbName());
    Assert.assertEquals("catalog1__*", result.get().externalDbName());
  }
}
