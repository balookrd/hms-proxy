package io.github.mmalykhin.hmsproxy;

import java.util.Map;
import java.util.LinkedHashMap;
import org.junit.Assert;
import org.junit.Test;

public class CatalogRouterTest {
  @Test
  public void resolvesLegacyCatalogPrefixedDbName() throws Exception {
    ProxyConfig config = new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null),
        "catalog1",
        Map.of(
            "catalog1", new ProxyConfig.CatalogConfig("catalog1", "c1", "file:///c1", Map.of("hive.metastore.uris", "thrift://one")),
            "catalog2", new ProxyConfig.CatalogConfig("catalog2", "c2", "file:///c2", Map.of("hive.metastore.uris", "thrift://two"))));

    Map<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", null);
    backends.put("catalog2", null);
    CatalogRouter router = new CatalogRouter(config, backends);

    CatalogRouter.ResolvedNamespace namespace = router.resolveDatabase("catalog2.sales");

    Assert.assertEquals("catalog2", namespace.catalogName());
    Assert.assertEquals("sales", namespace.backendDbName());
    Assert.assertEquals("catalog2.sales", namespace.externalDbName());
  }
}
