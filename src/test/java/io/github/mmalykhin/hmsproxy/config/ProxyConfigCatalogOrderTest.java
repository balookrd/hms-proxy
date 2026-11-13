package io.github.mmalykhin.hmsproxy.config;

import io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogExposureMode;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityMode;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

// Every catalog walk (router backends, readiness probes, fan-out, merge-with-default precedence)
// follows config.catalogs(). It used to be Map.copyOf, whose iteration order is salted per JVM
// start, so the walk order changed from one proxy restart to the next.
public class ProxyConfigCatalogOrderTest {

  // Eight catalogs, so a salted order matches the declared one by chance only once in 8! starts.
  private static final List<String> DECLARED =
      List.of("zeta", "hdp", "apache", "omega", "beta", "kappa", "alpha", "delta");

  @Test
  public void catalogsKeepTheDeclaredOrder() {
    Map<String, CatalogConfig> catalogs = new LinkedHashMap<>();
    for (String name : DECLARED) {
      catalogs.put(name, catalog(name));
    }

    ProxyConfig config = config(catalogs);

    Assert.assertEquals(DECLARED, new ArrayList<>(config.catalogs().keySet()));
  }

  @Test
  public void catalogsStayImmutable() {
    Map<String, CatalogConfig> catalogs = new LinkedHashMap<>();
    catalogs.put("hdp", catalog("hdp"));
    ProxyConfig config = config(catalogs);

    catalogs.put("late", catalog("late"));

    Assert.assertEquals(List.of("hdp"), new ArrayList<>(config.catalogs().keySet()));
    Assert.assertThrows(UnsupportedOperationException.class,
        () -> config.catalogs().put("other", catalog("other")));
  }

  @Test
  public void catalogsRejectNullsLikeMapCopyOf() {
    Map<String, CatalogConfig> catalogs = new LinkedHashMap<>();
    catalogs.put("hdp", null);

    Assert.assertThrows(NullPointerException.class, () -> config(catalogs));
  }

  private static ProxyConfig config(Map<String, CatalogConfig> catalogs) {
    return ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog(catalogs.keySet().iterator().next())
        .catalogs(catalogs)
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();
  }

  private static CatalogConfig catalog(String name) {
    return new CatalogConfig(
        name, name, "file:///" + name, false, CatalogAccessMode.READ_WRITE, List.of(),
        CatalogExposureMode.ALLOW_ALL, List.of(), Map.of(), List.of(), false, null, null,
        Map.of("hive.metastore.uris", "thrift://" + name));
  }
}
