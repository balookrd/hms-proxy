package io.github.mmalykhin.hmsproxy;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Assert;
import org.junit.Test;

public class ProxyConfigLoaderTest {
  @Test
  public void loadsCatalogsAndDefaultRouting() throws Exception {
    Path file = Files.createTempFile("hms-proxy", ".properties");
    Files.writeString(file, """
        server.port=9088
        catalogs=catalog1,catalog2
        routing.default-catalog=catalog1
        catalog.catalog1.conf.hive.metastore.uris=thrift://hms1:9083
        catalog.catalog2.conf.hive.metastore.uris=thrift://hms2:9083
        """);

    ProxyConfig config = ProxyConfigLoader.load(file);

    Assert.assertEquals(9088, config.server().port());
    Assert.assertEquals("catalog1", config.defaultCatalog());
    Assert.assertEquals(2, config.catalogs().size());
    Assert.assertEquals("thrift://hms2:9083",
        config.catalogs().get("catalog2").hiveConf().get("hive.metastore.uris"));
  }
}
