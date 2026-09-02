package io.github.mmalykhin.hmsproxy.config.security;

import io.github.mmalykhin.hmsproxy.config.PropertyReader;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.ProxyConfigLoader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

public class RangerConfigParserTest {

  @Test
  public void testDisabledByDefault() {
    Properties props = new Properties();
    PropertyReader reader = new PropertyReader(props);
    RangerConfig config = RangerConfigParser.parse(reader, Set.of("cat1", "cat2"));

    Assert.assertFalse(config.enabled());
    CatalogRangerConfig cat1 = config.forCatalog("cat1");
    Assert.assertFalse(cat1.enabled());
    Assert.assertEquals("cat1", cat1.serviceName());
    Assert.assertEquals("hive", cat1.serviceType());
  }

  @Test
  public void testGlobalRangerConfig() {
    Properties props = new Properties();
    props.setProperty("ranger.enabled", "true");
    props.setProperty("ranger.service-type", "hive");
    props.setProperty("ranger.service-name", "global_hive_svc");
    props.setProperty("ranger.app-id", "hms-proxy");
    props.setProperty("ranger.policy.rest.url", "http://ranger-admin:6080");
    props.setProperty("ranger.policy.cache.dir", "/var/cache/ranger");
    props.setProperty("ranger.policy.poll-interval-ms", "45000");
    props.setProperty("ranger.policy.connection-timeout-ms", "8000");
    props.setProperty("ranger.policy.read-timeout-ms", "12000");
    props.setProperty("ranger.ssl.truststore.file", "/etc/truststore.jks");
    props.setProperty("ranger.ssl.truststore.password", "secret");
    props.setProperty("ranger.config-dir", "/etc/ranger/conf");

    PropertyReader reader = new PropertyReader(props);
    RangerConfig config = RangerConfigParser.parse(reader, Set.of("cat1", "cat2"));

    Assert.assertTrue(config.enabled());
    Assert.assertEquals("http://ranger-admin:6080", config.policyRestUrl());
    Assert.assertEquals(45000L, config.policyPollIntervalMs());
    Assert.assertEquals(8000, config.connectionTimeoutMs());
    Assert.assertEquals(12000, config.readTimeoutMs());

    // Both catalogs inherit from global defaults
    CatalogRangerConfig cat1 = config.forCatalog("cat1");
    Assert.assertTrue(cat1.enabled());
    Assert.assertEquals("global_hive_svc", cat1.serviceName());
    Assert.assertEquals("http://ranger-admin:6080", cat1.policyRestUrl());

    CatalogRangerConfig cat2 = config.forCatalog("cat2");
    Assert.assertTrue(cat2.enabled());
  }

  @Test
  public void testPerCatalogRangerOverrides() {
    Properties props = new Properties();
    props.setProperty("ranger.enabled", "true");
    props.setProperty("ranger.policy.rest.url", "http://ranger-global:6080");
    props.setProperty("ranger.service-name", "global_svc");

    // Override for cat2
    props.setProperty("catalog.cat2.ranger.enabled", "true");
    props.setProperty("catalog.cat2.ranger.service-name", "cat2_custom_svc");
    props.setProperty("catalog.cat2.ranger.policy.rest.url", "http://ranger-cat2:6080");
    props.setProperty("catalog.cat2.ranger.policy.poll-interval-ms", "10000");

    // Explicitly disable for cat3
    props.setProperty("catalog.cat3.ranger.enabled", "false");

    PropertyReader reader = new PropertyReader(props);
    RangerConfig config = RangerConfigParser.parse(reader, Set.of("cat1", "cat2", "cat3"));

    Assert.assertTrue(config.enabled());

    // cat1 inherits global
    CatalogRangerConfig cat1 = config.forCatalog("cat1");
    Assert.assertTrue(cat1.enabled());
    Assert.assertEquals("global_svc", cat1.serviceName());
    Assert.assertEquals("http://ranger-global:6080", cat1.policyRestUrl());

    // cat2 has overrides
    CatalogRangerConfig cat2 = config.forCatalog("cat2");
    Assert.assertTrue(cat2.enabled());
    Assert.assertEquals("cat2_custom_svc", cat2.serviceName());
    Assert.assertEquals("http://ranger-cat2:6080", cat2.policyRestUrl());
    Assert.assertEquals(10000L, cat2.policyPollIntervalMs());

    // cat3 is disabled
    CatalogRangerConfig cat3 = config.forCatalog("cat3");
    Assert.assertFalse(cat3.enabled());
  }

  @Test
  public void testProxyConfigLoaderFullRangerAndSharedCache() throws Exception {
    Path file = Files.createTempFile("hms-proxy-ranger", ".properties");
    try {
      Files.writeString(file, """
          synthetic-read-lock.store.mode=IN_MEMORY
          server.port=9088
          catalogs=c1,c2
          routing.default-catalog=c1
          catalog.c1.conf.hive.metastore.uris=thrift://hms1:9083
          catalog.c2.conf.hive.metastore.uris=thrift://hms2:9083

          ranger.enabled=true
          ranger.policy.rest.url=http://ranger:6080
          ranger.service-name=hms_service
          ranger.policy.poll-interval-ms=30000

          catalog.c2.ranger.service-name=c2_service
          catalog.c2.ranger.policy.rest.url=http://ranger-c2:6080

          routing.database-list-cache.shared-across-users=true
          routing.database-metadata-cache.shared-across-users=true
          """);

      ProxyConfig config = ProxyConfigLoader.load(file);

      Assert.assertTrue(config.ranger().enabled());
      Assert.assertEquals("http://ranger:6080", config.ranger().policyRestUrl());
      Assert.assertEquals("hms_service", config.ranger().serviceName());

      Assert.assertTrue(config.catalogs().get("c1").ranger().enabled());
      Assert.assertEquals("hms_service", config.catalogs().get("c1").ranger().serviceName());
      Assert.assertEquals("http://ranger:6080", config.catalogs().get("c1").ranger().policyRestUrl());

      Assert.assertTrue(config.catalogs().get("c2").ranger().enabled());
      Assert.assertEquals("c2_service", config.catalogs().get("c2").ranger().serviceName());
      Assert.assertEquals("http://ranger-c2:6080", config.catalogs().get("c2").ranger().policyRestUrl());

      Assert.assertTrue(config.latencyRouting().databaseListCache().sharedAcrossUsers());
      Assert.assertTrue(config.latencyRouting().databaseMetadataCache().sharedAcrossUsers());
    } finally {
      Files.deleteIfExists(file);
    }
  }
}
