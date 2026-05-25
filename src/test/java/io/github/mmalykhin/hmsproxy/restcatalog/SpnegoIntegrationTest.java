package io.github.mmalykhin.hmsproxy.restcatalog;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.restcatalog.RestCatalogConfig;
import io.github.mmalykhin.hmsproxy.config.routing.BackendConfig;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;
import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.security.PrivilegedExceptionAction;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class SpnegoIntegrationTest {
  private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(10);
  private static final String SPNEGO_OID = "1.3.6.1.5.5.2";

  private static MiniKdc kdc;
  private static File workDir;
  private static File serverKeytab;
  private static File clientKeytab;
  private static String realm;
  private static String serverPrincipal;
  private static String clientPrincipal;

  @BeforeClass
  public static void startKdc() throws Exception {
    workDir = Files.createTempDirectory("hms-proxy-spnego-test").toFile();
    Properties kdcProps = MiniKdc.createConf();
    kdc = new MiniKdc(kdcProps, workDir);
    kdc.start();
    realm = kdc.getRealm();
    serverPrincipal = "HTTP/localhost@" + realm;
    clientPrincipal = "alice@" + realm;

    serverKeytab = new File(workDir, "server.keytab");
    kdc.createPrincipal(serverKeytab, "HTTP/localhost");
    clientKeytab = new File(workDir, "alice.keytab");
    kdc.createPrincipal(clientKeytab, "alice");

    // Krb5 Config caches the previously-loaded krb5.conf; force a reload so
    // UGI.setConfiguration picks up MiniKdc's freshly-written file when other
    // tests in the suite already touched the JGSS singleton.
    Class<?> configClass = Class.forName("sun.security.krb5.Config");
    configClass.getDeclaredMethod("refresh").invoke(null);

    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication", "kerberos");
    // KerberosName.rules is JVM-global; other tests in the suite may have
    // installed custom rules. Explicit per-component rules (instead of bare
    // DEFAULT) reliably match HTTP/<host>@REALM and alice@REALM regardless of
    // what previously ran in this JVM.
    String rules = String.join("\n",
        "RULE:[1:$1@$0](.*@" + realm + ")s/@.*//",
        "RULE:[2:$1@$0](.*@" + realm + ")s/@.*//",
        "DEFAULT");
    conf.set("hadoop.security.auth_to_local", rules);
    UserGroupInformation.setConfiguration(conf);
    KerberosName.setRules(rules);
  }

  @AfterClass
  public static void stopKdc() {
    if (kdc != null) {
      kdc.stop();
    }
    Configuration simple = new Configuration();
    simple.set("hadoop.security.authentication", "simple");
    UserGroupInformation.setConfiguration(simple);
  }

  @Test
  public void unauthenticatedRequestGetsNegotiateChallenge() throws Exception {
    try (RestCatalogServer server = RestCatalogServer.open(buildProxyConfig(), null)) {
      HttpResponse<String> response = HttpClient.newBuilder().connectTimeout(HTTP_TIMEOUT).build()
          .send(HttpRequest.newBuilder()
              .uri(URI.create("http://127.0.0.1:" + server.boundPort() + "/v1/config"))
              .timeout(HTTP_TIMEOUT)
              .GET()
              .build(),
              HttpResponse.BodyHandlers.ofString());
      Assert.assertEquals(401, response.statusCode());
      Assert.assertEquals("Negotiate",
          response.headers().firstValue("WWW-Authenticate").orElse(""));
    }
  }

  @Test
  public void invalidNegotiateTokenIsRejected() throws Exception {
    try (RestCatalogServer server = RestCatalogServer.open(buildProxyConfig(), null)) {
      HttpResponse<String> response = HttpClient.newBuilder().connectTimeout(HTTP_TIMEOUT).build()
          .send(HttpRequest.newBuilder()
              .uri(URI.create("http://127.0.0.1:" + server.boundPort() + "/v1/config"))
              .timeout(HTTP_TIMEOUT)
              .header("Authorization", "Negotiate not-a-real-base64!@#")
              .GET()
              .build(),
              HttpResponse.BodyHandlers.ofString());
      Assert.assertTrue("expected 4xx, got " + response.statusCode(),
          response.statusCode() == 400 || response.statusCode() == 401);
    }
  }

  @Test
  public void authenticatedRequestSucceeds() throws Exception {
    try (RestCatalogServer server = RestCatalogServer.open(buildProxyConfig(), null)) {
      UserGroupInformation clientUgi = UserGroupInformation
          .loginUserFromKeytabAndReturnUGI(clientPrincipal, clientKeytab.getAbsolutePath());
      String token = clientUgi.doAs((PrivilegedExceptionAction<String>) () -> {
        GSSManager manager = GSSManager.getInstance();
        Oid spnegoOid = new Oid(SPNEGO_OID);
        GSSName serverName = manager.createName(serverPrincipal, GSSName.NT_USER_NAME);
        GSSContext context = manager.createContext(serverName, spnegoOid, null, GSSContext.DEFAULT_LIFETIME);
        try {
          byte[] outToken = context.initSecContext(new byte[0], 0, 0);
          return Base64.getEncoder().encodeToString(outToken);
        } finally {
          context.dispose();
        }
      });

      HttpResponse<String> response = HttpClient.newBuilder().connectTimeout(HTTP_TIMEOUT).build()
          .send(HttpRequest.newBuilder()
              .uri(URI.create("http://127.0.0.1:" + server.boundPort() + "/v1/config"))
              .timeout(HTTP_TIMEOUT)
              .header("Authorization", "Negotiate " + token)
              .GET()
              .build(),
              HttpResponse.BodyHandlers.ofString());
      Assert.assertEquals("body: " + response.body(), 200, response.statusCode());
      Assert.assertTrue("expected ConfigResponse skeleton, got " + response.body(),
          response.body().contains("\"defaults\"") && response.body().contains("\"overrides\""));
    }
  }

  private static ProxyConfig buildProxyConfig() {
    RestCatalogConfig rest = new RestCatalogConfig(
        true, "127.0.0.1", 0, 1, 4, serverPrincipal, serverKeytab.getAbsolutePath());
    return ProxyConfig.builder()
        .server(new ServerConfig("hms-proxy-spnego-test", "127.0.0.1", 9083, 1, 4))
        .catalogDbSeparator(".")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", new CatalogConfig(
            "catalog1",
            null,
            null,
            false,
            CatalogAccessMode.READ_WRITE,
            List.of(),
            null,
            null,
            Map.of("hive.metastore.uris", "thrift://hms-test:9083"))))
        .backend(new BackendConfig(Map.of()))
        .restCatalog(rest)
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();
  }
}
