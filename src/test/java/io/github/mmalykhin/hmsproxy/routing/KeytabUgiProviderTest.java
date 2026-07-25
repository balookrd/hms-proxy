package io.github.mmalykhin.hmsproxy.routing;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Test;

public class KeytabUgiProviderTest {
  private static final String PRINCIPAL = "hms/host@EXAMPLE.COM";
  private static final String KEYTAB = "/etc/security/keytabs/hms.keytab";

  @Test
  public void reusesFirstKeytabLoginAndRefreshesTicketBeforeEveryReuse() throws Exception {
    AtomicInteger logins = new AtomicInteger();
    AtomicInteger refreshes = new AtomicInteger();
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("hms-proxy");
    KeytabUgiProvider provider = new KeytabUgiProvider(
        (principal, keytab) -> {
          logins.incrementAndGet();
          return ugi;
        },
        loggedIn -> refreshes.incrementAndGet());

    Assert.assertSame(ugi, provider.get(PRINCIPAL, KEYTAB));
    Assert.assertSame(ugi, provider.get(PRINCIPAL, KEYTAB));
    Assert.assertSame(ugi, provider.get(PRINCIPAL, KEYTAB));

    Assert.assertEquals(1, logins.get());
    Assert.assertEquals(2, refreshes.get());
  }

  @Test
  public void logsInAgainWhenPrincipalOrKeytabChanges() throws Exception {
    AtomicInteger logins = new AtomicInteger();
    KeytabUgiProvider provider = new KeytabUgiProvider(
        (principal, keytab) -> {
          return UserGroupInformation.createRemoteUser("hms-proxy-" + logins.incrementAndGet());
        },
        loggedIn -> { });

    provider.get(PRINCIPAL, KEYTAB);
    provider.get(PRINCIPAL, KEYTAB);
    provider.get("other/host@EXAMPLE.COM", KEYTAB);
    provider.get("other/host@EXAMPLE.COM", "/etc/security/keytabs/other.keytab");

    Assert.assertEquals(3, logins.get());
  }

  @Test
  public void dropsCachedLoginWhenTicketRefreshFails() throws Exception {
    AtomicInteger logins = new AtomicInteger();
    AtomicInteger refreshes = new AtomicInteger();
    KeytabUgiProvider provider = new KeytabUgiProvider(
        (principal, keytab) -> {
          return UserGroupInformation.createRemoteUser("hms-proxy-" + logins.incrementAndGet());
        },
        loggedIn -> {
          if (refreshes.incrementAndGet() == 1) {
            throw new IOException("simulated expired keytab");
          }
        });

    provider.get(PRINCIPAL, KEYTAB);
    try {
      provider.get(PRINCIPAL, KEYTAB);
      Assert.fail("expected the refresh failure to propagate");
    } catch (IOException expected) {
      Assert.assertEquals("simulated expired keytab", expected.getMessage());
    }
    provider.get(PRINCIPAL, KEYTAB);

    Assert.assertEquals("a failed refresh must force a fresh keytab login", 2, logins.get());
    Assert.assertEquals(1, refreshes.get());
  }
}
