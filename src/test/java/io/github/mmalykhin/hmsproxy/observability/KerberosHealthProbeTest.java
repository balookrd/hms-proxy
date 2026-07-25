package io.github.mmalykhin.hmsproxy.observability;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class KerberosHealthProbeTest {
  private static final String PRINCIPAL = "hive/proxy-host.example.com@EXAMPLE.COM";
  private static final long NOW_EPOCH_SECOND = 1_700_000_000L;

  @BeforeClass
  public static void ensureKerberosNameRules() {
    // UserGroupInformation resolves short names through auth_to_local rules, which are unset in
    // tests because no Hadoop security configuration is installed.
    if (!KerberosName.hasRulesBeenSet()) {
      KerberosName.setRules("RULE:[1:$1]\nRULE:[2:$1]\nDEFAULT");
    }
  }

  @Test
  public void disabledStatusIsAlwaysHealthy() {
    KerberosHealthProbe.KerberosStatus status = KerberosHealthProbe.disabled("frontDoor");

    Assert.assertFalse(status.enabled());
    Assert.assertFalse(status.loggedIn());
    Assert.assertTrue(status.healthy());
    Assert.assertEquals("frontDoor", status.component());
  }

  @Test
  public void probeReportsFreshTgtOfExistingLoginUser() {
    UserGroupInformation loginUser = loginUserWithTickets(
        PRINCIPAL, tgt(PRINCIPAL, epochSecond(NOW_EPOCH_SECOND + 3_600L)));

    KerberosHealthProbe.KerberosStatus status =
        KerberosHealthProbe.probe("frontDoor", PRINCIPAL, () -> loginUser, NOW_EPOCH_SECOND);

    Assert.assertTrue(status.enabled());
    Assert.assertTrue(status.loggedIn());
    Assert.assertTrue(status.healthy());
    Assert.assertEquals(PRINCIPAL, status.principal());
    Assert.assertEquals(Long.valueOf(NOW_EPOCH_SECOND), status.checkedAtEpochSecond());
    Assert.assertEquals(Long.valueOf(NOW_EPOCH_SECOND + 3_600L), status.tgtExpiresAtEpochSecond());
    Assert.assertEquals(Long.valueOf(3_600L), status.secondsUntilExpiry());
    Assert.assertEquals("fresh", status.detail());
  }

  @Test
  public void probeReportsExpiringTgtAsHealthy() {
    UserGroupInformation loginUser = loginUserWithTickets(
        PRINCIPAL, tgt(PRINCIPAL, epochSecond(NOW_EPOCH_SECOND + 120L)));

    KerberosHealthProbe.KerberosStatus status =
        KerberosHealthProbe.probe("frontDoor", PRINCIPAL, () -> loginUser, NOW_EPOCH_SECOND);

    Assert.assertTrue(status.healthy());
    Assert.assertEquals("expiring", status.detail());
  }

  @Test
  public void probeReportsExpiredTgtAsStaleWithoutFailingReadiness() {
    // Hadoop does not renew the TGT of a keytab login on its own, and the front-door SASL acceptor
    // authenticates clients with the keytab service keys rather than the TGT, so an aged-out TGT
    // must be visible without pulling a working proxy out of rotation.
    UserGroupInformation loginUser = loginUserWithTickets(
        PRINCIPAL, tgt(PRINCIPAL, epochSecond(NOW_EPOCH_SECOND - 60L)));

    KerberosHealthProbe.KerberosStatus status =
        KerberosHealthProbe.probe("frontDoor", PRINCIPAL, () -> loginUser, NOW_EPOCH_SECOND);

    Assert.assertEquals(KerberosHealthProbe.LoginState.STALE, status.state());
    Assert.assertTrue(status.healthy());
    Assert.assertEquals("expired", status.detail());
    Assert.assertEquals(Long.valueOf(-60L), status.secondsUntilExpiry());
  }

  @Test
  public void probeKeepsKerberosLoginUserActiveWhenTicketsAreNotReadable() {
    // Fail-open path: the login user reports Kerberos credentials but the probe cannot read its
    // tickets, for example when a Hadoop upgrade hides the login subject.
    KerberosHealthProbe.KerberosStatus status =
        KerberosHealthProbe.status("frontDoor", PRINCIPAL, PRINCIPAL, null, true, NOW_EPOCH_SECOND);

    Assert.assertEquals(KerberosHealthProbe.LoginState.ACTIVE, status.state());
    Assert.assertTrue(status.healthy());
    Assert.assertEquals("unknown", status.detail());
    Assert.assertNull(status.tgtExpiresAtEpochSecond());
  }

  @Test
  public void probePrefersLocalRealmTgtOverCrossRealmTgt() {
    UserGroupInformation loginUser = loginUserWithTickets(
        PRINCIPAL,
        ticket(PRINCIPAL, "krbtgt/OTHER.EXAMPLE.COM@EXAMPLE.COM",
            epochSecond(NOW_EPOCH_SECOND + 86_400L)),
        ticket(PRINCIPAL, "krbtgt/EXAMPLE.COM@EXAMPLE.COM",
            epochSecond(NOW_EPOCH_SECOND + 900L)));

    KerberosHealthProbe.KerberosStatus status =
        KerberosHealthProbe.probe("frontDoor", PRINCIPAL, () -> loginUser, NOW_EPOCH_SECOND);

    Assert.assertEquals(Long.valueOf(NOW_EPOCH_SECOND + 900L), status.tgtExpiresAtEpochSecond());
  }

  @Test
  public void probeIgnoresServiceTicketsWhenReadingTgtExpiry() {
    UserGroupInformation loginUser = loginUserWithTickets(
        PRINCIPAL,
        tgt(PRINCIPAL, epochSecond(NOW_EPOCH_SECOND + 600L)),
        ticket(PRINCIPAL, "hive/backend-host.example.com@EXAMPLE.COM",
            epochSecond(NOW_EPOCH_SECOND + 86_400L)));

    KerberosHealthProbe.KerberosStatus status =
        KerberosHealthProbe.probe("backend", PRINCIPAL, () -> loginUser, NOW_EPOCH_SECOND);

    Assert.assertEquals(Long.valueOf(NOW_EPOCH_SECOND + 600L), status.tgtExpiresAtEpochSecond());
  }

  @Test
  public void probeReadsLoginUserExactlyOnce() {
    UserGroupInformation loginUser = loginUserWithTickets(
        PRINCIPAL, tgt(PRINCIPAL, epochSecond(NOW_EPOCH_SECOND + 3_600L)));
    AtomicInteger reads = new AtomicInteger();

    KerberosHealthProbe.probe("frontDoor", PRINCIPAL, () -> {
      reads.incrementAndGet();
      return loginUser;
    }, NOW_EPOCH_SECOND);

    Assert.assertEquals(1, reads.get());
  }

  @Test
  public void probeReportsLoginUserWithoutKerberosCredentialsAsUnhealthy() {
    UserGroupInformation loginUser = UserGroupInformation.createRemoteUser(PRINCIPAL);

    KerberosHealthProbe.KerberosStatus status =
        KerberosHealthProbe.probe("frontDoor", PRINCIPAL, () -> loginUser, NOW_EPOCH_SECOND);

    Assert.assertFalse(status.loggedIn());
    Assert.assertFalse(status.healthy());
    Assert.assertNotNull(status.detail());
    Assert.assertTrue(status.detail(), status.detail().contains("no Kerberos credentials"));
  }

  @Test
  public void probeReportsMissingLoginAsPendingAndHealthy() {
    KerberosHealthProbe.KerberosStatus status =
        KerberosHealthProbe.probe("backend", PRINCIPAL, () -> null, NOW_EPOCH_SECOND);

    Assert.assertTrue(status.enabled());
    Assert.assertFalse(status.loggedIn());
    Assert.assertTrue(status.healthy());
    Assert.assertEquals(PRINCIPAL, status.principal());
  }

  @Test
  public void probeReportsLoginLookupFailureAsUnhealthy() {
    KerberosHealthProbe.KerberosStatus status = KerberosHealthProbe.probe(
        "frontDoor",
        PRINCIPAL,
        () -> {
          throw new IOException("login user unavailable");
        },
        NOW_EPOCH_SECOND);

    Assert.assertFalse(status.loggedIn());
    Assert.assertFalse(status.healthy());
    Assert.assertEquals("IOException: login user unavailable", status.detail());
  }

  @Test
  public void backendProbeReadsTgtOfRecordedBackendLogin() {
    Subject subject = new Subject();
    subject.getPrivateCredentials().add(tgt(PRINCIPAL, epochSecond(NOW_EPOCH_SECOND + 1_800L)));
    BackendKerberosLoginTracker.BackendLogin login =
        new BackendKerberosLoginTracker.BackendLogin(PRINCIPAL, subject);

    KerberosHealthProbe.KerberosStatus status =
        KerberosHealthProbe.probeBackendLogin("backend", PRINCIPAL, login, NOW_EPOCH_SECOND);

    Assert.assertTrue(status.loggedIn());
    Assert.assertTrue(status.healthy());
    Assert.assertEquals(PRINCIPAL, status.principal());
    Assert.assertEquals(Long.valueOf(1_800L), status.secondsUntilExpiry());
    Assert.assertEquals("fresh", status.detail());
  }

  @Test
  public void backendProbeReportsExpiredBackendTgtAsStaleWithoutFailingReadiness() {
    // Backend sessions perform their own keytab login whenever they open, so a stale recorded
    // login is reported but backend readiness stays owned by the connectivity probes.
    Subject subject = new Subject();
    subject.getPrivateCredentials().add(tgt(PRINCIPAL, epochSecond(NOW_EPOCH_SECOND - 5L)));
    BackendKerberosLoginTracker.BackendLogin login =
        new BackendKerberosLoginTracker.BackendLogin(PRINCIPAL, subject);

    KerberosHealthProbe.KerberosStatus status =
        KerberosHealthProbe.probeBackendLogin("backend", PRINCIPAL, login, NOW_EPOCH_SECOND);

    Assert.assertEquals(KerberosHealthProbe.LoginState.STALE, status.state());
    Assert.assertFalse(status.loggedIn());
    Assert.assertTrue(status.healthy());
    Assert.assertEquals("expired", status.detail());
    Assert.assertEquals(Long.valueOf(-5L), status.secondsUntilExpiry());
  }

  @Test
  public void backendProbeWithoutRecordedLoginIsPendingAndHealthy() {
    KerberosHealthProbe.KerberosStatus status =
        KerberosHealthProbe.probeBackendLogin("backend", PRINCIPAL, null, NOW_EPOCH_SECOND);

    Assert.assertTrue(status.enabled());
    Assert.assertFalse(status.loggedIn());
    Assert.assertTrue(status.healthy());
    Assert.assertEquals(PRINCIPAL, status.principal());
  }

  @Test
  public void backendProbeWithoutKerberosCredentialsIsStaleWithoutFailingReadiness() {
    BackendKerberosLoginTracker.BackendLogin login =
        new BackendKerberosLoginTracker.BackendLogin(PRINCIPAL, new Subject());

    KerberosHealthProbe.KerberosStatus status =
        KerberosHealthProbe.probeBackendLogin("backend", PRINCIPAL, login, NOW_EPOCH_SECOND);

    Assert.assertEquals(KerberosHealthProbe.LoginState.STALE, status.state());
    Assert.assertFalse(status.loggedIn());
    Assert.assertTrue(status.healthy());
    Assert.assertTrue(status.detail(), status.detail().contains("no Kerberos credentials"));
  }

  @Test
  public void probeReportsPrincipalMismatchWithoutFailingReadiness() {
    String otherPrincipal = "hive/other-host.example.com@EXAMPLE.COM";
    UserGroupInformation loginUser = loginUserWithTickets(
        otherPrincipal, tgt(otherPrincipal, epochSecond(NOW_EPOCH_SECOND + 3_600L)));

    KerberosHealthProbe.KerberosStatus status =
        KerberosHealthProbe.probe("frontDoor", PRINCIPAL, () -> loginUser, NOW_EPOCH_SECOND);

    Assert.assertTrue(status.healthy());
    Assert.assertEquals(otherPrincipal, status.principal());
    Assert.assertTrue(status.detail(), status.detail().contains(PRINCIPAL));
  }

  private static UserGroupInformation loginUserWithTickets(String principal, KerberosTicket... tickets) {
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(principal);
    try {
      Method getSubject = UserGroupInformation.class.getDeclaredMethod("getSubject");
      getSubject.setAccessible(true);
      Subject subject = (Subject) getSubject.invoke(ugi);
      subject.getPrivateCredentials().addAll(List.of(tickets));
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Unable to attach Kerberos tickets to the test login user", e);
    }
    return ugi;
  }

  private static KerberosTicket tgt(String client, Date endTime) {
    return ticket(client, "krbtgt/EXAMPLE.COM@EXAMPLE.COM", endTime);
  }

  private static KerberosTicket ticket(String client, String server, Date endTime) {
    Date issuedAt = epochSecond(NOW_EPOCH_SECOND - 60L);
    return new KerberosTicket(
        new byte[] {1},
        new KerberosPrincipal(client),
        new KerberosPrincipal(server),
        new byte[] {1, 2, 3, 4, 5, 6, 7, 8},
        1,
        null,
        issuedAt,
        issuedAt,
        endTime,
        null,
        null);
  }

  private static Date epochSecond(long epochSecond) {
    return new Date(epochSecond * 1000L);
  }
}
