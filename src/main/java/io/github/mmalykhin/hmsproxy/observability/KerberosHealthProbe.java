package io.github.mmalykhin.hmsproxy.observability;

import io.github.mmalykhin.hmsproxy.security.KerberosPrincipalUtil;
import io.github.mmalykhin.hmsproxy.security.LoginSubjects;
import java.io.IOException;
import java.util.Comparator;
import java.util.Date;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads the state of the Kerberos logins the proxy already uses.
 *
 * <p>The probe never calls {@code UserGroupInformation.setConfiguration} and never performs a new
 * keytab login: {@code /readyz} is unauthenticated and scraped every few seconds, so a probe with
 * side effects would replace the live process-wide security configuration and hammer the KDC.
 */
public final class KerberosHealthProbe {
  private static final Logger LOG = LoggerFactory.getLogger(KerberosHealthProbe.class);
  private static final long EXPIRING_THRESHOLD_SECONDS = 300L;
  private static final AtomicBoolean UNREADABLE_TICKETS_WARNED = new AtomicBoolean();

  private KerberosHealthProbe() {
  }

  @FunctionalInterface
  interface LoginUserSupplier {
    UserGroupInformation get() throws IOException;
  }

  public enum LoginState {
    DISABLED,
    PENDING,
    ACTIVE,
    STALE,
    FAILED
  }

  public static KerberosStatus disabled(String component) {
    return new KerberosStatus(component, false, LoginState.DISABLED, null, null, null, null, null);
  }

  /** Reports the state of the process login user, which serves front-door SASL handshakes. */
  public static KerberosStatus probeLoginUser(String component, String principal) {
    return probe(component, principal, UserGroupInformation::getLoginUser, nowEpochSecond());
  }

  /** Reports the state of the most recent backend Kerberos login performed by backend sessions. */
  public static KerberosStatus probeBackendLogin(String component, String principal) {
    return probeBackendLogin(
        component, principal, BackendKerberosLoginTracker.processWide().lastLogin(), nowEpochSecond());
  }

  static KerberosStatus probe(
      String component,
      String principal,
      LoginUserSupplier loginUserSupplier,
      long nowEpochSecond
  ) {
    String expectedPrincipal = KerberosPrincipalUtil.resolveForLocalHost(principal);
    UserGroupInformation loginUser;
    try {
      loginUser = loginUserSupplier.get();
    } catch (Exception error) {
      return failed(component, expectedPrincipal, nowEpochSecond, describe(error));
    }
    if (loginUser == null) {
      return pending(component, expectedPrincipal, nowEpochSecond);
    }
    return status(
        component,
        expectedPrincipal,
        loginUser.getUserName(),
        LoginSubjects.of(loginUser),
        loginUser.hasKerberosCredentials(),
        nowEpochSecond);
  }

  static KerberosStatus probeBackendLogin(
      String component,
      String principal,
      BackendKerberosLoginTracker.BackendLogin login,
      long nowEpochSecond
  ) {
    String expectedPrincipal = KerberosPrincipalUtil.resolveForLocalHost(principal);
    if (login == null) {
      return pending(component, expectedPrincipal, nowEpochSecond);
    }
    return informational(status(
        component,
        expectedPrincipal,
        login.principal(),
        login.subject(),
        false,
        nowEpochSecond));
  }

  /**
   * Backend sessions perform their own keytab login whenever they open, so an unusable recorded
   * login is reported as {@code STALE} for diagnostics while backend readiness stays owned by the
   * connectivity probes.
   */
  private static KerberosStatus informational(KerberosStatus status) {
    if (status.state() != LoginState.FAILED) {
      return status;
    }
    return new KerberosStatus(
        status.component(),
        status.enabled(),
        LoginState.STALE,
        status.principal(),
        status.checkedAtEpochSecond(),
        status.tgtExpiresAtEpochSecond(),
        status.secondsUntilExpiry(),
        status.detail());
  }

  static KerberosStatus status(
      String component,
      String expectedPrincipal,
      String loginPrincipal,
      Subject loginSubject,
      boolean kerberosCredentialsHint,
      long nowEpochSecond
  ) {
    Long expiryEpochSecond = tgtExpiryEpochSecond(loginSubject).orElse(null);
    if (expiryEpochSecond == null && !kerberosCredentialsHint) {
      return failed(
          component,
          loginPrincipal,
          nowEpochSecond,
          "login user '" + loginPrincipal + "' has no Kerberos credentials");
    }
    if (expiryEpochSecond == null) {
      warnOnceAboutUnreadableTickets(loginPrincipal);
    }

    Long secondsUntilExpiry = expiryEpochSecond == null ? null : expiryEpochSecond - nowEpochSecond;
    // Hadoop never renews the TGT of a keytab login on its own, and the SASL acceptor authenticates
    // clients with keytab service keys, so an aged-out TGT is reported without failing readiness.
    LoginState state = secondsUntilExpiry != null && secondsUntilExpiry <= 0L
        ? LoginState.STALE
        : LoginState.ACTIVE;
    return new KerberosStatus(
        component,
        true,
        state,
        loginPrincipal,
        nowEpochSecond,
        expiryEpochSecond,
        secondsUntilExpiry,
        freshness(secondsUntilExpiry) + principalMismatch(expectedPrincipal, loginPrincipal));
  }

  private static void warnOnceAboutUnreadableTickets(String loginPrincipal) {
    if (UNREADABLE_TICKETS_WARNED.compareAndSet(false, true)) {
      LOG.warn("Kerberos health probe cannot read the tickets of login user {}, so TGT freshness is "
              + "reported as unknown. This usually means the Hadoop login subject is no longer "
              + "reachable after a library upgrade.",
          loginPrincipal);
    }
  }

  private static KerberosStatus pending(String component, String principal, long nowEpochSecond) {
    return new KerberosStatus(
        component,
        true,
        LoginState.PENDING,
        principal,
        nowEpochSecond,
        null,
        null,
        "no Kerberos login recorded yet");
  }

  private static KerberosStatus failed(
      String component,
      String principal,
      long nowEpochSecond,
      String detail
  ) {
    return new KerberosStatus(
        component, true, LoginState.FAILED, principal, nowEpochSecond, null, null, detail);
  }

  private static String freshness(Long secondsUntilExpiry) {
    if (secondsUntilExpiry == null) {
      return "unknown";
    }
    if (secondsUntilExpiry <= 0L) {
      return "expired";
    }
    return secondsUntilExpiry < EXPIRING_THRESHOLD_SECONDS ? "expiring" : "fresh";
  }

  private static String principalMismatch(String expectedPrincipal, String loginPrincipal) {
    if (expectedPrincipal == null || expectedPrincipal.equals(loginPrincipal)) {
      return "";
    }
    return "; configured principal " + expectedPrincipal + " differs from login user " + loginPrincipal;
  }

  private static String describe(Exception error) {
    return error.getClass().getSimpleName()
        + (error.getMessage() == null ? "" : ": " + error.getMessage());
  }

  private static long nowEpochSecond() {
    return System.currentTimeMillis() / 1000L;
  }

  private static Optional<Long> tgtExpiryEpochSecond(Subject subject) {
    if (subject == null) {
      return Optional.empty();
    }
    try {
<<<<<<< HEAD
      Set<KerberosTicket> tickets = subject.getPrivateCredentials(KerberosTicket.class);
      // Cross-realm TGTs (krbtgt/OTHER@LOCAL) can outlive the local TGT the login actually depends
      // on, so they are only considered when no local TGT is present.
      return latestExpiry(tickets, KerberosHealthProbe::isLocalTgt)
          .or(() -> latestExpiry(tickets, KerberosHealthProbe::isTgt));
    } catch (RuntimeException concurrentRelogin) {
      // The subject can be mutated by a relogin while the probe iterates its credentials. Log the
      // cause, otherwise the endpoint reports freshness "unknown" with no trace of why.
      LOG.warn("Kerberos TGT expiry lookup failed; readiness will report unknown TGT freshness",
          concurrentRelogin);
      return Optional.empty();
    }
  }

  private static Optional<Long> latestExpiry(
      Set<KerberosTicket> tickets,
      Predicate<KerberosTicket> filter
  ) {
    return tickets.stream()
        .filter(filter)
        .map(KerberosTicket::getEndTime)
        .filter(date -> date != null)
        .max(Comparator.comparing(Date::getTime))
        .map(date -> date.getTime() / 1000L);
  }

  private static boolean isTgt(KerberosTicket ticket) {
    KerberosPrincipal server = ticket.getServer();
    return server != null && server.getName() != null && server.getName().startsWith("krbtgt/");
  }

  private static boolean isLocalTgt(KerberosTicket ticket) {
    if (!isTgt(ticket)) {
      return false;
    }
    KerberosPrincipal server = ticket.getServer();
    String serviceRealm = server.getName().substring("krbtgt/".length());
    int realmSeparator = serviceRealm.indexOf('@');
    return realmSeparator > 0
        && serviceRealm.substring(0, realmSeparator).equals(server.getRealm());
  }

  public record KerberosStatus(
      String component,
      boolean enabled,
      LoginState state,
      String principal,
      Long checkedAtEpochSecond,
      Long tgtExpiresAtEpochSecond,
      Long secondsUntilExpiry,
      String detail
  ) {
    public boolean loggedIn() {
      return state == LoginState.ACTIVE;
    }

    public boolean healthy() {
      return switch (state) {
        case DISABLED, PENDING, STALE -> true;
        case ACTIVE -> secondsUntilExpiry == null || secondsUntilExpiry > 0L;
        case FAILED -> false;
      };
    }
  }
}
