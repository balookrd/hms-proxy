package io.github.mmalykhin.hmsproxy.observability;

import io.github.mmalykhin.hmsproxy.security.KerberosPrincipalUtil;
import java.security.AccessController;
import java.util.Comparator;
import java.util.Date;
import java.util.Optional;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

public final class KerberosHealthProbe {
  private KerberosHealthProbe() {
  }

  public static KerberosStatus disabled(String component) {
    return new KerberosStatus(component, false, false, null, null, null, null, null);
  }

  public static KerberosStatus probe(String component, String principal, String keytab) {
    try {
      Configuration securityConf = new Configuration(false);
      securityConf.set("hadoop.security.authentication", "kerberos");
      UserGroupInformation.setConfiguration(securityConf);
      String resolvedPrincipal = KerberosPrincipalUtil.resolveForLocalHost(principal);
      UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(resolvedPrincipal, keytab);
      Long expiryEpochSecond = tgtExpiryEpochSecond(ugi).orElse(null);
      Long nowEpochSecond = System.currentTimeMillis() / 1000L;
      Long secondsUntilExpiry = expiryEpochSecond == null ? null : expiryEpochSecond - nowEpochSecond;
      String freshness = expiryEpochSecond == null
          ? "unknown"
          : secondsUntilExpiry <= 0 ? "expired" : secondsUntilExpiry < 300 ? "expiring" : "fresh";
      return new KerberosStatus(
          component,
          true,
          true,
          resolvedPrincipal,
          nowEpochSecond,
          expiryEpochSecond,
          secondsUntilExpiry,
          freshness);
    } catch (Exception error) {
      return new KerberosStatus(
          component,
          true,
          false,
          principal,
          System.currentTimeMillis() / 1000L,
          null,
          null,
          error.getClass().getSimpleName() + (error.getMessage() == null ? "" : ": " + error.getMessage()));
    }
  }

  private static Optional<Long> tgtExpiryEpochSecond(UserGroupInformation ugi) {
    try {
      return ugi.doAs((java.security.PrivilegedExceptionAction<Optional<Long>>) () -> {
        Subject subject = Subject.getSubject(AccessController.getContext());
        if (subject == null) {
          return Optional.empty();
        }
        return subject.getPrivateCredentials(KerberosTicket.class).stream()
            .filter(ticket -> isTgt(ticket.getServer()))
            .map(KerberosTicket::getEndTime)
            .filter(date -> date != null)
            .max(Comparator.comparing(Date::getTime))
            .map(date -> date.getTime() / 1000L);
      });
    } catch (Exception error) {
      return Optional.empty();
    }
  }

  private static boolean isTgt(javax.security.auth.kerberos.KerberosPrincipal principal) {
    if (principal == null) {
      return false;
    }
    String name = principal.getName();
    return name != null && name.startsWith("krbtgt/");
  }

  public record KerberosStatus(
      String component,
      boolean enabled,
      boolean loggedIn,
      String principal,
      Long checkedAtEpochSecond,
      Long tgtExpiresAtEpochSecond,
      Long secondsUntilExpiry,
      String detail
  ) {
    public boolean healthy() {
      return !enabled || loggedIn;
    }
  }
}
