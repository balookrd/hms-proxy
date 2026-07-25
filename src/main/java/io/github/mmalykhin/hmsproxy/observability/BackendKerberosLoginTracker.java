package io.github.mmalykhin.hmsproxy.observability;

import javax.security.auth.Subject;

/**
 * Keeps the most recent backend Kerberos login so health probes can report backend credential
 * freshness without performing a keytab login of their own.
 *
 * <p>The login subject is stored instead of the login user because isolated metastore runtimes load
 * their own {@code UserGroupInformation} class, while {@link Subject} stays a JDK type.
 */
public final class BackendKerberosLoginTracker {
  private static final BackendKerberosLoginTracker PROCESS_WIDE = new BackendKerberosLoginTracker();

  public record BackendLogin(String principal, Subject subject) {
  }

  private volatile BackendLogin lastLogin;

  BackendKerberosLoginTracker() {
  }

  public static BackendKerberosLoginTracker processWide() {
    return PROCESS_WIDE;
  }

  public void record(String principal, Subject subject) {
    this.lastLogin = new BackendLogin(principal, subject);
  }

  public BackendLogin lastLogin() {
    return lastLogin;
  }
}
