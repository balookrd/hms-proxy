package io.github.mmalykhin.hmsproxy.routing;

import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Keeps a single keytab login for the outbound service principal alive instead of running a fresh
 * kinit per call. A new UGI per call also leaks: Hadoop's static {@code FileSystem.CACHE} is keyed
 * by UGI, so every login permanently adds a never-closed FileSystem with its own client connection
 * pools. Reusing one UGI keeps that cache bounded without weakening isolation: purges always ran
 * under the same service principal, never under the calling client's identity.
 */
final class KeytabUgiProvider {
  @FunctionalInterface
  interface KeytabLogin {
    UserGroupInformation login(String principal, String keytab) throws IOException;
  }

  @FunctionalInterface
  interface TicketRefresh {
    void refresh(UserGroupInformation ugi) throws IOException;
  }

  private final KeytabLogin login;
  private final TicketRefresh refresh;

  private String cachedPrincipal;
  private String cachedKeytab;
  private UserGroupInformation cachedUgi;

  KeytabUgiProvider() {
    this(
        UserGroupInformation::loginUserFromKeytabAndReturnUGI,
        UserGroupInformation::checkTGTAndReloginFromKeytab);
  }

  KeytabUgiProvider(KeytabLogin login, TicketRefresh refresh) {
    this.login = login;
    this.refresh = refresh;
  }

  synchronized UserGroupInformation get(String principal, String keytab) throws IOException {
    if (cachedUgi == null
        || !Objects.equals(cachedPrincipal, principal)
        || !Objects.equals(cachedKeytab, keytab)) {
      cachedUgi = login.login(principal, keytab);
      cachedPrincipal = principal;
      cachedKeytab = keytab;
      return cachedUgi;
    }
    try {
      refresh.refresh(cachedUgi);
    } catch (IOException e) {
      // Drop the cached login so the next purge retries a full kinit, e.g. after a keytab rotation.
      cachedUgi = null;
      cachedPrincipal = null;
      cachedKeytab = null;
      throw e;
    }
    return cachedUgi;
  }
}
