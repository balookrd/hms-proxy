package io.github.mmalykhin.hmsproxy.security;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Process-wide cache of keytab-backed outbound {@link UserGroupInformation} instances keyed by
 * {@code (principal, keytab)}. Reuses logged-in UGIs and refreshes tickets via
 * {@link UserGroupInformation#checkTGTAndReloginFromKeytab()} instead of running a fresh KDC
 * login on every new backend connection or REST filesystem operation.
 */
public final class OutboundKerberosUgiCache {
  @FunctionalInterface
  public interface KeytabLogin {
    UserGroupInformation login(String principal, String keytab) throws IOException;
  }

  @FunctionalInterface
  public interface TicketRefresh {
    void refresh(UserGroupInformation ugi) throws IOException;
  }

  private static final OutboundKerberosUgiCache DEFAULT = new OutboundKerberosUgiCache(
      UserGroupInformation::loginUserFromKeytabAndReturnUGI,
      UserGroupInformation::checkTGTAndReloginFromKeytab);

  private final KeytabLogin login;
  private final TicketRefresh refresh;
  private final ConcurrentMap<Key, UserGroupInformation> cache = new ConcurrentHashMap<>();

  OutboundKerberosUgiCache(KeytabLogin login, TicketRefresh refresh) {
    this.login = Objects.requireNonNull(login, "login");
    this.refresh = Objects.requireNonNull(refresh, "refresh");
  }

  public static UserGroupInformation getOrLogin(String principal, String keytab) throws IOException {
    return DEFAULT.resolve(principal, keytab);
  }

  UserGroupInformation resolve(String principal, String keytab) throws IOException {
    Objects.requireNonNull(principal, "principal");
    Objects.requireNonNull(keytab, "keytab");
    Key key = new Key(principal, keytab);
    UserGroupInformation existing = cache.get(key);
    if (existing != null) {
      synchronized (existing) {
        if (cache.get(key) == existing) {
          try {
            refresh.refresh(existing);
            return existing;
          } catch (IOException refreshFailure) {
            cache.remove(key, existing);
          }
        }
      }
    }
    try {
      return cache.computeIfAbsent(key, k -> {
        try {
          return login.login(k.principal(), k.keytab());
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      });
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
  }

  static void clearDefaultForTest() {
    DEFAULT.cache.clear();
  }

  private record Key(String principal, String keytab) {
  }
}
