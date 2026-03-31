package io.github.mmalykhin.hmsproxy.security;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.hadoop.security.SecurityUtil;

public final class KerberosPrincipalUtil {
  private KerberosPrincipalUtil() {
  }

  public static String resolveForLocalHost(String principal) {
    if (principal == null || !principal.contains("_HOST")) {
      return principal;
    }
    try {
      return resolveForHost(principal, localCanonicalHostname());
    } catch (IOException e) {
      throw new IllegalStateException("Unable to resolve local hostname for Kerberos principal " + principal, e);
    }
  }

  public static String resolveForHost(String principal, String hostname) throws IOException {
    if (principal == null || !principal.contains("_HOST")) {
      return principal;
    }
    return SecurityUtil.getServerPrincipal(principal, hostname);
  }

  public static String localCanonicalHostname() {
    try {
      return InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
      throw new IllegalStateException("Unable to determine local canonical hostname", e);
    }
  }
}
