package io.github.mmalykhin.hmsproxy.util;

public final class PrincipalUtil {
  private PrincipalUtil() {}

  public static String shortUserName(String principalOrUser) {
    if (principalOrUser == null || principalOrUser.isBlank()) {
      return principalOrUser;
    }
    int slash = principalOrUser.indexOf('/');
    int at = principalOrUser.indexOf('@');
    int end = principalOrUser.length();
    if (slash >= 0) {
      end = Math.min(end, slash);
    }
    if (at >= 0) {
      end = Math.min(end, at);
    }
    return principalOrUser.substring(0, end);
  }
}
