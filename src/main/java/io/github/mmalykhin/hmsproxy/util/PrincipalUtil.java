package io.github.mmalykhin.hmsproxy.util;

public final class PrincipalUtil {
  private PrincipalUtil() {}

  public static String shortUserName(String principalOrUser) {
    if (principalOrUser == null || principalOrUser.isBlank()) {
      return null;
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
    String shortName = principalOrUser.substring(0, end).trim();
    return shortName.isEmpty() ? null : shortName;
  }

  public static String realm(String principalOrUser) {
    if (principalOrUser == null || principalOrUser.isBlank()) {
      return null;
    }
    int at = principalOrUser.indexOf('@');
    if (at < 0 || at + 1 >= principalOrUser.length()) {
      return null;
    }
    String realm = principalOrUser.substring(at + 1).trim();
    return realm.isEmpty() ? null : realm;
  }
}
