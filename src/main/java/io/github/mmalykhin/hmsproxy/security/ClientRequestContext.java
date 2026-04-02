package io.github.mmalykhin.hmsproxy.security;

import java.util.Optional;

public final class ClientRequestContext {
  private static final ThreadLocal<String> REMOTE_ADDRESS = new ThreadLocal<>();
  private static final ThreadLocal<String> REMOTE_USER = new ThreadLocal<>();

  private ClientRequestContext() {
  }

  public static Optional<String> remoteAddress() {
    return Optional.ofNullable(REMOTE_ADDRESS.get());
  }

  public static Optional<String> remoteUser() {
    return Optional.ofNullable(REMOTE_USER.get());
  }

  public static String setRemoteAddress(String remoteAddress) {
    String previous = REMOTE_ADDRESS.get();
    if (remoteAddress == null) {
      REMOTE_ADDRESS.remove();
    } else {
      REMOTE_ADDRESS.set(remoteAddress);
    }
    return previous;
  }

  public static void restoreRemoteAddress(String remoteAddress) {
    if (remoteAddress == null) {
      REMOTE_ADDRESS.remove();
    } else {
      REMOTE_ADDRESS.set(remoteAddress);
    }
  }

  public static String setRemoteUser(String remoteUser) {
    String previous = REMOTE_USER.get();
    if (remoteUser == null) {
      REMOTE_USER.remove();
    } else {
      REMOTE_USER.set(remoteUser);
    }
    return previous;
  }

  public static void restoreRemoteUser(String remoteUser) {
    if (remoteUser == null) {
      REMOTE_USER.remove();
    } else {
      REMOTE_USER.set(remoteUser);
    }
  }
}
