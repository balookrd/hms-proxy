package io.github.mmalykhin.hmsproxy.security;

import java.util.Optional;

public final class ClientRequestContext {
  private static final ThreadLocal<String> REMOTE_ADDRESS = new ThreadLocal<>();

  private ClientRequestContext() {
  }

  public static Optional<String> remoteAddress() {
    return Optional.ofNullable(REMOTE_ADDRESS.get());
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
}
