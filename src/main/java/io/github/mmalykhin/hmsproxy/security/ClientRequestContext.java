package io.github.mmalykhin.hmsproxy.security;

import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.WeakHashMap;
import org.apache.thrift.transport.TTransport;

public final class ClientRequestContext {
  private static final ThreadLocal<String> REMOTE_ADDRESS = new ThreadLocal<>();
  private static final ThreadLocal<String> REMOTE_USER = new ThreadLocal<>();
  private static final ThreadLocal<TTransport> CURRENT_TRANSPORT = new ThreadLocal<>();
  private static final Map<TTransport, ImpersonationContext> CONNECTION_UGI =
      Collections.synchronizedMap(new WeakHashMap<>());

  private ClientRequestContext() {
  }

  public static Optional<String> remoteAddress() {
    return Optional.ofNullable(REMOTE_ADDRESS.get());
  }

  public static Optional<String> remoteUser() {
    return Optional.ofNullable(REMOTE_USER.get());
  }

  public static Optional<TTransport> currentTransport() {
    return Optional.ofNullable(CURRENT_TRANSPORT.get());
  }

  public static TTransport setCurrentTransport(TTransport transport) {
    TTransport previous = CURRENT_TRANSPORT.get();
    if (transport == null) {
      CURRENT_TRANSPORT.remove();
    } else {
      CURRENT_TRANSPORT.set(transport);
    }
    return previous;
  }

  public static void restoreCurrentTransport(TTransport transport) {
    if (transport == null) {
      CURRENT_TRANSPORT.remove();
    } else {
      CURRENT_TRANSPORT.set(transport);
    }
  }

  public static void setConnectionUgi(TTransport transport, ImpersonationContext context) {
    if (transport != null) {
      if (context != null) {
        CONNECTION_UGI.put(transport, context);
      } else {
        CONNECTION_UGI.remove(transport);
      }
    }
  }

  public static Optional<ImpersonationContext> connectionUgi(TTransport transport) {
    if (transport == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(CONNECTION_UGI.get(transport));
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
