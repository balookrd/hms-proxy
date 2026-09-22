package io.github.mmalykhin.hmsproxy.security;

import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.thrift.transport.TTransport;

public final class ClientRequestContext {
  private static final ThreadLocal<String> REMOTE_ADDRESS = new ThreadLocal<>();
  private static final ThreadLocal<String> REMOTE_USER = new ThreadLocal<>();
  private static final ThreadLocal<TTransport> CURRENT_TRANSPORT = new ThreadLocal<>();
  private static final Map<TTransport, ImpersonationContext> CONNECTION_UGI =
      Collections.synchronizedMap(new WeakHashMap<>());
  private static final Map<TTransport, Map<String, String>> CONNECTION_META_CONF =
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

  public static void setConnectionMetaConf(TTransport transport, String key, String value) {
    if (transport != null && key != null) {
      if (value != null) {
        CONNECTION_META_CONF.computeIfAbsent(transport, t -> new ConcurrentHashMap<>()).put(key, value);
      } else {
        Map<String, String> conf = CONNECTION_META_CONF.get(transport);
        if (conf != null) {
          conf.remove(key);
        }
      }
    }
  }

  public static Optional<String> connectionMetaConf(TTransport transport, String key) {
    if (transport == null || key == null) {
      return Optional.empty();
    }
    Map<String, String> conf = CONNECTION_META_CONF.get(transport);
    return conf != null ? Optional.ofNullable(conf.get(key)) : Optional.empty();
  }

  private static final ThreadLocal<Map<String, String>> THREAD_LOCAL_META_CONF =
      ThreadLocal.withInitial(ConcurrentHashMap::new);

  public static void setSessionMetaConf(String key, String value) {
    Optional<TTransport> transport = currentTransport();
    if (transport.isPresent()) {
      setConnectionMetaConf(transport.get(), key, value);
    } else if (key != null) {
      if (value != null) {
        THREAD_LOCAL_META_CONF.get().put(key, value);
      } else {
        THREAD_LOCAL_META_CONF.get().remove(key);
      }
    }
  }

  public static Optional<String> sessionMetaConf(String key) {
    if (key == null) {
      return Optional.empty();
    }
    Optional<TTransport> transport = currentTransport();
    if (transport.isPresent()) {
      return connectionMetaConf(transport.get(), key);
    }
    return Optional.ofNullable(THREAD_LOCAL_META_CONF.get().get(key));
  }

  public static void clearThreadLocalMetaConf() {
    THREAD_LOCAL_META_CONF.remove();
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
