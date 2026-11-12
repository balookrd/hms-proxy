package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftFailureClassifier;
import java.lang.reflect.Method;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;

final class BackendErrorNormalizer {
  private BackendErrorNormalizer() {}

  static Throwable normalize(Method method, String backendName, Throwable cause) {
    if (ThriftFailureClassifier.isConnectFailure(cause)) {
      return wrapInMetaException(backendName, method.getName(), cause);
    }
    if (!(cause instanceof TException) || isDeclaredMethodException(method, cause)) {
      return cause;
    }
    return wrapInMetaException(backendName, method.getName(), cause);
  }

  /**
   * Normalizes infrastructure {@link TException} subclasses ({@code TTransportException},
   * {@code TApplicationException}, etc.) when no {@code Method} object is available.
   *
   * <p>Business exceptions from the metastore API package ({@code MetaException},
   * {@code NoSuchObjectException}, etc.) are always declared by IDL methods and serialize
   * correctly in result structs, so they are returned as-is.  Infrastructure exceptions are
   * never declared: {@code ProcessFunction} cannot serialize them, logs an ERROR, and sends
   * the client a generic {@code TApplicationException("Internal error processing ...")} that
   * loses the actual error text.  Wrapping them in {@code MetaException} preserves the message
   * for the client.
   *
   * <p>A failed connect is the one {@code MetaException} that is wrapped too: the metastore client
   * reports it as {@code "Could not connect to meta store using any of the URIs provided..."},
   * which names a host but not the catalog, so a client of a federated proxy could not tell which
   * backend is down. See {@link ThriftFailureClassifier#isConnectFailure}; the original stays the
   * cause and its text stays in the message, so outage detection still recognizes it.
   */
  static Throwable normalizeInfrastructure(String backendName, String methodName, Throwable cause) {
    if (ThriftFailureClassifier.isConnectFailure(cause)) {
      return wrapInMetaException(backendName, methodName, cause);
    }
    if (!(cause instanceof TException)
        || cause.getClass().getName().startsWith("org.apache.hadoop.hive.metastore.api.")) {
      return cause;
    }
    return wrapInMetaException(backendName, methodName, cause);
  }

  private static MetaException wrapInMetaException(String backendName, String methodName, Throwable cause) {
    String message = "Backend catalog '" + backendName + "' failed in method '" + methodName
        + "' with " + cause.getClass().getSimpleName();
    if (cause.getMessage() != null && !cause.getMessage().isBlank()) {
      message += ": " + cause.getMessage();
    }
    MetaException metaException = new MetaException(message);
    metaException.initCause(cause);
    return metaException;
  }

  private static boolean isDeclaredMethodException(Method method, Throwable cause) {
    for (Class<?> declaredType : method.getExceptionTypes()) {
      if (declaredType == TException.class) {
        continue;
      }
      if (declaredType.isAssignableFrom(cause.getClass())) {
        return true;
      }
    }
    return false;
  }
}
