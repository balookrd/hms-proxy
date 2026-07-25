package io.github.mmalykhin.hmsproxy.thriftbridge;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.transport.TTransportException;

/**
 * Single classifier for backend Thrift failures. A {@link TApplicationException} is an
 * application-level reply on a live connection, so only its {@code type} tells whether the backend
 * is missing the method, corrupted the response stream, or simply failed the call.
 */
public final class ThriftFailureClassifier {
  private static final int MAX_UNWRAP_DEPTH = 4;

  private ThriftFailureClassifier() {
  }

  /**
   * The backend, or the loaded metastore runtime, does not implement the method at all. Only this
   * category may trigger a legacy downgrade or an empty compatibility response.
   *
   * <p>{@code INVALID_PROTOCOL} and {@code UNSUPPORTED_CLIENT_TYPE} describe the whole connection
   * rather than one method, and a legacy downgrade would speak the same protocol on the same
   * connection, so they are deliberately excluded. {@code WRONG_METHOD_NAME} means the reply
   * belongs to another call, which is a desync, not a missing method.
   */
  public static boolean isUnsupportedMethod(Throwable cause) {
    Throwable root = unwrap(cause);
    if (root instanceof NoSuchMethodException || root instanceof NoSuchMethodError) {
      return true;
    }
    return root instanceof TApplicationException application
        && application.getType() == TApplicationException.UNKNOWN_METHOD;
  }

  /**
   * Connection-level failure. The session is unusable and the call may be retried once on a fresh
   * session.
   */
  public static boolean isTransportFailure(Throwable cause) {
    return unwrap(cause) instanceof TTransportException;
  }

  /**
   * Application-level protocol desync: the reply does not belong to the call that is waiting for
   * it. The connection must be dropped, but the call must not be replayed, because the backend may
   * already have applied it.
   */
  public static boolean isProtocolDesync(Throwable cause) {
    if (!(unwrap(cause) instanceof TApplicationException application)) {
      return false;
    }
    return application.getType() == TApplicationException.WRONG_METHOD_NAME
        || application.getType() == TApplicationException.BAD_SEQUENCE_ID
        || application.getType() == TApplicationException.INVALID_MESSAGE_TYPE;
  }

  private static Throwable unwrap(Throwable cause) {
    Throwable current = cause;
    for (int depth = 0; depth < MAX_UNWRAP_DEPTH; depth++) {
      boolean reflective = current instanceof InvocationTargetException
          || current instanceof UndeclaredThrowableException;
      if (!reflective || current.getCause() == null) {
        return current;
      }
      current = current.getCause();
    }
    return current;
  }
}
