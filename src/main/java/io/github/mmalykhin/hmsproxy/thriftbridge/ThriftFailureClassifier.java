package io.github.mmalykhin.hmsproxy.thriftbridge;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.SocketTimeoutException;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.transport.TTransportException;

/**
 * Single classifier for backend Thrift failures. A {@link TApplicationException} is an
 * application-level reply on a live connection, so only its {@code type} tells whether the backend
 * is missing the method, corrupted the response stream, or simply failed the call.
 */
public final class ThriftFailureClassifier {
  private static final int MAX_UNWRAP_DEPTH = 4;
  private static final int MAX_CAUSE_DEPTH = 8;
  private static final String META_EXCEPTION_CLASS = "org.apache.hadoop.hive.metastore.api.MetaException";
  private static final String CONNECT_FAILURE_MESSAGE =
      "Could not connect to meta store using any of the URIs provided";

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
   * Whether a transport failure allows sending the call again on a fresh session. Only
   * {@code NOT_OPEN} is raised before the request is written; every other transport failure - a
   * read timeout, a reset or EOF while waiting for the reply - may arrive after the backend applied
   * the call, so replaying it is safe only for a call that changes no state.
   */
  public static boolean isSafeToReplay(Throwable cause, boolean callChangesNoState) {
    if (!(unwrap(cause) instanceof TTransportException transport)) {
      return false;
    }
    return callChangesNoState || transport.getType() == TTransportException.NOT_OPEN;
  }

  /**
   * The backend did not answer within the socket timeout. The connection was alive when the call
   * went out, so this failure says nothing about the other pooled sessions - unlike an EOF or a
   * reset, after which the idle ones are usually dead as well. libthrift 0.9.3 reports a socket
   * read timeout as type {@code UNKNOWN} with the {@code SocketTimeoutException} as cause, and an
   * exception mapped across an isolated loader may keep only the text.
   */
  public static boolean isReadTimeout(Throwable cause) {
    if (!(unwrap(cause) instanceof TTransportException transport)) {
      return false;
    }
    if (transport.getType() == TTransportException.TIMED_OUT) {
      return true;
    }
    Throwable current = transport;
    for (int depth = 0; current != null && depth < MAX_CAUSE_DEPTH; depth++) {
      if (current instanceof SocketTimeoutException) {
        return true;
      }
      String message = current.getMessage();
      if (message != null && (message.contains("SocketTimeoutException") || message.contains("timed out"))) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  /**
   * Where a transport failure hit the generated Thrift client: {@code send} while writing the
   * request, {@code receive} while waiting for the reply, {@code unknown} when the stack trace
   * names neither. Diagnostic label only - no replay decision may be taken from it.
   */
  public static String transportFailurePhase(Throwable cause) {
    Throwable current = unwrap(cause);
    for (int depth = 0; current != null && depth < MAX_CAUSE_DEPTH; depth++) {
      for (StackTraceElement frame : current.getStackTrace()) {
        String method = frame.getMethodName();
        if (method.startsWith("send_") || method.equals("sendBase")) {
          return "send";
        }
        if (method.startsWith("recv_") || method.equals("receiveBase")) {
          return "receive";
        }
      }
      current = current.getCause();
    }
    return "unknown";
  }

  /**
   * The metastore client could not open a connection to any configured URI. {@code
   * HiveMetaStoreClient.open()} reports this as a {@code MetaException} that carries the transport
   * failure only in its message, not as a cause, so {@link #isTransportFailure} never sees it. The
   * message is the same in every bundled runtime (3.1 lines and Hive 4); the class is matched by
   * name because the exception may come from an isolated runtime class loader. No request reached
   * the backend, so this says nothing about replaying a call.
   */
  public static boolean isConnectFailure(Throwable cause) {
    Throwable current = cause;
    for (int depth = 0; current != null && depth < MAX_CAUSE_DEPTH; depth++) {
      String message = current.getMessage();
      if (META_EXCEPTION_CLASS.equals(current.getClass().getName())
          && message != null
          && message.startsWith(CONNECT_FAILURE_MESSAGE)) {
        return true;
      }
      current = current.getCause();
    }
    return false;
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
