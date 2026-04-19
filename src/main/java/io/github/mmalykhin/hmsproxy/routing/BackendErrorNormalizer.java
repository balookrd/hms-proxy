package io.github.mmalykhin.hmsproxy.routing;

import java.lang.reflect.Method;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;

final class BackendErrorNormalizer {
  private BackendErrorNormalizer() {}

  static Throwable normalize(Method method, String backendName, Throwable cause) {
    if (!(cause instanceof TException) || isDeclaredMethodException(method, cause)) {
      return cause;
    }
    String message = "Backend catalog '" + backendName + "' failed in method '" + method.getName()
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
