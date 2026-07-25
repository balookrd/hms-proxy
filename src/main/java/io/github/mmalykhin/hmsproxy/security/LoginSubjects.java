package io.github.mmalykhin.hmsproxy.security;

import java.lang.reflect.Method;
import javax.security.auth.Subject;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Reads the {@link Subject} behind a Hadoop login user.
 *
 * <p>{@code UserGroupInformation.getSubject()} is protected, and the usual
 * {@code Subject.getSubject(AccessController.getContext())} workaround throws on JDK 24+, where the
 * security manager is permanently disabled. Reflection also covers login users loaded by an
 * isolated metastore class loader, whose {@code UserGroupInformation} class is a different class.
 */
public final class LoginSubjects {
  private LoginSubjects() {
  }

  public static Subject of(Object loginUser) {
    if (loginUser == null) {
      return null;
    }
    Subject subject = reflectiveSubject(loginUser);
    if (subject != null) {
      return subject;
    }
    return loginUser instanceof UserGroupInformation ugi ? doAsSubject(ugi) : null;
  }

  private static Subject reflectiveSubject(Object loginUser) {
    try {
      Method getSubject = loginUser.getClass().getMethod("getSubject");
      getSubject.setAccessible(true);
      Object subject = getSubject.invoke(loginUser);
      return subject instanceof Subject ? (Subject) subject : null;
    } catch (NoSuchMethodException e) {
      return declaredSubject(loginUser);
    } catch (ReflectiveOperationException | RuntimeException e) {
      return null;
    }
  }

  private static Subject declaredSubject(Object loginUser) {
    try {
      Method getSubject = loginUser.getClass().getDeclaredMethod("getSubject");
      getSubject.setAccessible(true);
      Object subject = getSubject.invoke(loginUser);
      return subject instanceof Subject ? (Subject) subject : null;
    } catch (ReflectiveOperationException | RuntimeException e) {
      return null;
    }
  }

  @SuppressWarnings("removal")
  private static Subject doAsSubject(UserGroupInformation ugi) {
    try {
      return ugi.doAs((java.security.PrivilegedExceptionAction<Subject>) () ->
          Subject.getSubject(java.security.AccessController.getContext()));
    } catch (Exception e) {
      return null;
    }
  }
}
