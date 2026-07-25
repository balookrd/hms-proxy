package io.github.mmalykhin.hmsproxy.security;

import javax.security.auth.Subject;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class LoginSubjectsTest {
  @BeforeClass
  public static void ensureKerberosNameRules() {
    // UserGroupInformation resolves short names through auth_to_local rules, which are unset in
    // tests because no Hadoop security configuration is installed.
    if (!KerberosName.hasRulesBeenSet()) {
      KerberosName.setRules("RULE:[1:$1]\nRULE:[2:$1]\nDEFAULT");
    }
  }

  @Test
  public void readsSubjectOfLoginUser() {
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("hive/proxy@EXAMPLE.COM");
    Object credential = new Object();

    Subject subject = LoginSubjects.of(ugi);
    Assert.assertNotNull(subject);
    subject.getPrivateCredentials().add(credential);

    Assert.assertTrue(LoginSubjects.of(ugi).getPrivateCredentials().contains(credential));
  }

  @Test
  public void missingLoginUserHasNoSubject() {
    Assert.assertNull(LoginSubjects.of(null));
  }

  @Test
  public void unknownLoginUserTypeHasNoSubject() {
    Assert.assertNull(LoginSubjects.of("not a UserGroupInformation"));
  }

  @Test
  public void readsSubjectOfForeignLoginUserClass() {
    // Isolated metastore runtimes load their own UserGroupInformation class, so the subject is read
    // reflectively from whatever class the login user actually is.
    Subject subject = new Subject();

    Assert.assertSame(subject, LoginSubjects.of(new IsolatedLoginUser(subject)));
  }

  public static final class IsolatedLoginUser {
    private final Subject subject;

    IsolatedLoginUser(Subject subject) {
      this.subject = subject;
    }

    public Subject getSubject() {
      return subject;
    }
  }
}
