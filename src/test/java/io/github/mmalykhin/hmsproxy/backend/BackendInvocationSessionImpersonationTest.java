package io.github.mmalykhin.hmsproxy.backend;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class BackendInvocationSessionImpersonationTest {

  @BeforeClass
  public static void ensureKerberosNameRules() {
    if (!KerberosName.hasRulesBeenSet()) {
      KerberosName.setRules("RULE:[1:$1]\nRULE:[2:$1]\nDEFAULT");
    }
  }

  @Test
  public void proxyUserWrapsLoginUserCorrectly() {
    UserGroupInformation loginUser = UserGroupInformation.createRemoteUser("hive/hd-hdp-31-08.dmp.vimpelcom.ru@EXAMPLE.COM");
    UserGroupInformation proxyUser = UserGroupInformation.createProxyUser("alice", loginUser);

    Assert.assertEquals("alice", proxyUser.getShortUserName());
    Assert.assertEquals("alice", proxyUser.getUserName());
    Assert.assertSame(loginUser, proxyUser.getRealUser());
    Assert.assertEquals(UserGroupInformation.AuthenticationMethod.PROXY, proxyUser.getAuthenticationMethod());
  }

  @Test
  public void proxyUserPreservesRealUserKerberosSubject() {
    UserGroupInformation serviceUser = UserGroupInformation.createRemoteUser("hive/proxy-host@EXAMPLE.COM");
    UserGroupInformation proxyUser = UserGroupInformation.createProxyUser("bob", serviceUser);

    Assert.assertEquals("bob", proxyUser.getUserName());
    Assert.assertEquals("hive", proxyUser.getRealUser().getShortUserName());
  }
}
