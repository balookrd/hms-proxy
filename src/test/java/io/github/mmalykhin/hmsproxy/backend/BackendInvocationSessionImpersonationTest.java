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

  @Test
  public void delegationTokenSelectorFindsTokenWithSignature() throws Exception {
    org.apache.hadoop.security.token.Token<?> token = new org.apache.hadoop.security.token.Token<>(
        new byte[] {1, 2, 3},
        new byte[] {4, 5, 6},
        new org.apache.hadoop.io.Text("HIVE_DELEGATION_TOKEN"),
        new org.apache.hadoop.io.Text("")
    );
    String tokenUrl = token.encodeToUrlString();

    org.apache.hadoop.security.token.Token<?> decoded = new org.apache.hadoop.security.token.Token<>();
    decoded.decodeFromUrlString(tokenUrl);

    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("alice");
    ugi.addToken(decoded);

    String tokenSig = "hdp";
    org.apache.hadoop.security.token.Token<?> signedToken = new org.apache.hadoop.security.token.Token<>(decoded);
    signedToken.setService(new org.apache.hadoop.io.Text(tokenSig));
    ugi.addToken(signedToken);

    org.apache.hadoop.hive.metastore.security.DelegationTokenSelector selector =
        new org.apache.hadoop.hive.metastore.security.DelegationTokenSelector();

    org.apache.hadoop.security.token.Token<?> foundBySig = selector.selectToken(new org.apache.hadoop.io.Text("hdp"), ugi.getTokens());
    Assert.assertNotNull(foundBySig);
    Assert.assertEquals("hdp", foundBySig.getService().toString());

    org.apache.hadoop.security.token.Token<?> foundDefault = selector.selectToken(new org.apache.hadoop.io.Text(""), ugi.getTokens());
    Assert.assertNotNull(foundDefault);
    Assert.assertEquals("", foundDefault.getService().toString());
  }
}
