package io.github.mmalykhin.hmsproxy.util;

import org.junit.Assert;
import org.junit.Test;

public class PrincipalUtilTest {
  @Test
  public void extractsShortUserNameAndRejectsEmptyPrincipalPart() {
    Assert.assertEquals("hive", PrincipalUtil.shortUserName("hive/hms.example.com@EXAMPLE.COM"));
    Assert.assertEquals("alice", PrincipalUtil.shortUserName("alice@EXAMPLE.COM"));
    Assert.assertEquals("bob", PrincipalUtil.shortUserName("bob"));
    Assert.assertNull(PrincipalUtil.shortUserName("/hms.example.com@EXAMPLE.COM"));
    Assert.assertNull(PrincipalUtil.shortUserName("@EXAMPLE.COM"));
    Assert.assertNull(PrincipalUtil.shortUserName("   "));
    Assert.assertNull(PrincipalUtil.shortUserName(null));
  }

  @Test
  public void extractsRealmWhenPresent() {
    Assert.assertEquals("EXAMPLE.COM", PrincipalUtil.realm("hive/hms.example.com@EXAMPLE.COM"));
    Assert.assertEquals("SMOKE.LOCAL", PrincipalUtil.realm("alice@SMOKE.LOCAL"));
    Assert.assertNull(PrincipalUtil.realm("alice@"));
    Assert.assertNull(PrincipalUtil.realm("alice"));
    Assert.assertNull(PrincipalUtil.realm("   "));
    Assert.assertNull(PrincipalUtil.realm(null));
  }
}
