package io.github.mmalykhin.hmsproxy.config;

import io.github.mmalykhin.hmsproxy.security.KerberosPrincipalUtil;
import org.junit.Assert;
import org.junit.Test;

public class KerberosPrincipalUtilTest {
  @Test
  public void resolvesHostPlaceholderForGivenHostname() throws Exception {
    Assert.assertEquals(
        "hive/algaraev.dmp.vimpelcom.ru@EXAMPLE.COM",
        KerberosPrincipalUtil.resolveForHost("hive/_HOST@EXAMPLE.COM", "algaraev.dmp.vimpelcom.ru"));
  }

  @Test
  public void leavesConcretePrincipalUnchanged() throws Exception {
    Assert.assertEquals(
        "hive/algaraev.dmp.vimpelcom.ru@EXAMPLE.COM",
        KerberosPrincipalUtil.resolveForHost(
            "hive/algaraev.dmp.vimpelcom.ru@EXAMPLE.COM",
            "anything.example.com"));
  }
}
