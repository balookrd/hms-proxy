package io.github.mmalykhin.hmsproxy.security;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Test;

public class OutboundKerberosUgiCacheTest {

  @Test
  public void cachesLoggedInUgiRefreshesOnReuseAndReloginsWhenRefreshFails() throws Exception {
    AtomicInteger loginCalls = new AtomicInteger();
    AtomicInteger refreshCalls = new AtomicInteger();
    AtomicBoolean failRefresh = new AtomicBoolean(false);

    OutboundKerberosUgiCache cache = new OutboundKerberosUgiCache(
        (principal, keytab) -> {
          loginCalls.incrementAndGet();
          return UserGroupInformation.createRemoteUser(principal);
        },
        ugi -> {
          refreshCalls.incrementAndGet();
          if (failRefresh.get()) {
            throw new IOException("TGT refresh failed");
          }
        });

    UserGroupInformation first = cache.resolve("hive/proxy@EXAMPLE.COM", "/etc/security/proxy.keytab");
    UserGroupInformation second = cache.resolve("hive/proxy@EXAMPLE.COM", "/etc/security/proxy.keytab");
    Assert.assertSame(first, second);
    Assert.assertEquals(1, loginCalls.get());
    Assert.assertEquals(1, refreshCalls.get());

    failRefresh.set(true);
    UserGroupInformation third = cache.resolve("hive/proxy@EXAMPLE.COM", "/etc/security/proxy.keytab");
    Assert.assertNotSame(first, third);
    Assert.assertEquals(2, loginCalls.get());
    Assert.assertEquals(2, refreshCalls.get());
  }
}
