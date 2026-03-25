package io.github.mmalykhin.hmsproxy;

import org.junit.Assert;
import org.junit.Test;

public class MetastoreThriftServerTest {
  @Test
  public void frontDoorAlwaysUsesServerPrincipalForInboundKerberos() {
    ProxyConfig.SecurityConfig security = new ProxyConfig.SecurityConfig(
        ProxyConfig.SecurityMode.KERBEROS,
        "hive/proxy-host.example.com@EXAMPLE.COM",
        "hive/backend-host.example.com@EXAMPLE.COM",
        "/tmp/proxy.keytab",
        "/tmp/backend.keytab",
        false);

    Assert.assertEquals(
        "hive/proxy-host.example.com@EXAMPLE.COM",
        MetastoreThriftServer.frontDoorClientPrincipal(security));
  }

  @Test
  public void frontDoorDoesNotExposeBackendClientPrincipal() {
    ProxyConfig.SecurityConfig security = new ProxyConfig.SecurityConfig(
        ProxyConfig.SecurityMode.KERBEROS,
        "hive/proxy-host.example.com@EXAMPLE.COM",
        "hive/backend-host.example.com@EXAMPLE.COM",
        "/tmp/proxy.keytab",
        "/tmp/backend.keytab",
        false);

    Assert.assertNotEquals(
        security.clientPrincipal(),
        MetastoreThriftServer.frontDoorClientPrincipal(security));
  }
}
