package io.github.mmalykhin.hmsproxy.util;

import org.junit.Assert;
import org.junit.Test;

public class ClientAddressMatcherTest {
  @Test
  public void matchesIpv4CidrAndSingleHost() {
    ClientAddressMatcher cidr = ClientAddressMatcher.parse("10.10.0.0/16");
    ClientAddressMatcher host = ClientAddressMatcher.parse("10.20.30.40");

    Assert.assertTrue(cidr.matches("10.10.1.10"));
    Assert.assertFalse(cidr.matches("10.11.1.10"));
    Assert.assertTrue(host.matches("10.20.30.40"));
    Assert.assertFalse(host.matches("10.20.30.41"));
  }

  @Test
  public void decodedAddressMatchesTheSameWayAsTheRawString() {
    ClientAddressMatcher ipv4 = ClientAddressMatcher.parse("10.10.0.0/16");
    ClientAddressMatcher ipv6 = ClientAddressMatcher.parse("2001:db8::/32");

    for (String candidate : new String[]{"10.10.1.10", " 10.10.1.10 ", "10.11.1.10", "2001:db8::1", "::1"}) {
      Assert.assertEquals(candidate, ipv4.matches(candidate), ipv4.matches(ClientAddressMatcher.decodeAddress(candidate)));
      Assert.assertEquals(candidate, ipv6.matches(candidate), ipv6.matches(ClientAddressMatcher.decodeAddress(candidate)));
    }
    Assert.assertTrue(ipv6.matches(ClientAddressMatcher.decodeAddress("2001:db8::1")));
    Assert.assertFalse(ipv4.matches(ClientAddressMatcher.decodeAddress("2001:db8::1")));
  }

  @Test
  public void blankAndUnparseableAddressesNeverMatch() {
    ClientAddressMatcher matcher = ClientAddressMatcher.parse("10.10.0.0/16");

    Assert.assertNull(ClientAddressMatcher.decodeAddress(null));
    Assert.assertNull(ClientAddressMatcher.decodeAddress("  "));
    Assert.assertFalse(matcher.matches((String) null));
    Assert.assertFalse(matcher.matches(""));
    Assert.assertFalse(matcher.matches((byte[]) null));
  }
}
