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
    Assert.assertNull(ClientAddressMatcher.decodeAddress("localhost"));
    Assert.assertNull(ClientAddressMatcher.decodeAddress("example.com"));
    Assert.assertNull(ClientAddressMatcher.decodeAddress("256.1.1.1"));
    Assert.assertNull(ClientAddressMatcher.decodeAddress("127.1"));
    Assert.assertNull(ClientAddressMatcher.decodeAddress("1::2::3"));
    Assert.assertFalse(matcher.matches((String) null));
    Assert.assertFalse(matcher.matches(""));
    Assert.assertFalse(matcher.matches("localhost"));
    Assert.assertFalse(matcher.matches((byte[]) null));
    try {
      ClientAddressMatcher.parse("localhost/24");
      Assert.fail("Hostname rules must be rejected without DNS lookup");
    } catch (IllegalArgumentException expected) {
      // Expected.
    }
  }

  @Test
  public void supportsIpv4MappedIpv6CidrsWithoutBreakingNormalIpv6Cidrs() {
    ClientAddressMatcher mappedCidr = ClientAddressMatcher.parse("::ffff:10.0.0.0/104");
    ClientAddressMatcher ipv4Cidr = ClientAddressMatcher.parse("10.0.0.0/8");
    ClientAddressMatcher normalIpv6Cidr = ClientAddressMatcher.parse("::/64");

    Assert.assertTrue(mappedCidr.matches("10.0.0.5"));
    Assert.assertTrue(mappedCidr.matches("::ffff:10.0.0.5"));
    Assert.assertFalse(mappedCidr.matches("11.0.0.5"));
    Assert.assertFalse(mappedCidr.matches("::ffff:11.0.0.5"));

    Assert.assertTrue(ipv4Cidr.matches("::ffff:10.0.0.5"));
    Assert.assertFalse(ipv4Cidr.matches("::ffff:11.0.0.5"));
    Assert.assertFalse(ipv4Cidr.matches("2001:db8::1"));

    Assert.assertFalse("Normal IPv6 CIDR ::/64 must not match IPv4 addresses",
        normalIpv6Cidr.matches("10.0.0.1"));
    Assert.assertTrue(normalIpv6Cidr.matches("::1"));
    Assert.assertTrue(ClientAddressMatcher.parse("[fe80::1%eth0]").matches("fe80::1"));
  }
}
