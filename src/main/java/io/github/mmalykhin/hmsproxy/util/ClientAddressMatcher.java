package io.github.mmalykhin.hmsproxy.util;

import java.util.ArrayList;
import java.util.List;

public final class ClientAddressMatcher {
  private static final int IPV4_BYTES = 4;
  private static final int IPV6_BYTES = 16;

  private final byte[] network;
  private final int prefixLength;
  private final String source;

  private ClientAddressMatcher(byte[] network, int prefixLength, String source) {
    this.network = network;
    this.prefixLength = prefixLength;
    this.source = source;
  }

  public static List<ClientAddressMatcher> parseAll(List<String> rules) {
    List<ClientAddressMatcher> matchers = new ArrayList<>(rules.size());
    for (String rule : rules) {
      matchers.add(parse(rule));
    }
    return List.copyOf(matchers);
  }

  public static ClientAddressMatcher parse(String rule) {
    if (rule == null || rule.isBlank()) {
      throw new IllegalArgumentException("Client address rule must not be blank");
    }
    String trimmed = rule.trim();
    int separatorIndex = trimmed.indexOf('/');
    if (separatorIndex < 0) {
      byte[] address = parseAddress(trimmed);
      return new ClientAddressMatcher(address, address.length * 8, trimmed);
    }

    String addressPart = trimmed.substring(0, separatorIndex).trim();
    String prefixPart = trimmed.substring(separatorIndex + 1).trim();
    if (addressPart.isEmpty() || prefixPart.isEmpty()) {
      throw new IllegalArgumentException("Invalid client address rule: " + trimmed);
    }
    byte[] address = parseAddress(addressPart);
    int maxPrefixLength = address.length * 8;
    int prefixLength;
    try {
      prefixLength = Integer.parseInt(prefixPart);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid CIDR prefix length in client address rule: " + trimmed, e);
    }
    if (prefixLength < 0 || prefixLength > maxPrefixLength) {
      throw new IllegalArgumentException(
          "CIDR prefix length out of range in client address rule: " + trimmed);
    }
    return new ClientAddressMatcher(applyMask(address, prefixLength), prefixLength, trimmed);
  }

  /**
   * Decodes a numeric IPv4 or IPv6 literal into raw bytes without performing DNS resolution,
   * returning null when blank or invalid. Callers that check one address against several rules
   * should decode once and use {@link #matches(byte[])}.
   */
  public static byte[] decodeAddress(String remoteAddress) {
    if (remoteAddress == null || remoteAddress.isBlank()) {
      return null;
    }
    String trimmed = remoteAddress.trim();
    if (trimmed.startsWith("[") || trimmed.endsWith("]")) {
      if (!(trimmed.startsWith("[") && trimmed.endsWith("]") && trimmed.length() > 2)) {
        return null;
      }
      trimmed = trimmed.substring(1, trimmed.length() - 1).trim();
    }
    int zoneIndex = trimmed.indexOf('%');
    if (zoneIndex >= 0) {
      if (zoneIndex == 0 || zoneIndex == trimmed.length() - 1 || !trimmed.contains(":")) {
        return null;
      }
      trimmed = trimmed.substring(0, zoneIndex);
    }
    if (trimmed.indexOf(':') >= 0) {
      return parseIpv6Literal(trimmed);
    }
    return parseIpv4Literal(trimmed);
  }

  public boolean matches(String remoteAddress) {
    return matches(decodeAddress(remoteAddress));
  }

  public boolean matches(byte[] remoteAddress) {
    if (remoteAddress == null) {
      return false;
    }
    if (remoteAddress.length == network.length) {
      return matchesMasked(remoteAddress);
    }
    if (remoteAddress.length == IPV4_BYTES
        && network.length == IPV6_BYTES
        && prefixLength >= 96
        && isIpv4MappedIpv6(network)) {
      return matchesMasked(toIpv4MappedIpv6(remoteAddress));
    }
    if (remoteAddress.length == IPV6_BYTES
        && network.length == IPV4_BYTES
        && isIpv4MappedIpv6(remoteAddress)) {
      return matchesMasked(extractIpv4FromMapped(remoteAddress));
    }
    return false;
  }

  @Override
  public String toString() {
    return source;
  }

  private boolean matchesMasked(byte[] candidate) {
    int fullBytes = prefixLength / 8;
    int remainderBits = prefixLength % 8;
    for (int index = 0; index < fullBytes; index++) {
      if (candidate[index] != network[index]) {
        return false;
      }
    }
    if (remainderBits == 0) {
      return true;
    }
    int mask = 0xFF << (8 - remainderBits);
    return (candidate[fullBytes] & mask) == (network[fullBytes] & mask);
  }

  private static byte[] parseAddress(String value) {
    byte[] parsed = decodeAddress(value);
    if (parsed == null) {
      throw new IllegalArgumentException("Invalid IP address in client address rule: " + value);
    }
    return parsed;
  }

  private static byte[] applyMask(byte[] address, int prefixLength) {
    byte[] masked = address.clone();
    int fullBytes = prefixLength / 8;
    int remainderBits = prefixLength % 8;
    if (remainderBits != 0 && fullBytes < masked.length) {
      int mask = 0xFF << (8 - remainderBits);
      masked[fullBytes] = (byte) (masked[fullBytes] & mask);
      fullBytes++;
    }
    for (int index = fullBytes; index < masked.length; index++) {
      masked[index] = 0;
    }
    return masked;
  }

  private static byte[] parseIpv4Literal(String s) {
    if (s.isEmpty()) {
      return null;
    }
    byte[] out = new byte[IPV4_BYTES];
    int part = 0;
    int len = s.length();
    int idx = 0;
    while (idx < len) {
      if (part >= IPV4_BYTES) {
        return null;
      }
      int digits = 0;
      int value = 0;
      while (idx < len && s.charAt(idx) != '.') {
        char c = s.charAt(idx++);
        if (c < '0' || c > '9' || digits == 3) {
          return null;
        }
        value = value * 10 + (c - '0');
        digits++;
      }
      if (digits == 0 || value > 255) {
        return null;
      }
      out[part++] = (byte) value;
      if (idx < len) {
        // Consumed '.' — must not be trailing dot
        if (idx == len - 1) {
          return null;
        }
        idx++;
      }
    }
    return part == IPV4_BYTES ? out : null;
  }

  private static byte[] parseIpv6Literal(String s) {
    int doubleColon = s.indexOf("::");
    if (doubleColon >= 0 && s.indexOf("::", doubleColon + 2) >= 0) {
      return null;
    }
    String left = doubleColon >= 0 ? s.substring(0, doubleColon) : s;
    String right = doubleColon >= 0 ? s.substring(doubleColon + 2) : null;

    if (left.indexOf('.') >= 0 && right != null) {
      // Embedded IPv4 may only appear in the trailing part after '::'
      return null;
    }

    int[] leftHex = parseHexGroups(left, doubleColon < 0);
    if (leftHex == null) {
      return null;
    }
    if (doubleColon < 0) {
      if (left.indexOf('.') >= 0) {
        int lastColon = left.lastIndexOf(':');
        if (lastColon <= 0) {
          return null;
        }
        int[] prefix = parseHexGroups(left.substring(0, lastColon), false);
        byte[] ipv4 = parseIpv4Literal(left.substring(lastColon + 1));
        if (prefix == null || ipv4 == null || prefix.length != 6) {
          return null;
        }
        return assembleIpv6(prefix, new int[0], ipv4);
      }
      return leftHex.length == 8 ? assembleIpv6(leftHex, new int[0], null) : null;
    }

    byte[] ipv4Tail = null;
    int[] rightHex = new int[0];
    if (right != null && !right.isEmpty()) {
      if (right.indexOf('.') >= 0) {
        int lastColon = right.lastIndexOf(':');
        String rightPrefix = lastColon >= 0 ? right.substring(0, lastColon) : "";
        String ipv4Part = lastColon >= 0 ? right.substring(lastColon + 1) : right;
        if (!rightPrefix.isEmpty()) {
          rightHex = parseHexGroups(rightPrefix, false);
          if (rightHex == null) {
            return null;
          }
        }
        ipv4Tail = parseIpv4Literal(ipv4Part);
        if (ipv4Tail == null) {
          return null;
        }
      } else {
        rightHex = parseHexGroups(right, false);
        if (rightHex == null) {
          return null;
        }
      }
    }

    int totalGroups = leftHex.length + rightHex.length + (ipv4Tail != null ? 2 : 0);
    if (totalGroups >= 8) {
      return null;
    }
    return assembleIpv6(leftHex, rightHex, ipv4Tail);
  }

  private static int[] parseHexGroups(String segment, boolean allowTrailingIpv4Ignored) {
    if (segment.isEmpty()) {
      return new int[0];
    }
    if (allowTrailingIpv4Ignored && segment.indexOf('.') >= 0) {
      return new int[0];
    }
    if (segment.startsWith(":") || segment.endsWith(":")) {
      return null;
    }
    String[] parts = segment.split(":", -1);
    int[] values = new int[parts.length];
    for (int i = 0; i < parts.length; i++) {
      String group = parts[i];
      if (group.isEmpty() || group.length() > 4) {
        return null;
      }
      int val = 0;
      for (int j = 0; j < group.length(); j++) {
        int digit = Character.digit(group.charAt(j), 16);
        if (digit < 0) {
          return null;
        }
        val = (val << 4) | digit;
      }
      values[i] = val;
    }
    return values;
  }

  private static byte[] assembleIpv6(int[] left, int[] right, byte[] ipv4Tail) {
    byte[] out = new byte[IPV6_BYTES];
    int pos = 0;
    for (int group : left) {
      out[pos++] = (byte) ((group >> 8) & 0xFF);
      out[pos++] = (byte) (group & 0xFF);
    }
    int rightBytes = right.length * 2 + (ipv4Tail != null ? IPV4_BYTES : 0);
    int zeroBytes = IPV6_BYTES - pos - rightBytes;
    if (zeroBytes < 0) {
      return null;
    }
    pos += zeroBytes;
    for (int group : right) {
      out[pos++] = (byte) ((group >> 8) & 0xFF);
      out[pos++] = (byte) (group & 0xFF);
    }
    if (ipv4Tail != null) {
      System.arraycopy(ipv4Tail, 0, out, pos, IPV4_BYTES);
    }
    return out;
  }

  private static boolean isIpv4MappedIpv6(byte[] addr) {
    for (int i = 0; i < 10; i++) {
      if (addr[i] != 0) {
        return false;
      }
    }
    return addr[10] == (byte) 0xFF && addr[11] == (byte) 0xFF;
  }

  private static byte[] toIpv4MappedIpv6(byte[] ipv4) {
    byte[] mapped = new byte[IPV6_BYTES];
    mapped[10] = (byte) 0xFF;
    mapped[11] = (byte) 0xFF;
    System.arraycopy(ipv4, 0, mapped, 12, IPV4_BYTES);
    return mapped;
  }

  private static byte[] extractIpv4FromMapped(byte[] mapped) {
    byte[] ipv4 = new byte[IPV4_BYTES];
    System.arraycopy(mapped, 12, ipv4, 0, IPV4_BYTES);
    return ipv4;
  }
}
