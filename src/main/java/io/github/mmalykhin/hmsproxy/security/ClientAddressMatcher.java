package io.github.mmalykhin.hmsproxy.security;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public final class ClientAddressMatcher {
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

  public boolean matches(String remoteAddress) {
    if (remoteAddress == null || remoteAddress.isBlank()) {
      return false;
    }
    byte[] candidate;
    try {
      candidate = InetAddress.getByName(remoteAddress.trim()).getAddress();
    } catch (UnknownHostException e) {
      return false;
    }
    if (candidate.length != network.length) {
      return false;
    }
    return matchesMasked(candidate);
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
    try {
      return InetAddress.getByName(value).getAddress();
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException("Invalid IP address in client address rule: " + value, e);
    }
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
}
