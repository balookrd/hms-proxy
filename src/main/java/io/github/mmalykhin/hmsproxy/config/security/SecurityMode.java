package io.github.mmalykhin.hmsproxy.config.security;

public enum SecurityMode {
  NONE,
  KERBEROS;

  public String hadoopAuthValue() {
    return this == KERBEROS ? "kerberos" : "simple";
  }
}
