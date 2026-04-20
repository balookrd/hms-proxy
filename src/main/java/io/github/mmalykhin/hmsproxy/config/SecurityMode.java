package io.github.mmalykhin.hmsproxy.config;

public enum SecurityMode {
  NONE,
  KERBEROS;

  public String hadoopAuthValue() {
    return this == KERBEROS ? "kerberos" : "simple";
  }
}
