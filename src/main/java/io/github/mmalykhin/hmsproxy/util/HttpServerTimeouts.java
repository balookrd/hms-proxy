package io.github.mmalykhin.hmsproxy.util;

/**
 * Installs defensive defaults for the JDK built-in {@code HttpServer} system properties before
 * {@code sun.net.httpserver.ServerConfig} loads its static initializer, unless an operator has
 * already configured them explicitly via JVM {@code -D} flags.
 */
public final class HttpServerTimeouts {
  static final String MAX_REQ_TIME_PROP = "sun.net.httpserver.maxReqTime";
  static final String MAX_RSP_TIME_PROP = "sun.net.httpserver.maxRspTime";
  static final String MAX_CONNECTIONS_PROP = "jdk.httpserver.maxConnections";
  static final String MAX_IDLE_CONNECTIONS_PROP = "sun.net.httpserver.maxIdleConnections";
  static final String NODELAY_PROP = "sun.net.httpserver.nodelay";

  private HttpServerTimeouts() {
  }

  public static void installDefaultsIfUnset() {
    setIfAbsent(MAX_REQ_TIME_PROP, "30");
    setIfAbsent(MAX_RSP_TIME_PROP, "300");
    setIfAbsent(MAX_CONNECTIONS_PROP, "256");
    setIfAbsent(MAX_IDLE_CONNECTIONS_PROP, "64");
    setIfAbsent(NODELAY_PROP, "true");
  }

  private static void setIfAbsent(String key, String value) {
    if (System.getProperty(key) == null) {
      System.setProperty(key, value);
    }
  }
}
