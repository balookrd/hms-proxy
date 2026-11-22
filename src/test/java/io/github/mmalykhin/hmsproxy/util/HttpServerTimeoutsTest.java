package io.github.mmalykhin.hmsproxy.util;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class HttpServerTimeoutsTest {
  private static final List<String> ALL_PROPS = List.of(
      HttpServerTimeouts.MAX_REQ_TIME_PROP,
      HttpServerTimeouts.MAX_RSP_TIME_PROP,
      HttpServerTimeouts.MAX_CONNECTIONS_PROP,
      HttpServerTimeouts.MAX_IDLE_CONNECTIONS_PROP,
      HttpServerTimeouts.NODELAY_PROP);

  @Test
  public void installDefaultsIfUnsetPopulatesMissingPropertiesWithSafeDefaults() {
    withCleanProperties(() -> {
      HttpServerTimeouts.installDefaultsIfUnset();

      Assert.assertEquals("30", System.getProperty(HttpServerTimeouts.MAX_REQ_TIME_PROP));
      Assert.assertEquals("300", System.getProperty(HttpServerTimeouts.MAX_RSP_TIME_PROP));
      Assert.assertEquals("256", System.getProperty(HttpServerTimeouts.MAX_CONNECTIONS_PROP));
      Assert.assertEquals("64", System.getProperty(HttpServerTimeouts.MAX_IDLE_CONNECTIONS_PROP));
      Assert.assertEquals("true", System.getProperty(HttpServerTimeouts.NODELAY_PROP));
    });
  }

  @Test
  public void installDefaultsIfUnsetPreservesExplicitOperatorOverrides() {
    withCleanProperties(() -> {
      System.setProperty(HttpServerTimeouts.MAX_RSP_TIME_PROP, "900");
      System.setProperty(HttpServerTimeouts.NODELAY_PROP, "false");

      HttpServerTimeouts.installDefaultsIfUnset();

      Assert.assertEquals("30", System.getProperty(HttpServerTimeouts.MAX_REQ_TIME_PROP));
      Assert.assertEquals("900", System.getProperty(HttpServerTimeouts.MAX_RSP_TIME_PROP));
      Assert.assertEquals("256", System.getProperty(HttpServerTimeouts.MAX_CONNECTIONS_PROP));
      Assert.assertEquals("64", System.getProperty(HttpServerTimeouts.MAX_IDLE_CONNECTIONS_PROP));
      Assert.assertEquals("false", System.getProperty(HttpServerTimeouts.NODELAY_PROP));
    });
  }

  private static void withCleanProperties(Runnable body) {
    Map<String, String> saved = new LinkedHashMap<>();
    for (String prop : ALL_PROPS) {
      saved.put(prop, System.getProperty(prop));
      System.clearProperty(prop);
    }
    try {
      body.run();
    } finally {
      for (Map.Entry<String, String> entry : saved.entrySet()) {
        if (entry.getValue() == null) {
          System.clearProperty(entry.getKey());
        } else {
          System.setProperty(entry.getKey(), entry.getValue());
        }
      }
    }
  }
}
