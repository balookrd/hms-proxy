package io.github.mmalykhin.hmsproxy;

import java.net.URL;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;

final class LoggingBootstrap {
  private static final String CONFIG_RESOURCE = "hms-proxy-log4j2.properties";

  private LoggingBootstrap() {
  }

  static void initialize() {
    URL resource = LoggingBootstrap.class.getClassLoader().getResource(CONFIG_RESOURCE);
    if (resource == null) {
      System.err.println("WARN: bundled logging config not found: " + CONFIG_RESOURCE);
      return;
    }

    String current = System.getProperty("log4j.configurationFile");
    String target = resource.toExternalForm();
    if (!target.equals(current)) {
      System.setProperty("log4j.configurationFile", target);
    }

    try {
      LoggerContext context = (LoggerContext) LogManager.getContext(false);
      context.setConfigLocation(resource.toURI());
      context.reconfigure();
    } catch (Exception e) {
      System.err.println("WARN: failed to reconfigure Log4j2 from " + target + ": " + e);
    }
  }
}
