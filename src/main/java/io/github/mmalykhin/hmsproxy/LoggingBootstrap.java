package io.github.mmalykhin.hmsproxy;

import java.net.URL;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;

final class LoggingBootstrap {
  private LoggingBootstrap() {
  }

  static void initialize() {
    if (LogManager.getRootLogger().getAllAppenders().hasMoreElements()) {
      return;
    }

    URL config = LoggingBootstrap.class.getClassLoader().getResource("log4j.properties");
    if (config != null) {
      PropertyConfigurator.configure(config);
      return;
    }

    BasicConfigurator.configure();
  }
}
