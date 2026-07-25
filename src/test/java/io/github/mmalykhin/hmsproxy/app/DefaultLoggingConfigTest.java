package io.github.mmalykhin.hmsproxy.app;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import org.apache.log4j.Hierarchy;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.spi.RootLogger;
import org.junit.Assert;
import org.junit.Test;

/**
 * Guards the logging defaults shipped inside the fat jar. The previous default set the proxy
 * package to DEBUG with additivity off and no appenders of its own, which both dropped every audit
 * record and paid the debug rendering cost on every request.
 */
public class DefaultLoggingConfigTest {
  private static final Path DEFAULT_CONFIG = Path.of("src/main/resources/log4j.properties");
  private static final String AUDIT_LOGGER = "io.github.mmalykhin.hmsproxy.audit";
  private static final String PROXY_LOGGER = "io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreProxy";

  @Test
  public void proxyPackageDoesNotLogAtDebugByDefault() throws Exception {
    Path directory = Files.createTempDirectory("hms-proxy-logging");
    try {
      Hierarchy hierarchy = configure(directory);

      Logger proxy = hierarchy.getLogger(PROXY_LOGGER);

      Assert.assertFalse("debug tracing must be opt-in", proxy.isDebugEnabled());
      Assert.assertTrue(proxy.isInfoEnabled());
      hierarchy.shutdown();
    } finally {
      deleteRecursively(directory);
    }
  }

  @Test
  public void auditRecordsReachTheirOwnFileAndStayOutOfTheGeneralLog() throws Exception {
    Path directory = Files.createTempDirectory("hms-proxy-logging");
    try {
      Hierarchy hierarchy = configure(directory);
      Logger audit = hierarchy.getLogger(AUDIT_LOGGER);
      Logger proxy = hierarchy.getLogger(PROXY_LOGGER);

      Assert.assertTrue(audit.isInfoEnabled());
      Assert.assertTrue("audit logger needs an appender of its own",
          audit.getAllAppenders().hasMoreElements());
      Assert.assertFalse("audit must not duplicate into the general log", audit.getAdditivity());

      String record = "{\"event\":\"hms_proxy_audit\",\"requestId\":42}";
      audit.info(record);
      proxy.info("general proxy line");
      hierarchy.shutdown();

      List<String> auditLines = Files.readAllLines(directory.resolve("hms-proxy-audit.log"));
      // No layout prefix: the audit file has to stay parseable as JSON lines.
      Assert.assertEquals(List.of(record), auditLines);

      String general = Files.readString(directory.resolve("hms-proxy.log"));
      Assert.assertTrue(general.contains("general proxy line"));
      Assert.assertFalse(general.contains("hms_proxy_audit"));
    } finally {
      deleteRecursively(directory);
    }
  }

  @Test
  public void rootLoggerDoesNotWriteEveryLineToMultipleFiles() throws Exception {
    Properties properties = loadDefaults();

    String rootLogger = properties.getProperty("log4j.rootLogger");

    Assert.assertNotNull(rootLogger);
    long fileAppenders = java.util.Arrays.stream(rootLogger.split(","))
        .map(String::trim)
        .filter(name -> properties.getProperty("log4j.appender." + name, "").contains("FileAppender"))
        .count();
    Assert.assertEquals("root should write to a single file to avoid duplicating every line",
        1L, fileAppenders);
    Assert.assertFalse("DailyRollingFileAppender has no backup limit and grows without bound",
        properties.values().stream().anyMatch(value -> String.valueOf(value).contains("DailyRollingFileAppender")));
  }

  private static Hierarchy configure(Path directory) throws IOException {
    Properties properties = loadDefaults();
    properties.setProperty("log4j.appender.rollingFile.File",
        directory.resolve("hms-proxy.log").toString());
    properties.setProperty("log4j.appender.auditFile.File",
        directory.resolve("hms-proxy-audit.log").toString());
    // Keep the test output clean; the console appender itself is not what this test asserts on.
    properties.setProperty("log4j.rootLogger",
        properties.getProperty("log4j.rootLogger").replace("console,", "").replace(", console", ""));

    Hierarchy hierarchy = new Hierarchy(new RootLogger(Level.DEBUG));
    new PropertyConfigurator().doConfigure(properties, hierarchy);
    return hierarchy;
  }

  private static Properties loadDefaults() throws IOException {
    Properties properties = new Properties();
    try (BufferedReader reader = Files.newBufferedReader(DEFAULT_CONFIG)) {
      properties.load(reader);
    }
    return properties;
  }

  private static void deleteRecursively(Path directory) throws IOException {
    try (var paths = Files.walk(directory)) {
      paths.sorted(Comparator.reverseOrder()).forEach(path -> {
        try {
          Files.deleteIfExists(path);
        } catch (IOException ignored) {
          // Best effort temp cleanup.
        }
      });
    }
  }
}
