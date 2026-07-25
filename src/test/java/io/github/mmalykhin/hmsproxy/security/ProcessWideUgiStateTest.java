package io.github.mmalykhin.hmsproxy.security;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@code UserGroupInformation.setConfiguration} and {@code loginUserFromKeytab} mutate process-wide
 * Kerberos state, including auth_to_local rules and the login user shared by SASL handshakes.
 * Calling them outside startup replaces the live configuration, so they stay behind
 * {@link ProcessKerberosConfiguration}.
 */
public class ProcessWideUgiStateTest {
  private static final List<String> FORBIDDEN_CALLS = List.of(
      "UserGroupInformation.setConfiguration(",
      "UserGroupInformation.loginUserFromKeytab(");
  private static final String REFLECTIVE_CALL = "getMethod(\"setConfiguration\"";
  /** These reflect into an isolated class loader, whose UserGroupInformation is a different class. */
  private static final List<String> REFLECTIVE_CALL_ALLOWED_IN = List.of(
      "IsolatedMetastoreClient.java",
      "HmsMetastoreSmokeCli.java");

  @Test
  public void onlyProcessKerberosConfigurationMutatesProcessWideUgiState() throws IOException {
    List<String> violations = new ArrayList<>();
    for (Path source : mainSources()) {
      String fileName = source.getFileName().toString();
      if (fileName.equals("ProcessKerberosConfiguration.java")) {
        continue;
      }
      if (!REFLECTIVE_CALL_ALLOWED_IN.contains(fileName)) {
        List<String> reflective = Files.readAllLines(source);
        for (int index = 0; index < reflective.size(); index++) {
          if (reflective.get(index).contains(REFLECTIVE_CALL)) {
            violations.add(source + ":" + (index + 1) + " " + reflective.get(index).trim());
          }
        }
      }
      List<String> lines = Files.readAllLines(source);
      for (int index = 0; index < lines.size(); index++) {
        String line = lines.get(index);
        if (FORBIDDEN_CALLS.stream().anyMatch(line::contains)) {
          violations.add(source + ":" + (index + 1) + " " + line.trim());
        }
      }
    }

    Assert.assertEquals(
        "Process-wide UGI state must only be mutated through ProcessKerberosConfiguration",
        List.of(),
        violations);
  }

  private static List<Path> mainSources() throws IOException {
    try (Stream<Path> paths = Files.walk(Path.of("src", "main", "java"))) {
      return paths.filter(path -> path.toString().endsWith(".java")).collect(Collectors.toList());
    }
  }
}
