package io.github.mmalykhin.hmsproxy.docs;

import java.io.IOException;
import java.nio.file.Files;
import org.junit.Assert;
import org.junit.Test;

public class CapabilityMatrixDocSyncTest {
  @Test
  public void englishReadmeMatchesGeneratedCapabilityMatrix() throws IOException {
    assertReadmeSync(
        CapabilityMatrixData.README_PATH,
        CapabilityMatrixData.Language.EN);
  }

  @Test
  public void russianReadmeMatchesGeneratedCapabilityMatrix() throws IOException {
    assertReadmeSync(
        CapabilityMatrixData.README_RU_PATH,
        CapabilityMatrixData.Language.RU);
  }

  private static void assertReadmeSync(
      java.nio.file.Path readmePath,
      CapabilityMatrixData.Language language
  ) throws IOException {
    CapabilityMatrixData.CapabilityMatrix matrix = CapabilityMatrixData.loadDefault();
    String current = Files.readString(readmePath);
    String expected = CapabilityMatrixData.syncReadme(current, matrix, language);

    if (Boolean.getBoolean("capabilities.updateReadme")) {
      if (!current.equals(expected)) {
        Files.writeString(readmePath, expected);
      }
      return;
    }

    Assert.assertEquals(
        "Generated capability matrix is out of date in " + readmePath
            + ". Refresh it with: mvn -o -q -Dtest=CapabilityMatrixDocSyncTest -Dcapabilities.updateReadme=true test",
        expected,
        current);
  }
}
