package io.github.mmalykhin.hmsproxy.docs;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

public class CapabilityMatrixSmokeCoverageTest {
  @Test
  public void capabilityIdsAreUniqueAndEveryCapabilityHasSmokeCoverage() throws IOException, ClassNotFoundException {
    CapabilityMatrixData.CapabilityMatrix matrix = CapabilityMatrixData.loadDefault();
    Set<String> ids = new HashSet<>();

    for (CapabilityMatrixData.Capability capability : matrix.capabilities()) {
      Assert.assertTrue("Duplicate capability id: " + capability.id(), ids.add(capability.id()));
      Assert.assertFalse("Capability " + capability.id() + " is missing smoke tests", capability.smokeTests().isEmpty());

      for (String smokeTest : capability.smokeTests()) {
        assertSmokeTestReference(smokeTest);
      }
    }
  }

  private static void assertSmokeTestReference(String smokeTest) throws ClassNotFoundException {
    int separator = smokeTest.indexOf('#');
    Assert.assertTrue("Smoke test reference must look like Class#method: " + smokeTest, separator > 0);

    String className = smokeTest.substring(0, separator);
    String methodName = smokeTest.substring(separator + 1);
    Assert.assertFalse("Smoke test method name is missing: " + smokeTest, methodName.isBlank());

    Class<?> testClass = Class.forName(className);
    Method method;
    try {
      method = testClass.getDeclaredMethod(methodName);
    } catch (NoSuchMethodException e) {
      Assert.fail("Smoke test method does not exist: " + smokeTest);
      return;
    }

    Assert.assertNotNull("Smoke test method is missing @Test: " + smokeTest, method.getAnnotation(Test.class));
  }
}
