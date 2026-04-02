package io.github.mmalykhin.hmsproxy.observability;

import org.junit.Assert;
import org.junit.Test;

public class KerberosHealthProbeTest {
  @Test
  public void disabledStatusIsAlwaysHealthy() {
    KerberosHealthProbe.KerberosStatus status = KerberosHealthProbe.disabled("frontDoor");

    Assert.assertFalse(status.enabled());
    Assert.assertFalse(status.loggedIn());
    Assert.assertTrue(status.healthy());
    Assert.assertEquals("frontDoor", status.component());
  }
}
