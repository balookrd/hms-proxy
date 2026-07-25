package io.github.mmalykhin.hmsproxy.app;

import io.github.mmalykhin.hmsproxy.observability.KerberosHealthProbe;
import org.junit.Assert;
import org.junit.Test;

public class ManagementHttpServerReadinessJsonTest {
  @Test
  public void kerberosJsonReportsLoginStateAndReadiness() {
    KerberosHealthProbe.KerberosStatus status = new KerberosHealthProbe.KerberosStatus(
        "frontDoor",
        true,
        KerberosHealthProbe.LoginState.STALE,
        "hive/proxy-host.example.com@EXAMPLE.COM",
        1_700_000_000L,
        1_699_999_990L,
        -10L,
        "expired");

    String json = ManagementHttpServer.renderKerberos(status);

    Assert.assertTrue(json, json.contains("\"component\":\"frontDoor\""));
    Assert.assertTrue(json, json.contains("\"state\":\"STALE\""));
    Assert.assertTrue(json, json.contains("\"loggedIn\":false"));
    Assert.assertTrue(json, json.contains("\"healthy\":true"));
    Assert.assertTrue(json, json.contains("\"secondsUntilExpiry\":-10"));
    Assert.assertTrue(json, json.contains("\"detail\":\"expired\""));
  }

  @Test
  public void disabledKerberosJsonKeepsNullFields() {
    String json = ManagementHttpServer.renderKerberos(KerberosHealthProbe.disabled("backend"));

    Assert.assertTrue(json, json.contains("\"enabled\":false"));
    Assert.assertTrue(json, json.contains("\"state\":\"DISABLED\""));
    Assert.assertTrue(json, json.contains("\"healthy\":true"));
    Assert.assertTrue(json, json.contains("\"principal\":null"));
    Assert.assertTrue(json, json.contains("\"detail\":null"));
  }
}
