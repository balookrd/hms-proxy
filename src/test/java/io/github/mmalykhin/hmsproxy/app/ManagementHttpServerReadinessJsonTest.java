package io.github.mmalykhin.hmsproxy.app;

import io.github.mmalykhin.hmsproxy.security.KerberosHealthProbe;
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

  @Test
  public void kerberosJsonOmitsPrincipalAndSanitizesDetailWhenIncludeDetailsIsFalse() {
    KerberosHealthProbe.KerberosStatus status = new KerberosHealthProbe.KerberosStatus(
        "frontDoor",
        true,
        KerberosHealthProbe.LoginState.FAILED,
        "hive/internal-host.corp@EXAMPLE.COM",
        1_700_000_000L,
        null,
        null,
        "Keytab /etc/security/keytabs/secret.keytab not readable");

    String sanitized = ManagementHttpServer.renderKerberos(status, false);

    Assert.assertTrue(sanitized, sanitized.contains("\"principal\":null"));
    Assert.assertTrue(sanitized, sanitized.contains("\"detail\":\"unavailable\""));
    Assert.assertFalse(sanitized, sanitized.contains("internal-host.corp"));
    Assert.assertFalse(sanitized, sanitized.contains("secret.keytab"));

    String detailed = ManagementHttpServer.renderKerberos(status, true);
    Assert.assertTrue(detailed, detailed.contains("hive/internal-host.corp@EXAMPLE.COM"));
    Assert.assertTrue(detailed, detailed.contains("secret.keytab"));
  }
}
