package io.github.mmalykhin.hmsproxy.security;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class ProcessKerberosConfigurationTest {
  private static final String PRINCIPAL = "hive/proxy-host.example.com@EXAMPLE.COM";
  private static final String KEYTAB = "/etc/security/keytabs/hms-proxy.keytab";

  @Test
  public void ensureConfiguredInstallsMinimalKerberosConfigurationOnce() {
    RecordingInstaller installer = new RecordingInstaller();
    ProcessKerberosConfiguration configuration =
        new ProcessKerberosConfiguration(installer, new RecordingLogin());

    configuration.ensureConfigured("kerberos");
    configuration.ensureConfigured("kerberos");

    Assert.assertEquals(1, installer.installed.size());
    Assert.assertEquals("kerberos", installer.installed.get(0).get("hadoop.security.authentication"));
  }

  @Test
  public void ensureConfiguredNeverReplacesFrontDoorConfiguration() {
    RecordingInstaller installer = new RecordingInstaller();
    ProcessKerberosConfiguration configuration =
        new ProcessKerberosConfiguration(installer, new RecordingLogin());
    Configuration frontDoorConf = new Configuration(false);
    frontDoorConf.set("hadoop.security.authentication", "kerberos");
    frontDoorConf.set("hadoop.security.auth_to_local", "RULE:[2:$1@$0](.*@EXAMPLE.COM)s/@.*//");

    configuration.installFrontDoorConfiguration(frontDoorConf);
    configuration.ensureConfigured("kerberos");

    Assert.assertEquals(1, installer.installed.size());
    Assert.assertSame(frontDoorConf, installer.installed.get(0));
  }

  @Test
  public void ensureLoginUserFromKeytabSkipsRepeatedLoginForSamePrincipal() throws Exception {
    RecordingLogin login = new RecordingLogin();
    ProcessKerberosConfiguration configuration =
        new ProcessKerberosConfiguration(new RecordingInstaller(), login);

    configuration.ensureLoginUserFromKeytab(PRINCIPAL, KEYTAB);
    configuration.ensureLoginUserFromKeytab(PRINCIPAL, KEYTAB);

    Assert.assertEquals(List.of(PRINCIPAL), login.principals);
  }

  @Test
  public void recordedLibraryLoginSuppressesAnotherKeytabLogin() throws Exception {
    // HadoopThriftAuthBridge logs the front-door principal in itself, so later startup steps must
    // not replace the process login user underneath the running SASL server.
    RecordingLogin login = new RecordingLogin();
    ProcessKerberosConfiguration configuration =
        new ProcessKerberosConfiguration(new RecordingInstaller(), login);

    configuration.recordLoginUser(PRINCIPAL);
    configuration.ensureLoginUserFromKeytab(PRINCIPAL, KEYTAB);

    Assert.assertEquals(List.of(), login.principals);
  }

  @Test
  public void conflictingAuthenticationValueDoesNotReplaceInstalledConfiguration() {
    RecordingInstaller installer = new RecordingInstaller();
    ProcessKerberosConfiguration configuration =
        new ProcessKerberosConfiguration(installer, new RecordingLogin());

    configuration.ensureConfigured("kerberos");
    configuration.ensureConfigured("simple");

    Assert.assertEquals(1, installer.installed.size());
    Assert.assertEquals("kerberos", installer.installed.get(0).get("hadoop.security.authentication"));
  }

  @Test
  public void ensureLoginUserFromKeytabLogsInAgainForDifferentPrincipal() throws Exception {
    RecordingLogin login = new RecordingLogin();
    ProcessKerberosConfiguration configuration =
        new ProcessKerberosConfiguration(new RecordingInstaller(), login);

    configuration.ensureLoginUserFromKeytab(PRINCIPAL, KEYTAB);
    configuration.ensureLoginUserFromKeytab("hive/other-host.example.com@EXAMPLE.COM", KEYTAB);

    Assert.assertEquals(
        List.of(PRINCIPAL, "hive/other-host.example.com@EXAMPLE.COM"),
        login.principals);
  }

  private static final class RecordingInstaller implements ProcessKerberosConfiguration.ConfigurationInstaller {
    private final List<Configuration> installed = new ArrayList<>();

    @Override
    public void install(Configuration conf) {
      installed.add(conf);
    }
  }

  private static final class RecordingLogin implements ProcessKerberosConfiguration.KeytabLogin {
    private final List<String> principals = new ArrayList<>();

    @Override
    public void login(String principal, String keytab) {
      principals.add(principal);
    }
  }
}
