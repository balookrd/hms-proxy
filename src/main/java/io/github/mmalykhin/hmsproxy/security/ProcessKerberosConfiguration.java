package io.github.mmalykhin.hmsproxy.security;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single owner of the process-wide {@link UserGroupInformation} security configuration.
 *
 * <p>{@code UserGroupInformation.setConfiguration} is a process-wide static that also reinitializes
 * {@code HadoopKerberosName} auth_to_local rules, so calling it after startup silently replaces the
 * live Kerberos configuration and races with in-flight SASL handshakes and TGT relogins. Every
 * component must therefore route through this class instead of calling the static directly: the
 * front door installs the full configuration once at startup, and everything else only fills in a
 * minimal configuration when no configuration has been installed yet.
 */
public final class ProcessKerberosConfiguration {
  private static final Logger LOG = LoggerFactory.getLogger(ProcessKerberosConfiguration.class);
  private static final String AUTHENTICATION_KEY = "hadoop.security.authentication";

  private static final ProcessKerberosConfiguration PROCESS_WIDE = new ProcessKerberosConfiguration(
      UserGroupInformation::setConfiguration,
      UserGroupInformation::loginUserFromKeytab);

  @FunctionalInterface
  interface ConfigurationInstaller {
    void install(Configuration conf);
  }

  @FunctionalInterface
  interface KeytabLogin {
    void login(String principal, String keytab) throws IOException;
  }

  private final ConfigurationInstaller installer;
  private final KeytabLogin keytabLogin;
  private volatile String configuredAuthValue;
  private volatile String loginPrincipal;

  ProcessKerberosConfiguration(ConfigurationInstaller installer, KeytabLogin keytabLogin) {
    this.installer = installer;
    this.keytabLogin = keytabLogin;
  }

  public static ProcessKerberosConfiguration processWide() {
    return PROCESS_WIDE;
  }

  /** Installs the authoritative front-door security configuration. Only called during startup. */
  public synchronized void installFrontDoorConfiguration(Configuration conf) {
    installer.install(conf);
    configuredAuthValue = conf.get(AUTHENTICATION_KEY);
  }

  /**
   * Installs a minimal Hadoop security configuration when nothing installed one yet, for example
   * when the front door runs without Kerberos but backend SASL is enabled.
   */
  public synchronized void ensureConfigured(String hadoopAuthValue) {
    if (configuredAuthValue != null) {
      if (!configuredAuthValue.equals(hadoopAuthValue)) {
        LOG.warn("Keeping the installed process-wide {}={} and ignoring the requested value {}",
            AUTHENTICATION_KEY, configuredAuthValue, hadoopAuthValue);
      }
      return;
    }
    Configuration conf = new Configuration(false);
    conf.set(AUTHENTICATION_KEY, hadoopAuthValue);
    installer.install(conf);
    configuredAuthValue = hadoopAuthValue;
    LOG.info("Installed process-wide Hadoop security configuration with {}={}",
        AUTHENTICATION_KEY, hadoopAuthValue);
  }

  /** Performs a keytab login for the process login user unless the same principal is already logged in. */
  public synchronized void ensureLoginUserFromKeytab(String principal, String keytab) throws IOException {
    if (principal != null && principal.equals(loginPrincipal)) {
      LOG.debug("Hadoop login user is already {}, skipping repeated keytab login", principal);
      return;
    }
    keytabLogin.login(principal, keytab);
    loginPrincipal = principal;
    LOG.info("Refreshed Hadoop login user from keytab {} using principal {}", keytab, principal);
  }

  /**
   * Records a keytab login performed by a library, such as the Hive thrift auth bridge, so later
   * startup steps do not replace the process login user underneath a running SASL server.
   */
  public synchronized void recordLoginUser(String principal) {
    loginPrincipal = principal;
  }
}
