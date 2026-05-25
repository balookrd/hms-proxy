package io.github.mmalykhin.hmsproxy.restcatalog;

import io.github.mmalykhin.hmsproxy.config.restcatalog.RestCatalogConfig;
import io.github.mmalykhin.hmsproxy.security.KerberosPrincipalUtil;
import java.io.IOException;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Logs in the SPNEGO service principal (HTTP/&lt;host&gt;@REALM) from its keytab
 * without overwriting the global Hadoop login user, which is owned by
 * FrontDoorSecurity for the Thrift listener. Returns a UGI usable as the
 * Subject for {@code GSSContext.acceptSecContext()}.
 */
final class RestKerberosBootstrap {
  private RestKerberosBootstrap() {
  }

  static UserGroupInformation login(RestCatalogConfig config) throws IOException {
    if (!config.kerberosEnabled()) {
      throw new IllegalStateException(
          "RestKerberosBootstrap.login called with Kerberos disabled in rest-catalog config");
    }
    if (!UserGroupInformation.isSecurityEnabled()) {
      throw new IllegalStateException(
          "Iceberg REST SPNEGO requires Hadoop security to be enabled. Set security.mode=KERBEROS "
              + "so FrontDoorSecurity installs a Kerberos UGI configuration before the REST listener starts.");
    }
    String principal = KerberosPrincipalUtil.resolveForLocalHost(config.kerberosPrincipal());
    return UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, config.kerberosKeytab());
  }
}
