package io.github.mmalykhin.smokestand;

import java.util.Base64;
import java.util.Map;
import org.apache.iceberg.rest.HTTPHeaders;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.ImmutableHTTPRequest;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthSession;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

/**
 * SPNEGO for the Iceberg REST client: the proxy's REST listener answers 401 Negotiate under
 * Kerberos, and Iceberg's own auth managers (none/basic/oauth2/sigv4) cannot speak it. Plugged
 * in through the standard {@code rest.auth.type} property with this class name. A fresh GSS
 * token is minted per request - SPNEGO tokens are one-shot AP-REQs, and the listener
 * authenticates every request independently (no session cookie). Credentials come from the
 * process's native ticket cache (a prior kinit), which is why the writer flips
 * {@code javax.security.auth.useSubjectCredsOnly} to {@code false} when this manager is chosen.
 */
public final class SpnegoAuthManager implements AuthManager {
  private static final Oid SPNEGO_OID = spnegoOid();

  private final AuthSession session = new AuthSession() {
    @Override
    public HTTPRequest authenticate(HTTPRequest request) {
      String host = request.baseUri().getHost();
      HTTPHeaders headers = request.headers().putIfAbsent(
          HTTPHeaders.HTTPHeader.of("Authorization", "Negotiate " + newToken(host)));
      return ImmutableHTTPRequest.builder().from(request).headers(headers).build();
    }

    @Override
    public void close() {
    }
  };

  // AuthManagers instantiates custom managers reflectively with the manager name.
  public SpnegoAuthManager(String name) {
  }

  @Override
  public AuthSession initSession(RESTClient initClient, Map<String, String> properties) {
    return session;
  }

  @Override
  public AuthSession catalogSession(RESTClient sharedClient, Map<String, String> properties) {
    return session;
  }

  @Override
  public void close() {
  }

  private static String newToken(String host) {
    try {
      GSSManager manager = GSSManager.getInstance();
      GSSName serverName = manager.createName("HTTP@" + host, GSSName.NT_HOSTBASED_SERVICE);
      GSSContext context =
          manager.createContext(serverName, SPNEGO_OID, null, GSSContext.DEFAULT_LIFETIME);
      try {
        context.requestMutualAuth(false);
        byte[] token = context.initSecContext(new byte[0], 0, 0);
        return Base64.getEncoder().encodeToString(token);
      } finally {
        context.dispose();
      }
    } catch (GSSException e) {
      throw new RuntimeException("Failed to mint a SPNEGO token for HTTP@" + host
          + " (is there a TGT in the ticket cache? run kinit first)", e);
    }
  }

  private static Oid spnegoOid() {
    try {
      return new Oid("1.3.6.1.5.5.2");
    } catch (GSSException e) {
      throw new ExceptionInInitializerError(e);
    }
  }
}
