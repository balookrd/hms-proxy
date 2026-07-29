package io.github.mmalykhin.hmsproxy.restcatalog;

import com.sun.net.httpserver.Authenticator;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpPrincipal;
import io.github.mmalykhin.hmsproxy.security.KerberosPrincipalUtil;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;
import java.util.Objects;
import org.apache.hadoop.security.UserGroupInformation;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SPNEGO Authenticator backed by JDK GSSAPI. Validates the inbound
 * Authorization: Negotiate header against credentials acquired from the REST
 * service keytab. On success the authenticated principal is exposed via
 * {@link HttpExchange#getPrincipal()} so the downstream handler can stamp
 * ClientRequestContext.
 *
 * Thread safety: serverCredentials is acquired once and reused. GSSContext is
 * not thread-safe but a new one is created per request.
 */
final class SpnegoAuthenticator extends Authenticator {
  private static final Logger LOG = LoggerFactory.getLogger(SpnegoAuthenticator.class);
  private static final String NEGOTIATE = "Negotiate";
  private static final Oid SPNEGO_OID = oid("1.3.6.1.5.5.2");
  private static final Oid KERBEROS_OID = oid("1.2.840.113554.1.2.2");

  private final UserGroupInformation ugi;
  private final String realm;
  private final GSSCredential serverCredentials;

  SpnegoAuthenticator(UserGroupInformation ugi, String principalName) throws Exception {
    this.ugi = Objects.requireNonNull(ugi, "ugi");
    String resolved = KerberosPrincipalUtil.resolveForLocalHost(
        Objects.requireNonNull(principalName, "principalName"));
    this.realm = extractRealm(resolved);
    this.serverCredentials = acquireServerCredentials(ugi, resolved);
  }

  @Override
  public Result authenticate(HttpExchange exchange) {
    String header = exchange.getRequestHeaders().getFirst("Authorization");
    if (header == null || !header.regionMatches(true, 0, NEGOTIATE, 0, NEGOTIATE.length())) {
      exchange.getResponseHeaders().set("WWW-Authenticate", NEGOTIATE);
      return new Retry(401);
    }
    String tokenPart = header.length() > NEGOTIATE.length()
        ? header.substring(NEGOTIATE.length()).trim()
        : "";
    if (tokenPart.isEmpty()) {
      exchange.getResponseHeaders().set("WWW-Authenticate", NEGOTIATE);
      return new Retry(401);
    }

    byte[] token;
    try {
      token = Base64.getDecoder().decode(tokenPart);
    } catch (IllegalArgumentException e) {
      LOG.debug("Malformed SPNEGO token (not base64)", e);
      return new Failure(400);
    }

    try {
      return ugi.doAs((PrivilegedExceptionAction<Result>) () -> acceptToken(exchange, token));
    } catch (Exception e) {
      LOG.warn("SPNEGO authentication failed: {}", e.getMessage());
      return new Failure(401);
    }
  }

  private Result acceptToken(HttpExchange exchange, byte[] token) throws Exception {
    GSSManager manager = GSSManager.getInstance();
    GSSContext context = manager.createContext(serverCredentials);
    try {
      byte[] outToken = context.acceptSecContext(token, 0, token.length);
      if (outToken != null && outToken.length > 0) {
        exchange.getResponseHeaders().set(
            "WWW-Authenticate",
            NEGOTIATE + " " + Base64.getEncoder().encodeToString(outToken));
      }
      if (!context.isEstablished()) {
        // Multi-leg SPNEGO is rare; treat as auth-incomplete.
        return new Retry(401);
      }
      String clientName = context.getSrcName().toString();
      return new Success(new HttpPrincipal(clientName, realm));
    } finally {
      try {
        context.dispose();
      } catch (Exception ignored) {
        // best effort
      }
    }
  }

  private static GSSCredential acquireServerCredentials(UserGroupInformation ugi, String principal) throws Exception {
    return ugi.doAs((PrivilegedExceptionAction<GSSCredential>) () -> {
      GSSManager manager = GSSManager.getInstance();
      GSSName serverName = manager.createName(principal, GSSName.NT_USER_NAME, KERBEROS_OID);
      return manager.createCredential(
          serverName,
          GSSCredential.INDEFINITE_LIFETIME,
          new Oid[]{SPNEGO_OID, KERBEROS_OID},
          GSSCredential.ACCEPT_ONLY);
    });
  }

  private static String extractRealm(String principal) {
    int at = principal.lastIndexOf('@');
    return at >= 0 ? principal.substring(at + 1) : "";
  }

  private static Oid oid(String value) {
    try {
      return new Oid(value);
    } catch (Exception e) {
      throw new IllegalStateException("Cannot construct OID " + value, e);
    }
  }
}
