package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.security.ImpersonationContext;
import io.github.mmalykhin.hmsproxy.security.ClientRequestContext;
import io.github.mmalykhin.hmsproxy.util.PrincipalUtil;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class SetUgiHandler implements SpecialCaseHandler {
  private static final Logger LOG = LoggerFactory.getLogger(SetUgiHandler.class);

  private final RoutingSupport support;
  private final NamespaceFallback fallback;

  SetUgiHandler(RoutingSupport support, NamespaceFallback fallback) {
    this.support = support;
    this.fallback = fallback;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    List<String> requestedGroups = new ArrayList<>();
    String requestedUser = null;
    if (args != null && args.length > 0 && args[0] instanceof String u && !u.isBlank()) {
      requestedUser = u;
      if (args.length > 1 && args[1] instanceof List<?> rawGroups) {
        for (Object g : rawGroups) {
          if (g != null) {
            requestedGroups.add(g.toString());
          }
        }
      }
    }

    boolean kerberosEnabled = support != null
        && support.config != null
        && support.config.security() != null
        && support.config.security().kerberosEnabled();
    if (kerberosEnabled) {
      // Classic Kerberos HMS never switches connection UGI or trusts client-supplied groups on set_ugi.
      String authenticatedUser = ClientRequestContext.authenticatedUser()
          .or(ClientRequestContext::remoteUser)
          .orElse(null);
      String authShortUser = PrincipalUtil.shortUserName(authenticatedUser);
      String requestedShortUser = PrincipalUtil.shortUserName(requestedUser);
      boolean mismatchedUser = requestedUser != null
          && (authShortUser == null || !authShortUser.equals(requestedShortUser));
      if (mismatchedUser || !requestedGroups.isEmpty()) {
        LOG.warn(
            "requestId={} ignoring set_ugi override under Kerberos: authenticated='{}', requestedUser='{}', requestedGroups={}",
            RequestContext.currentRequestId(), authenticatedUser, requestedUser, requestedGroups);
      }
      return authShortUser != null ? new ArrayList<>(support.resolveGroups(authShortUser)) : new ArrayList<>();
    }

    if (requestedUser != null) {
      if (requestedGroups.isEmpty()) {
        requestedGroups.addAll(support.resolveGroups(requestedUser));
      }
      ImpersonationContext impersonation = new ImpersonationContext(requestedUser, requestedGroups);
      ClientRequestContext.currentTransport()
          .ifPresent(t -> ClientRequestContext.setConnectionUgi(t, impersonation));
      LOG.info("requestId={} connection set_ugi user '{}' with groups {}",
          RequestContext.currentRequestId(), requestedUser, requestedGroups);
    }

    return requestedGroups;
  }
}
