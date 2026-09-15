package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import java.lang.reflect.Method;
import org.apache.hadoop.hive.metastore.api.MetaException;
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
    java.util.List<String> groups = new java.util.ArrayList<>();
    String requestedUser = null;
    if (args != null && args.length > 0 && args[0] instanceof String u && !u.isBlank()) {
      requestedUser = u;
      if (args.length > 1 && args[1] instanceof java.util.List<?> requestedGroups) {
        for (Object g : requestedGroups) {
          if (g != null) {
            groups.add(g.toString());
          }
        }
      }
    }

    if (requestedUser != null) {
      ImpersonationContext impersonation = new ImpersonationContext(requestedUser, groups);
      io.github.mmalykhin.hmsproxy.security.ClientRequestContext.currentTransport()
          .ifPresent(t -> io.github.mmalykhin.hmsproxy.security.ClientRequestContext.setConnectionUgi(t, impersonation));
      io.github.mmalykhin.hmsproxy.security.ClientRequestContext.setRemoteUser(requestedUser);
      LOG.info("requestId={} connection set_ugi user '{}' with groups {}",
          RequestContext.currentRequestId(), requestedUser, groups);
    }

    if (support.router == null || support.router.defaultBackend() == null || !support.router.defaultBackend().impersonationEnabled()) {
      return groups;
    }

    ImpersonationContext impersonation = support.impersonationResolver.resolve().orElseThrow(() ->
        new MetaException("Caller identity is unavailable for impersonation"));
    return fallback.invokeGlobal(method, new Object[]{impersonation.userName(), impersonation.groupNames()});
  }
}
