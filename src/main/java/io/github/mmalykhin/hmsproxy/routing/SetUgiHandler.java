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
    if (!support.router.defaultBackend().impersonationEnabled()) {
      return fallback.invokeGlobal(method, args);
    }
    ImpersonationContext impersonation = support.impersonationResolver.resolve().orElseThrow(() ->
        new MetaException("Kerberos caller identity is unavailable for impersonation"));
    if (args != null && args.length > 0 && args[0] instanceof String requestedUser
        && !requestedUser.isBlank()
        && !requestedUser.equals(impersonation.userName())) {
      LOG.warn("requestId={} ignoring client-requested set_ugi user '{}' and using authenticated user '{}'",
          RequestContext.currentRequestId(), requestedUser, impersonation.userName());
    }
    return fallback.invokeGlobal(method, new Object[]{impersonation.userName(), impersonation.groupNames()});
  }
}
