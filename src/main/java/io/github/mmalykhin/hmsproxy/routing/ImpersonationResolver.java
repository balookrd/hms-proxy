package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfig;

final class ImpersonationResolver {
  private static final Logger LOG = LoggerFactory.getLogger(ImpersonationResolver.class);

  private final boolean anyImpersonationEnabled;
  private final SecurityConfig security;

  ImpersonationResolver(ProxyConfig config) {
    this.anyImpersonationEnabled = config.ranger().enabled()
        || config.catalogs().values().stream().anyMatch(c -> c.impersonationEnabled() || c.ranger().enabled());
    this.security = config.security();
  }

  Optional<ImpersonationContext> resolve() throws MetaException {
    if (!anyImpersonationEnabled) {
      return Optional.empty();
    }
    try {
      String remoteUser = io.github.mmalykhin.hmsproxy.security.ClientRequestContext.remoteUser().orElse(null);
      UserGroupInformation currentUser = null;
      String userName = null;
      if (remoteUser != null && !remoteUser.isBlank()) {
        userName = io.github.mmalykhin.hmsproxy.util.PrincipalUtil.shortUserName(remoteUser);
      }
      if (userName == null || userName.isBlank()) {
        currentUser = UserGroupInformation.getCurrentUser();
        userName = currentUser != null ? currentUser.getShortUserName() : null;
      }
      if (userName == null || userName.isBlank()) {
        return Optional.empty();
      }
      if (RoutingMetaStoreProxy.isServicePrincipalUser(userName, security)) {
        return Optional.empty();
      }
      List<String> groups = currentUser != null ? resolveGroupNames(currentUser, userName) : List.of();
      return Optional.of(new ImpersonationContext(userName, groups));
    } catch (Exception e) {
      throw new MetaException("Unable to resolve authenticated caller for impersonation: " + e.getMessage());
    }
  }

  private List<String> resolveGroupNames(UserGroupInformation currentUser, String userName) {
    try {
      return List.of(currentUser.getGroupNames());
    } catch (RuntimeException e) {
      LOG.warn("requestId={} unable to resolve groups for authenticated user '{}', using empty group list",
          RequestContext.currentRequestId(), userName, e);
      return List.of();
    }
  }
}
