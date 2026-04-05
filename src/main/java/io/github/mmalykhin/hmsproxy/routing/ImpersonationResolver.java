package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ImpersonationResolver {
  private static final Logger LOG = LoggerFactory.getLogger(ImpersonationResolver.class);

  private final boolean anyImpersonationEnabled;
  private final ProxyConfig.SecurityConfig security;

  ImpersonationResolver(ProxyConfig config) {
    this.anyImpersonationEnabled = config.catalogs().values().stream()
        .anyMatch(ProxyConfig.CatalogConfig::impersonationEnabled);
    this.security = config.security();
  }

  Optional<ImpersonationContext> resolve() throws MetaException {
    if (!anyImpersonationEnabled) {
      return Optional.empty();
    }
    try {
      UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
      String userName = currentUser.getShortUserName();
      if (userName == null || userName.isBlank()) {
        return Optional.empty();
      }
      if (RoutingMetaStoreHandler.isServicePrincipalUser(userName, security)) {
        return Optional.empty();
      }
      return Optional.of(new ImpersonationContext(userName, resolveGroupNames(currentUser, userName)));
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
