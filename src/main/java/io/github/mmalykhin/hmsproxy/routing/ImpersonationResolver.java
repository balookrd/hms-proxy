package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfig;

import io.github.mmalykhin.hmsproxy.security.groups.UserGroupResolver;

final class ImpersonationResolver {
  private static final Logger LOG = LoggerFactory.getLogger(ImpersonationResolver.class);

  private final boolean anyImpersonationEnabled;
  private final SecurityConfig security;
  private final UserGroupResolver groupResolver;

  ImpersonationResolver(ProxyConfig config) {
    this(config, new UserGroupResolver());
  }

  ImpersonationResolver(ProxyConfig config, UserGroupResolver groupResolver) {
    this.anyImpersonationEnabled = config.ranger().enabled()
        || config.catalogs().values().stream().anyMatch(c -> c.impersonationEnabled() || c.ranger().enabled());
    this.security = config.security();
    this.groupResolver = groupResolver != null ? groupResolver : new UserGroupResolver();
  }

  UserGroupResolver groupResolver() {
    return groupResolver;
  }

  Optional<ImpersonationContext> resolve() throws MetaException {
    if (!anyImpersonationEnabled) {
      return Optional.empty();
    }
    try {
      Optional<ImpersonationContext> connUgi = io.github.mmalykhin.hmsproxy.security.ClientRequestContext.currentTransport()
          .flatMap(io.github.mmalykhin.hmsproxy.security.ClientRequestContext::connectionUgi);
      if (connUgi.isPresent()) {
        ImpersonationContext ctx = connUgi.get();
        if (ctx.groupNames() != null && !ctx.groupNames().isEmpty()) {
          return connUgi;
        }
        List<String> groups = resolveGroups(ctx.userName());
        return Optional.of(new ImpersonationContext(ctx.userName(), groups));
      }

      String remoteUser = io.github.mmalykhin.hmsproxy.security.ClientRequestContext.remoteUser().orElse(null);
      String userName = null;
      if (remoteUser != null && !remoteUser.isBlank()) {
        userName = io.github.mmalykhin.hmsproxy.util.PrincipalUtil.shortUserName(remoteUser);
      }
      if (userName == null || userName.isBlank()) {
        try {
          UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
          userName = currentUser != null ? currentUser.getShortUserName() : null;
        } catch (Exception ignored) {
          // No current user
        }
      }
      if (userName == null || userName.isBlank()) {
        return Optional.empty();
      }
      if (RoutingMetaStoreProxy.isServicePrincipalUser(userName, security)) {
        return Optional.empty();
      }
      List<String> groups = resolveGroups(userName);
      return Optional.of(new ImpersonationContext(userName, groups));
    } catch (Exception e) {
      throw new MetaException("Unable to resolve authenticated caller for impersonation: " + e.getMessage());
    }
  }

  List<String> resolveGroups(String userName) {
    return groupResolver.resolveGroups(userName);
  }
}
