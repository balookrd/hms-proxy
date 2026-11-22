package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.security.ImpersonationContext;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfig;
import io.github.mmalykhin.hmsproxy.security.ClientRequestContext;
import io.github.mmalykhin.hmsproxy.security.groups.UserGroupResolver;
import io.github.mmalykhin.hmsproxy.util.PrincipalUtil;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    boolean kerberosEnabled = security != null && security.kerberosEnabled();
    try {
      if (!kerberosEnabled) {
        Optional<ImpersonationContext> connUgi = ClientRequestContext.currentTransport()
            .flatMap(ClientRequestContext::connectionUgi);
        if (connUgi.isPresent()) {
          ImpersonationContext ctx = connUgi.get();
          if (ctx.groupNames() != null && !ctx.groupNames().isEmpty()) {
            return connUgi;
          }
          List<String> groups = resolveGroups(ctx.userName());
          return Optional.of(new ImpersonationContext(ctx.userName(), groups));
        }
      }

      String rawUser = ClientRequestContext.authenticatedUser()
          .or(ClientRequestContext::remoteUser)
          .orElse(null);
      String userName = null;
      if (rawUser != null && !rawUser.isBlank()) {
        if (RoutingMetaStoreProxy.isServicePrincipal(rawUser, security)) {
          return Optional.empty();
        }
        userName = PrincipalUtil.shortUserName(rawUser);
      } else if (!kerberosEnabled) {
        try {
          UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
          if (currentUser != null) {
            if (RoutingMetaStoreProxy.isServicePrincipal(currentUser.getUserName(), security)) {
              return Optional.empty();
            }
            userName = currentUser.getShortUserName();
          }
        } catch (Exception ignored) {
          // No current user
        }
      }
      if (userName == null || userName.isBlank()) {
        return Optional.empty();
      }
      List<String> groups = resolveGroups(userName);
      return Optional.of(new ImpersonationContext(userName, groups));
    } catch (Exception e) {
      throw new MetaException("Unable to resolve authenticated caller for impersonation: " + e.getMessage());
    }
  }

  boolean isAuthenticatedServicePrincipal() {
    boolean kerberosEnabled = security != null && security.kerberosEnabled();
    if (!kerberosEnabled) {
      Optional<ImpersonationContext> connUgi = ClientRequestContext.currentTransport()
          .flatMap(ClientRequestContext::connectionUgi);
      if (connUgi.isPresent()) {
        return RoutingMetaStoreProxy.isServicePrincipal(connUgi.get().userName(), security);
      }
    }
    String rawUser = ClientRequestContext.authenticatedUser()
        .or(ClientRequestContext::remoteUser)
        .orElse(null);
    if (rawUser != null && !rawUser.isBlank()) {
      return RoutingMetaStoreProxy.isServicePrincipal(rawUser, security);
    }
    if (!kerberosEnabled) {
      try {
        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
        if (currentUser != null && currentUser.getUserName() != null) {
          return RoutingMetaStoreProxy.isServicePrincipal(currentUser.getUserName(), security);
        }
      } catch (Exception ignored) {
        return false;
      }
    }
    return false;
  }

  List<String> resolveGroups(String userName) {
    return groupResolver.resolveGroups(userName);
  }
}
