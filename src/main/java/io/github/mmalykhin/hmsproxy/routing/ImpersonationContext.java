package io.github.mmalykhin.hmsproxy.routing;

import java.util.List;

/**
 * Carries the authenticated caller identity used for Kerberos impersonation.
 * Resolved once per request in {@link RoutingMetaStoreHandler} and threaded through
 * to backend invocations.
 */
public record ImpersonationContext(String userName, List<String> groupNames) {
}
