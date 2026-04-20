package io.github.mmalykhin.hmsproxy.config.routing;

import java.util.Optional;

import io.github.mmalykhin.hmsproxy.config.operation.HmsOperationPolicy;
public final class DefaultBackendRoutingPolicy {
  private DefaultBackendRoutingPolicy() {
  }

  public static boolean routesToDefaultBackend(String methodName) {
    return policyFor(methodName).isPresent();
  }

  public static Optional<Policy> policyFor(String methodName) {
    return HmsOperationPolicy.describe(methodName).defaultBackendPolicyOptional();
  }

  public enum Policy {
    SESSION_COMPATIBILITY,
    SERVICE_READS,
    TXN_AND_LOCK_LIFECYCLE,
    NAMESPACELESS_VALIDATION
  }
}
