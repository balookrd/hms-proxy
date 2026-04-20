package io.github.mmalykhin.hmsproxy.config;

import io.github.mmalykhin.hmsproxy.config.DefaultBackendRoutingPolicy.Policy;
import java.util.Optional;

public record OperationMetadata(
    String methodName,
    HmsOperationClass operationClass,
    boolean mutating,
    boolean trace,
    NamespaceStrategy namespaceStrategy,
    TableExposureMode tableExposureMode,
    ReadResultFilterKind readResultFilterKind,
    Policy defaultBackendPolicy,
    boolean safeFanout,
    boolean hdpAdapted
) {
  public Optional<Policy> defaultBackendPolicyOptional() {
    return Optional.ofNullable(defaultBackendPolicy);
  }
}
