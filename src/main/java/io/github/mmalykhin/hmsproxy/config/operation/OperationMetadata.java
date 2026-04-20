package io.github.mmalykhin.hmsproxy.config.operation;

import io.github.mmalykhin.hmsproxy.config.routing.DefaultBackendRoutingPolicy.Policy;
import java.util.Optional;

import io.github.mmalykhin.hmsproxy.config.catalog.NamespaceStrategy;
import io.github.mmalykhin.hmsproxy.config.catalog.ReadResultFilterKind;
import io.github.mmalykhin.hmsproxy.config.catalog.TableExposureMode;
import io.github.mmalykhin.hmsproxy.config.routing.DefaultBackendRoutingPolicy;
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
