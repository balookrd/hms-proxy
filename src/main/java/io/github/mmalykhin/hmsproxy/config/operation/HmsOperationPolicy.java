package io.github.mmalykhin.hmsproxy.config.operation;

import io.github.mmalykhin.hmsproxy.config.catalog.NamespaceStrategy;
import io.github.mmalykhin.hmsproxy.config.catalog.ReadResultFilterKind;
import io.github.mmalykhin.hmsproxy.config.catalog.TableExposureMode;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class HmsOperationPolicy {
  private static final Map<String, OperationMetadata> REGISTRY = buildRegistry();
  private static final ConcurrentMap<String, OperationMetadata> DERIVED_CACHE = new ConcurrentHashMap<>();

  private HmsOperationPolicy() {
  }

  public static OperationMetadata describe(String methodName) {
    String normalized = HmsMethodNameHeuristics.normalizeMethod(methodName);
    OperationMetadata registered = REGISTRY.get(normalized);
    if (registered != null) {
      return registered;
    }
    return DERIVED_CACHE.computeIfAbsent(normalized, HmsOperationPolicy::deriveOnly);
  }

  private static OperationMetadata deriveOnly(String methodName) {
    HmsOperationClass operationClass = HmsMethodNameHeuristics.deriveOperationClass(methodName);
    boolean mutating = HmsMethodNameHeuristics.deriveMutation(methodName);
    NamespaceStrategy namespaceStrategy =
        HmsMethodNameHeuristics.deriveNamespaceStrategy(operationClass, null);
    return new OperationMetadata(
        methodName,
        operationClass,
        mutating,
        false,
        namespaceStrategy,
        TableExposureMode.NONE,
        ReadResultFilterKind.NONE,
        null,
        false,
        false);
  }

  private static Map<String, OperationMetadata> buildRegistry() {
    OperationRegistry r = new OperationRegistry();
    AdminIntrospectionOps.contribute(r);
    ServiceGlobalOps.contribute(r);
    CompatibilityOnlyOps.contribute(r);
    AcidOps.contribute(r);
    DatabaseMetadataOps.contribute(r);
    TableMetadataOps.contribute(r);
    PartitionOps.contribute(r);
    FunctionOps.contribute(r);
    SafeFanoutOps.contribute(r);
    HdpAdaptedOps.contribute(r);
    TraceOnlyWriteOps.contribute(r);
    return r.freeze();
  }
}
