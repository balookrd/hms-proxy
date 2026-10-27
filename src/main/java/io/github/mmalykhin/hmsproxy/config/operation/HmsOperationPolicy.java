package io.github.mmalykhin.hmsproxy.config.operation;

import io.github.mmalykhin.hmsproxy.config.catalog.NamespaceStrategy;
import io.github.mmalykhin.hmsproxy.config.catalog.ReadResultFilterKind;
import io.github.mmalykhin.hmsproxy.config.catalog.TableExposureMode;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class HmsOperationPolicy {
  private static final Map<String, OperationMetadata> REGISTRY = buildRegistry();
  private static final ConcurrentMap<String, OperationMetadata> DERIVED_CACHE = new ConcurrentHashMap<>();
  // Not flagged mutating, yet each call issues, renews or revokes a credential or restarts the
  // server, so a second copy is a second side effect.
  private static final Set<String> NON_REPLAYABLE_NON_MUTATING = Set.of(
      "get_delegation_token",
      "renew_delegation_token",
      "cancel_delegation_token",
      "shutdown",
      "reinitialize");

  private HmsOperationPolicy() {
  }

  /**
   * Whether a call whose reply was lost may be sent again on a fresh backend session. The backend
   * may already have applied the first copy, so only calls that change no state qualify. A method
   * outside the registry qualifies only when its name reads as a query: an unknown name is treated
   * as a write rather than replayed on a guess.
   */
  public static boolean replayableAfterLostReply(String methodName) {
    String normalized = HmsMethodNameHeuristics.normalizeMethod(methodName);
    if (NON_REPLAYABLE_NON_MUTATING.contains(normalized)) {
      return false;
    }
    OperationMetadata registered = REGISTRY.get(normalized);
    if (registered != null) {
      return !registered.mutating();
    }
    return HmsMethodNameHeuristics.deriveOperationClass(normalized) == HmsOperationClass.METADATA_READ;
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
