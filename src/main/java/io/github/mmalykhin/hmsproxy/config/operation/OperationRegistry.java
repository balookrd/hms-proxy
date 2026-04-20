package io.github.mmalykhin.hmsproxy.config.operation;

import io.github.mmalykhin.hmsproxy.config.catalog.NamespaceStrategy;
import io.github.mmalykhin.hmsproxy.config.catalog.ReadResultFilterKind;
import io.github.mmalykhin.hmsproxy.config.catalog.TableExposureMode;
import io.github.mmalykhin.hmsproxy.config.routing.DefaultBackendRoutingPolicy.Policy;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

final class OperationRegistry {
  private final Map<String, OperationMetadata> entries = new LinkedHashMap<>();

  void op(String method, Consumer<OpBuilder> configurator) {
    OpBuilder builder = new OpBuilder(method);
    configurator.accept(builder);
    if (entries.put(method, builder.build()) != null) {
      throw new IllegalStateException("Duplicate operation registered: " + method);
    }
  }

  void all(Consumer<OpBuilder> configurator, String... methods) {
    for (String method : methods) {
      op(method, configurator);
    }
  }

  Map<String, OperationMetadata> freeze() {
    return Map.copyOf(entries);
  }

  static final class OpBuilder {
    private final String method;
    private HmsOperationClass operationClass;
    private Boolean mutatingOverride;
    private NamespaceStrategy namespaceStrategy;
    private TableExposureMode tableExposureMode = TableExposureMode.NONE;
    private ReadResultFilterKind readResultFilterKind = ReadResultFilterKind.NONE;
    private Policy defaultBackendPolicy;
    private boolean safeFanout;
    private boolean hdpAdapted;
    private boolean trace;

    OpBuilder(String method) {
      this.method = method;
    }

    OpBuilder cls(HmsOperationClass value) {
      this.operationClass = value;
      return this;
    }

    OpBuilder ns(NamespaceStrategy value) {
      this.namespaceStrategy = value;
      return this;
    }

    OpBuilder expose(TableExposureMode value) {
      this.tableExposureMode = value;
      return this;
    }

    OpBuilder filter(ReadResultFilterKind value) {
      this.readResultFilterKind = value;
      return this;
    }

    OpBuilder backend(Policy value) {
      this.defaultBackendPolicy = value;
      return this;
    }

    OpBuilder mutating() {
      this.mutatingOverride = Boolean.TRUE;
      return this;
    }

    OpBuilder nonMutating() {
      this.mutatingOverride = Boolean.FALSE;
      return this;
    }

    OpBuilder trace() {
      this.trace = true;
      return this;
    }

    OpBuilder safeFanout() {
      this.safeFanout = true;
      return this;
    }

    OpBuilder hdp() {
      this.hdpAdapted = true;
      return this;
    }

    OperationMetadata build() {
      HmsOperationClass resolvedClass = operationClass != null
          ? operationClass
          : HmsMethodNameHeuristics.deriveOperationClass(method);
      boolean resolvedMutating = mutatingOverride != null
          ? mutatingOverride
          : HmsMethodNameHeuristics.deriveMutation(method);
      NamespaceStrategy resolvedNamespace = namespaceStrategy != null
          ? namespaceStrategy
          : HmsMethodNameHeuristics.deriveNamespaceStrategy(resolvedClass, defaultBackendPolicy);
      return new OperationMetadata(
          method,
          resolvedClass,
          resolvedMutating,
          trace,
          resolvedNamespace,
          tableExposureMode,
          readResultFilterKind,
          defaultBackendPolicy,
          safeFanout,
          hdpAdapted);
    }
  }
}
