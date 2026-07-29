package io.github.mmalykhin.hmsproxy.restcatalog;

import java.util.List;
import java.util.Objects;

/**
 * Maps database names between a non-default catalog's internal view ("default")
 * and the proxy's external federated view ("apache__default"). The REST layer
 * shows internal names; the proxy keeps seeing external ones, so federation,
 * exposure rules and access modes stay untouched.
 */
final class CatalogNameTranslation {
  private final String externalPrefix;

  CatalogNameTranslation(String catalogName, String separator) {
    this.externalPrefix = Objects.requireNonNull(catalogName, "catalogName")
        + Objects.requireNonNull(separator, "separator");
  }

  String toExternal(String internalDb) {
    return externalPrefix + internalDb;
  }

  String fromExternalOrNull(String externalDb) {
    if (externalDb == null || !externalDb.startsWith(externalPrefix)) {
      return null;
    }
    String internal = externalDb.substring(externalPrefix.length());
    return internal.isEmpty() ? null : internal;
  }

  List<String> internalNames(List<String> externalDbs) {
    return externalDbs.stream()
        .map(this::fromExternalOrNull)
        .filter(Objects::nonNull)
        .toList();
  }
}
