package io.github.mmalykhin.hmsproxy.config;

import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Which settings a configuration reload actually applies to the running proxy.
 *
 * <p>A reload rebuilds the catalog router, the federation layer and the routing support, so only
 * the settings those read per request take effect. Everything else - backends and their pools,
 * security, Ranger, rate limits, latency routing, listeners, REST - is wired once at startup. The
 * lists below name what is applied; any other component, including one added later, is reported
 * as needing a restart rather than silently assumed live.
 */
public final class ConfigReloadScope {
  private static final Set<String> APPLIED_TOP_LEVEL = Set.of("catalogs", "federation");
  private static final Set<String> APPLIED_CATALOG_FIELDS = Set.of(
      "accessMode",
      "writeDbWhitelist",
      "exposeMode",
      "exposeDbPatterns",
      "exposeTablePatterns",
      "unprefixedDatabases",
      "mergeWithDefault");
  // externalTableDropPurgeMode is read by the drop purger, which is built once at startup.
  private static final Set<String> APPLIED_FEDERATION_FIELDS = Set.of(
      "preserveBackendCatalogName",
      "viewTextRewriteMode",
      "preserveOriginalViewText",
      "externalTableLocationRewriteMode",
      "externalTableLocationRewriteSourceDefaultFs");

  private ConfigReloadScope() {
  }

  /**
   * Settings that differ between the configuration the process started with and {@code next} but
   * that a reload does not apply, as config component paths (for example {@code catalog.hdp.hiveConf}).
   */
  public static List<String> changesRequiringRestart(ProxyConfig running, ProxyConfig next) {
    List<String> changes = new ArrayList<>();
    collectDifferences("", running, next, APPLIED_TOP_LEVEL, changes);
    collectDifferences("federation.", running.federation(), next.federation(), APPLIED_FEDERATION_FIELDS, changes);
    Set<String> catalogNames = new TreeSet<>(running.catalogs().keySet());
    catalogNames.addAll(next.catalogs().keySet());
    for (String name : catalogNames) {
      if (!running.catalogs().containsKey(name)) {
        changes.add("catalogs (catalog '" + name + "' added)");
      } else if (!next.catalogs().containsKey(name)) {
        changes.add("catalogs (catalog '" + name + "' removed)");
      } else {
        collectDifferences("catalog." + name + ".", running.catalogs().get(name), next.catalogs().get(name),
            APPLIED_CATALOG_FIELDS, changes);
      }
    }
    return changes;
  }

  private static void collectDifferences(
      String prefix,
      Record running,
      Record next,
      Set<String> applied,
      List<String> changes
  ) {
    for (RecordComponent component : running.getClass().getRecordComponents()) {
      if (applied.contains(component.getName())) {
        continue;
      }
      try {
        Object before = component.getAccessor().invoke(running);
        Object after = component.getAccessor().invoke(next);
        if (!Objects.equals(before, after)) {
          changes.add(prefix + component.getName());
        }
      } catch (ReflectiveOperationException e) {
        changes.add(prefix + component.getName());
      }
    }
  }
}
