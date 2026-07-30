package io.github.mmalykhin.hmsproxy.restcatalog;

import io.github.mmalykhin.hmsproxy.config.restcatalog.RestCatalogPurgeMode;
import io.github.mmalykhin.hmsproxy.util.PathPrefixAllowlist;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;

/**
 * The single place deciding what {@code DELETE ...?purgeRequested=true} may destroy. Kept apart
 * from {@link WriteRouteGate}, which answers the different question of which catalog a namespace
 * belongs to: the gate keeps a purge inside the default catalog, this policy bounds which paths
 * inside it may be deleted. Do not grow a second purge check anywhere else.
 *
 * <p>Two lines of enforcement, because neither is sufficient alone. The pre-flight check
 * ({@link #refusalFor}) answers loudly, before anything is dropped, for the ordinary case of a
 * table living in the wrong tree. The FileIO guard ({@link #guard}) covers what pre-flight cannot
 * see: the data and manifest paths only discovered while walking manifests the client wrote.
 */
final class IcebergPurgePolicy {
  private final RestCatalogPurgeMode mode;
  private final List<String> allowedPrefixes;

  IcebergPurgePolicy(RestCatalogPurgeMode mode, List<String> allowedPrefixes) {
    this.mode = Objects.requireNonNull(mode, "mode");
    this.allowedPrefixes = List.copyOf(allowedPrefixes);
  }

  boolean isDefaultBehaviour() {
    return mode == RestCatalogPurgeMode.ALLOW;
  }

  /** Null when the purge may proceed; otherwise the message the client gets with the 403. */
  String refusalFor(String tableName, TableMetadata metadata, Configuration conf) {
    if (mode == RestCatalogPurgeMode.ALLOW) {
      return null;
    }
    if (mode == RestCatalogPurgeMode.REFUSE) {
      return "Purge is disabled on this proxy (rest-catalog.purge.mode=REFUSE); table '"
          + tableName + "' was not dropped. Retry without purgeRequested to drop it and keep its"
          + " files.";
    }
    if (metadata == null) {
      // No metadata means nothing to walk and nothing to delete; the drop itself is not a purge.
      return null;
    }
    List<String> qualifiedPrefixes = PurgePathQualifier.qualifyPrefixes(allowedPrefixes, conf);
    String outside = firstPathOutside(
        qualifiedPrefixes, conf, metadata.location(), metadata.metadataFileLocation());
    if (outside == null) {
      return null;
    }
    return "Purge is restricted to rest-catalog.purge.allowed-prefixes; table '" + tableName
        + "' was not dropped because '" + outside + "' lies outside them.";
  }

  /** The delegate itself in ALLOW, so the default path keeps today's FileIO untouched. */
  FileIO guard(FileIO io, Configuration conf, String tableName) {
    if (mode != RestCatalogPurgeMode.ALLOWLIST) {
      return io;
    }
    return new PrefixGuardedFileIO(
        io, PurgePathQualifier.qualifyPrefixes(allowedPrefixes, conf), conf, tableName);
  }

  private static String firstPathOutside(
      List<String> qualifiedPrefixes, Configuration conf, String... locations) {
    for (String location : locations) {
      if (location == null) {
        continue;
      }
      String qualified;
      try {
        qualified = PurgePathQualifier.qualify(location, conf);
      } catch (IOException | IllegalArgumentException e) {
        // Fail closed, the same way the FileIO guard does: a location that cannot be qualified
        // cannot be shown to be inside the allowlist.
        return location;
      }
      if (!PathPrefixAllowlist.matches(qualified, qualifiedPrefixes)) {
        return qualified;
      }
    }
    return null;
  }
}
