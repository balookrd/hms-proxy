package io.github.mmalykhin.hmsproxy.restcatalog;

import io.github.mmalykhin.hmsproxy.config.restcatalog.RestCatalogPurgeMode;
import io.github.mmalykhin.hmsproxy.util.PathPrefixAllowlist;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.view.ViewMetadata;

/**
 * The single place deciding what {@code DELETE ...?purgeRequested=true} and runtime {@link FileIO}
 * deletions may destroy. Kept apart from {@link WriteRouteGate}, which answers the different
 * question of which catalog a namespace belongs to: the gate keeps a purge inside the default
 * catalog, this policy bounds which paths inside it may be deleted. Do not grow a second purge
 * check anywhere else.
 *
 * <p>Three lines of enforcement: pre-flight {@link #refusalFor} / {@link #refusalForView} before any
 * metastore drop, runtime {@link GuardedFileIO} enforcement via {@link #authorizeRuntimeDelete} on
 * every catalog delete path (including {@code write.metadata.delete-after-commit.enabled}), and
 * {@link PrefixGuardedFileIO} during {@code dropTable(..., purge=true)} manifest walks.
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

  RestCatalogPurgeMode mode() {
    return mode;
  }

  /** Null when the purge may proceed; otherwise the message the client gets with the 403. */
  String refusalFor(String tableName, TableMetadata metadata, Configuration conf) {
    return refusalFor(tableName, metadata, new RestFileIoPathGuard(conf, List.of()));
  }

  String refusalFor(String tableName, TableMetadata metadata, RestFileIoPathGuard guard) {
    return refusalForObject(
        "table",
        tableName,
        metadata == null ? null : metadata.location(),
        metadata == null ? null : metadata.metadataFileLocation(),
        guard,
        "Retry without purgeRequested to drop it and keep its files.");
  }

  String refusalForView(String viewName, ViewMetadata metadata, RestFileIoPathGuard guard) {
    return refusalForObject(
        "view",
        viewName,
        metadata == null ? null : metadata.location(),
        metadata == null ? null : metadata.metadataFileLocation(),
        guard,
        "");
  }

  /**
   * Authorizes a single runtime {@link FileIO#deleteFile} path (not originating from a current
   * request's rollback). Returns {@code null} when allowed or a refusal reason when refused.
   */
  String authorizeRuntimeDelete(String qualifiedPath, RestFileIoPathGuard guard) {
    if (mode == RestCatalogPurgeMode.REFUSE) {
      return "File deletion is disabled on this proxy (rest-catalog.purge.mode=REFUSE)";
    }
    if (mode == RestCatalogPurgeMode.ALLOWLIST) {
      List<String> qualifiedPrefixes = PurgePathQualifier.qualifyPrefixes(allowedPrefixes, guard.conf());
      if (!PathPrefixAllowlist.matchesStrictDescendant(qualifiedPath, qualifiedPrefixes)) {
        return "File deletion is restricted to strict descendants under "
            + "rest-catalog.purge.allowed-prefixes";
      }
      guard.verifyNoSymlinkEscape(qualifiedPath, qualifiedPrefixes);
      return null;
    }
    guard.verifyNoSymlinkEscape(qualifiedPath, null);
    return null;
  }

  /** Wraps delegate FileIO for manifest-walked table purge deletes. */
  FileIO guard(FileIO io, Configuration conf, String tableName) {
    return guard(io, new RestFileIoPathGuard(conf, List.of()), tableName);
  }

  FileIO guard(FileIO io, RestFileIoPathGuard guard, String tableName) {
    if (mode != RestCatalogPurgeMode.ALLOWLIST) {
      return io;
    }
    return new PrefixGuardedFileIO(
        io, PurgePathQualifier.qualifyPrefixes(allowedPrefixes, guard.conf()), guard, tableName);
  }

  private String refusalForObject(
      String kind,
      String objectName,
      String location,
      String metadataFileLocation,
      RestFileIoPathGuard guard,
      String suffixHint
  ) {
    if (mode == RestCatalogPurgeMode.ALLOW) {
      validateLocationsIfPresent(guard, location, metadataFileLocation);
      return null;
    }
    if (mode == RestCatalogPurgeMode.REFUSE) {
      StringBuilder msg = new StringBuilder()
          .append("Purge is disabled on this proxy (rest-catalog.purge.mode=REFUSE); ")
          .append(kind).append(" '").append(objectName).append("' was not dropped.");
      if (suffixHint != null && !suffixHint.isEmpty()) {
        msg.append(' ').append(suffixHint);
      }
      return msg.toString();
    }
    if (location == null && metadataFileLocation == null) {
      return null;
    }
    List<String> qualifiedPrefixes = PurgePathQualifier.qualifyPrefixes(allowedPrefixes, guard.conf());
    String outside = firstPathOutside(
        qualifiedPrefixes, guard, location, metadataFileLocation);
    if (outside == null) {
      return null;
    }
    return "Purge is restricted to rest-catalog.purge.allowed-prefixes; " + kind + " '"
        + objectName + "' was not dropped because '" + outside
        + "' lies outside them.";
  }

  private static void validateLocationsIfPresent(
      RestFileIoPathGuard guard, String... locations) {
    for (String location : locations) {
      if (location != null) {
        guard.validateBeforeFsAccess(location);
      }
    }
  }

  private static String firstPathOutside(
      List<String> qualifiedPrefixes, RestFileIoPathGuard guard, String... locations) {
    for (String location : locations) {
      if (location == null) {
        continue;
      }
      try {
        String qualified = guard.qualifySafely(location);
        if (!PathPrefixAllowlist.matches(qualified, qualifiedPrefixes)) {
          return qualified;
        }
        guard.verifyNoSymlinkEscape(qualified, qualifiedPrefixes, false);
      } catch (RuntimeException e) {
        // Fail closed: any invalid/foreign/symlink-escaping location refuses the drop.
        return location;
      }
    }
    return null;
  }
}
