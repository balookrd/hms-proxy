package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.server.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.util.PathPrefixAllowlist;
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class FileSystemExternalTableDropPurger implements ExternalTableDropPurger {
  static final String EXTERNAL_TABLE_PURGE_KEY = "external.table.purge";
  static final String ALLOWED_PREFIXES_CONF_KEY = "hms.proxy.external-table-drop-purge.allowed-prefixes";

  private static final Logger LOG = LoggerFactory.getLogger(FileSystemExternalTableDropPurger.class);
  private final ProxyConfig config;
  private final KeytabUgiProvider ugiProvider;

  FileSystemExternalTableDropPurger(ProxyConfig config) {
    this(config, new KeytabUgiProvider());
  }

  FileSystemExternalTableDropPurger(ProxyConfig config, KeytabUgiProvider ugiProvider) {
    this.config = config;
    this.ugiProvider = ugiProvider;
  }

  @Override
  public boolean enabledFor(CatalogBackend backend) {
    return config.federation().externalTableDropPurgeEnabled()
        && backend.runtimeProfile() == MetastoreRuntimeProfile.APACHE_3_1_3;
  }

  @Override
  public Optional<PurgeRequest> prepare(CatalogBackend backend, Table table) throws Exception {
    return prepare(backend, table, null);
  }

  @Override
  public Optional<PurgeRequest> prepare(
      CatalogBackend backend,
      Table table,
      ImpersonationContext impersonation
  ) throws Exception {
    if (!enabledFor(backend) || !isEligibleExternalTable(table)) {
      return Optional.empty();
    }
    List<String> allowedPrefixes = allowedPrefixes(backend);
    if (allowedPrefixes.isEmpty()) {
      LOG.warn(
          "requestId={} skipping external-table purge for catalog '{}' because {} is not configured",
          RequestContext.currentRequestId(), backend.name(), ALLOWED_PREFIXES_CONF_KEY);
      return Optional.empty();
    }
    String rawLocation = table.getSd().getLocation().trim();
    if (PathPrefixAllowlist.containsUnsafePathSyntax(rawLocation)) {
      LOG.warn(
          "requestId={} skipping external-table purge for catalog '{}' because location '{}' contains unsafe path syntax",
          RequestContext.currentRequestId(), backend.name(), rawLocation);
      return Optional.empty();
    }
    // Verify scheme/authority lexically before calling FileSystem.get(...) so hostile or non-allowlisted
    // locations cannot trigger outbound connections or pollute Hadoop's static FileSystem.CACHE.
    if (!isUriCompatibleWithAllowlist(rawLocation, allowedPrefixes, backend.hiveConf())) {
      LOG.warn(
          "requestId={} skipping external-table purge for catalog '{}' because URI scheme/authority of '{}' "
              + "does not match any allowlisted prefix",
          RequestContext.currentRequestId(), backend.name(), rawLocation);
      return Optional.empty();
    }
    return runWithFilesystemUgi(backend, impersonation, () -> {
      Path rawPath = new Path(rawLocation);
      FileSystem fileSystem = rawPath.getFileSystem(backend.hiveConf());
      Path qualifiedPath = rawPath.makeQualified(fileSystem.getUri(), fileSystem.getWorkingDirectory());
      String qualifiedLocation = qualifiedPath.toString();
      List<String> qualifiedPrefixes = qualifyPrefixes(allowedPrefixes, fileSystem);
      if (!PathPrefixAllowlist.matchesStrictDescendant(qualifiedLocation, qualifiedPrefixes)) {
        LOG.warn(
            "requestId={} skipping external-table purge for catalog '{}' because location '{}' "
                + "is outside configured allowlist (or equals allowlist root)",
            RequestContext.currentRequestId(), backend.name(), qualifiedLocation);
        return Optional.empty();
      }
      if (fileSystem.exists(qualifiedPath)) {
        String resolvedLocation = resolveRealPath(qualifiedPath, fileSystem);
        List<String> resolvedPrefixes = resolveExistingPrefixes(qualifiedPrefixes, fileSystem);
        if (!PathPrefixAllowlist.matchesStrictDescendant(resolvedLocation, resolvedPrefixes)) {
          LOG.warn(
              "requestId={} skipping external-table purge for catalog '{}' because resolved location '{}' "
                  + "(from '{}') escapes configured allowlist",
              RequestContext.currentRequestId(), backend.name(), resolvedLocation, qualifiedLocation);
          return Optional.empty();
        }
      }
      return Optional.of(new PurgeRequest(qualifiedLocation, impersonation));
    });
  }

  @Override
  public void purge(CatalogBackend backend, PurgeRequest request) throws Exception {
    if (request == null || PathPrefixAllowlist.containsUnsafePathSyntax(request.location())) {
      LOG.warn(
          "requestId={} skipping external-table purge for catalog '{}' because purge request location is invalid",
          RequestContext.currentRequestId(), backend.name());
      return;
    }
    List<String> allowedPrefixes = allowedPrefixes(backend);
    if (allowedPrefixes.isEmpty()
        || !isUriCompatibleWithAllowlist(request.location(), allowedPrefixes, backend.hiveConf())) {
      LOG.warn(
          "requestId={} skipping external-table purge for catalog '{}' because location '{}' is outside allowlist",
          RequestContext.currentRequestId(), backend.name(), request.location());
      return;
    }
    Path location = new Path(request.location());
    runWithFilesystemUgi(backend, request.impersonation(), () -> {
      deleteRecursively(backend, location, allowedPrefixes);
      return null;
    });
  }

  private void deleteRecursively(
      CatalogBackend backend,
      Path location,
      List<String> allowedPrefixes
  ) throws IOException {
    FileSystem fileSystem = location.getFileSystem(backend.hiveConf());
    Path qualifiedPath = location.makeQualified(fileSystem.getUri(), fileSystem.getWorkingDirectory());
    List<String> qualifiedPrefixes = qualifyPrefixes(allowedPrefixes, fileSystem);
    if (!PathPrefixAllowlist.matchesStrictDescendant(qualifiedPath.toString(), qualifiedPrefixes)) {
      LOG.warn(
          "requestId={} refusing external-table purge for catalog '{}' because '{}' is outside allowlist",
          RequestContext.currentRequestId(), backend.name(), qualifiedPath);
      return;
    }
    if (fileSystem.exists(qualifiedPath)) {
      String resolvedLocation = resolveRealPath(qualifiedPath, fileSystem);
      List<String> resolvedPrefixes = resolveExistingPrefixes(qualifiedPrefixes, fileSystem);
      if (!PathPrefixAllowlist.matchesStrictDescendant(resolvedLocation, resolvedPrefixes)) {
        LOG.warn(
            "requestId={} refusing external-table purge for catalog '{}' because resolved path '{}' "
                + "(from '{}') escapes allowlist",
            RequestContext.currentRequestId(), backend.name(), resolvedLocation, qualifiedPath);
        return;
      }
    }
    boolean deleted = fileSystem.delete(qualifiedPath, true);
    if (!deleted && fileSystem.exists(qualifiedPath)) {
      throw new IOException("FileSystem.delete returned false for " + qualifiedPath);
    }
    LOG.info(
        "requestId={} purged external table data for catalog '{}' at location '{}'",
        RequestContext.currentRequestId(), backend.name(), qualifiedPath);
  }

  private <T> T runWithFilesystemUgi(
      CatalogBackend backend,
      ImpersonationContext impersonation,
      PrivilegedExceptionAction<T> action
  ) throws Exception {
    if (usesKerberos(backend)) {
      UserGroupInformation serviceUgi = ugiProvider.get(
          config.security().outboundPrincipal(),
          config.security().outboundKeytab());
      if (impersonation != null && impersonation.userName() != null && !impersonation.userName().isBlank()
          && backend.impersonationEnabled()) {
        UserGroupInformation proxyUgi = UserGroupInformation.createProxyUser(impersonation.userName(), serviceUgi);
        try {
          return proxyUgi.doAs(action);
        } finally {
          FileSystem.closeAllForUGI(proxyUgi);
        }
      }
      // The FileSystem cached under this shared UGI is deliberately left open: it is bounded (one
      // entry per scheme/authority) and closing it would break concurrent purges using the same UGI.
      return serviceUgi.doAs(action);
    }
    if (impersonation != null && impersonation.userName() != null && !impersonation.userName().isBlank()
        && backend.impersonationEnabled()) {
      UserGroupInformation remoteUgi = UserGroupInformation.createRemoteUser(impersonation.userName());
      try {
        return remoteUgi.doAs(action);
      } finally {
        FileSystem.closeAllForUGI(remoteUgi);
      }
    }
    return action.run();
  }

  private boolean usesKerberos(CatalogBackend backend) {
    return "kerberos".equalsIgnoreCase(backend.hiveConf().get("hadoop.security.authentication"))
        && config.security().outboundPrincipal() != null
        && config.security().outboundKeytab() != null;
  }

  private List<String> allowedPrefixes(CatalogBackend backend) {
    CatalogConfig catalogConfig = config.catalogs().get(backend.name());
    if (catalogConfig == null) {
      return List.of();
    }
    return PathPrefixAllowlist.parse(catalogConfig.hiveConf().get(ALLOWED_PREFIXES_CONF_KEY));
  }

  private static boolean isUriCompatibleWithAllowlist(
      String rawLocation,
      List<String> allowedPrefixes,
      Configuration conf
  ) {
    URI locationUri;
    try {
      locationUri = new Path(rawLocation).toUri();
    } catch (IllegalArgumentException e) {
      return false;
    }
    URI defaultFsUri = FileSystem.getDefaultUri(conf);
    String locationScheme = locationUri.getScheme() != null ? locationUri.getScheme() : defaultFsUri.getScheme();
    String locationAuthority = locationUri.getScheme() != null
        ? locationUri.getAuthority()
        : defaultFsUri.getAuthority();
    for (String prefix : allowedPrefixes) {
      URI prefixUri;
      try {
        prefixUri = new Path(prefix).toUri();
      } catch (IllegalArgumentException e) {
        continue;
      }
      String prefixScheme = prefixUri.getScheme() != null ? prefixUri.getScheme() : defaultFsUri.getScheme();
      String prefixAuthority = prefixUri.getScheme() != null
          ? prefixUri.getAuthority()
          : defaultFsUri.getAuthority();
      if (sameSchemeAndAuthority(locationScheme, locationAuthority, prefixScheme, prefixAuthority)) {
        return true;
      }
    }
    return false;
  }

  private static boolean sameSchemeAndAuthority(
      String leftScheme,
      String leftAuthority,
      String rightScheme,
      String rightAuthority
  ) {
    if (leftScheme == null || rightScheme == null || !leftScheme.equalsIgnoreCase(rightScheme)) {
      return false;
    }
    if ("file".equalsIgnoreCase(leftScheme)) {
      String normLeft = leftAuthority == null ? "" : leftAuthority;
      String normRight = rightAuthority == null ? "" : rightAuthority;
      return normLeft.equalsIgnoreCase(normRight);
    }
    return Objects.equals(
        leftAuthority == null ? null : leftAuthority.toLowerCase(java.util.Locale.ROOT),
        rightAuthority == null ? null : rightAuthority.toLowerCase(java.util.Locale.ROOT));
  }

  private static List<String> qualifyPrefixes(List<String> prefixes, FileSystem fileSystem) {
    List<String> qualified = new ArrayList<>(prefixes.size());
    URI fsUri = fileSystem.getUri();
    Path workingDirectory = fileSystem.getWorkingDirectory();
    for (String prefix : prefixes) {
      try {
        Path prefixPath = new Path(prefix);
        URI prefixUri = prefixPath.toUri();
        if (prefixUri.getScheme() == null
            || sameSchemeAndAuthority(prefixUri.getScheme(), prefixUri.getAuthority(),
                fsUri.getScheme(), fsUri.getAuthority())) {
          qualified.add(prefixPath.makeQualified(fsUri, workingDirectory).toString());
        } else {
          qualified.add(prefix);
        }
      } catch (IllegalArgumentException e) {
        qualified.add(prefix);
      }
    }
    return qualified;
  }

  private static List<String> resolveExistingPrefixes(List<String> qualifiedPrefixes, FileSystem fileSystem) {
    List<String> resolved = new ArrayList<>(qualifiedPrefixes.size());
    URI fsUri = fileSystem.getUri();
    for (String prefix : qualifiedPrefixes) {
      try {
        Path prefixPath = new Path(prefix);
        URI prefixUri = prefixPath.toUri();
        if (sameSchemeAndAuthority(prefixUri.getScheme(), prefixUri.getAuthority(),
                fsUri.getScheme(), fsUri.getAuthority())
            && fileSystem.exists(prefixPath)) {
          resolved.add(resolveRealPath(prefixPath, fileSystem));
        } else {
          resolved.add(prefix);
        }
      } catch (IOException | IllegalArgumentException e) {
        resolved.add(prefix);
      }
    }
    return resolved;
  }

  private static String resolveRealPath(Path path, FileSystem fileSystem) throws IOException {
    Path hadoopResolved = fileSystem.resolvePath(path);
    if ("file".equalsIgnoreCase(fileSystem.getUri().getScheme())) {
      try {
        java.nio.file.Path realNioPath = java.nio.file.Path.of(hadoopResolved.toUri()).toRealPath();
        return realNioPath.toUri().toString();
      } catch (IllegalArgumentException ignored) {
        // Fall back to Hadoop resolution below.
      }
    }
    return hadoopResolved.toString();
  }

  private static boolean isEligibleExternalTable(Table table) {
    if (table == null || !"EXTERNAL_TABLE".equalsIgnoreCase(table.getTableType())) {
      return false;
    }
    StorageDescriptor storageDescriptor = table.getSd();
    if (storageDescriptor == null || storageDescriptor.getLocation() == null
        || storageDescriptor.getLocation().isBlank()) {
      return false;
    }
    return "true".equalsIgnoreCase(Optional.ofNullable(table.getParameters())
        .map(parameters -> parameters.get(EXTERNAL_TABLE_PURGE_KEY))
        .orElse(null));
  }
}
