package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.config.server.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;

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
    Path qualifiedLocation = qualifyLocation(backend, table.getSd().getLocation());
    String location = qualifiedLocation.toString();
    if (!matchesAllowedPrefixes(location, allowedPrefixes)) {
      LOG.warn(
          "requestId={} skipping external-table purge for catalog '{}' because location '{}' is outside configured allowlist",
          RequestContext.currentRequestId(), backend.name(), location);
      return Optional.empty();
    }
    return Optional.of(new PurgeRequest(location));
  }

  @Override
  public void purge(CatalogBackend backend, PurgeRequest request) throws Exception {
    Path location = new Path(request.location());
    if (usesKerberos(backend)) {
      UserGroupInformation ugi = ugiProvider.get(
          config.security().outboundPrincipal(),
          config.security().outboundKeytab());
      // The FileSystem cached under this shared UGI is deliberately left open: it is bounded (one
      // entry per scheme/authority) and closing it would break concurrent purges using the same UGI.
      ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
        deleteRecursively(backend, location);
        return null;
      });
      return;
    }
    deleteRecursively(backend, location);
  }

  private void deleteRecursively(CatalogBackend backend, Path location) throws IOException {
    FileSystem fileSystem = location.getFileSystem(backend.hiveConf());
    boolean deleted = fileSystem.delete(location, true);
    if (!deleted && fileSystem.exists(location)) {
      throw new IOException("FileSystem.delete returned false for " + location);
    }
    LOG.info(
        "requestId={} purged external table data for catalog '{}' at location '{}'",
        RequestContext.currentRequestId(), backend.name(), location);
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
    String rawPrefixes = catalogConfig.hiveConf().get(ALLOWED_PREFIXES_CONF_KEY);
    if (rawPrefixes == null || rawPrefixes.isBlank()) {
      return List.of();
    }
    return Arrays.stream(rawPrefixes.split(","))
        .map(String::trim)
        .filter(value -> !value.isEmpty())
        .toList();
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

  private static Path qualifyLocation(CatalogBackend backend, String location) throws IOException {
    Path path = new Path(location);
    FileSystem fileSystem = path.getFileSystem(backend.hiveConf());
    return path.makeQualified(fileSystem.getUri(), fileSystem.getWorkingDirectory());
  }

  private static boolean matchesAllowedPrefixes(String location, List<String> allowedPrefixes) {
    for (String prefix : allowedPrefixes) {
      String normalizedPrefix = prefix.trim();
      if (normalizedPrefix.isEmpty()) {
        continue;
      }
      if (location.equals(normalizedPrefix)) {
        return true;
      }
      String boundaryPrefix = normalizedPrefix.endsWith("/")
          ? normalizedPrefix
          : normalizedPrefix + "/";
      if (location.startsWith(boundaryPrefix)) {
        return true;
      }
    }
    return false;
  }
}
