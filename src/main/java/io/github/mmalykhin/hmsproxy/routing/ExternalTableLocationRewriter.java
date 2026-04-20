package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import java.net.URI;
import java.util.Locale;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import io.github.mmalykhin.hmsproxy.config.ExternalTableLocationRewriteMode;
import io.github.mmalykhin.hmsproxy.config.FederationConfig;

final class ExternalTableLocationRewriter {
  private static final String EXTERNAL_TABLE = "EXTERNAL_TABLE";

  private final FederationConfig federationConfig;
  private final URI sourceDefaultFsUri;

  ExternalTableLocationRewriter(FederationConfig federationConfig) {
    this.federationConfig = federationConfig;
    this.sourceDefaultFsUri = parseDefaultFsUri(
        federationConfig.externalTableLocationRewriteSourceDefaultFs(),
        "federation.external-table-location-rewrite.source-default-fs");
  }

  void rewriteObjectArguments(Object[] args, CatalogRouter.ResolvedNamespace namespace, String methodName)
      throws MetaException {
    if (args == null || args.length == 0 || !supports(methodName)
        || federationConfig.externalTableLocationRewriteMode()
        == ExternalTableLocationRewriteMode.DISABLED) {
      return;
    }
    for (Object argument : args) {
      if (argument instanceof Table table) {
        rewriteTableLocation(table, namespace);
      }
    }
  }

  private boolean supports(String methodName) {
    if (methodName == null) {
      return false;
    }
    return methodName.startsWith("create_table") || methodName.startsWith("alter_table");
  }

  private void rewriteTableLocation(Table table, CatalogRouter.ResolvedNamespace namespace) throws MetaException {
    if (!isExternalTable(table) || table == null || !table.isSetSd()) {
      return;
    }
    StorageDescriptor storageDescriptor = table.getSd();
    String location = blankToNull(storageDescriptor.getLocation());
    if (location == null) {
      return;
    }
    String rewritten = rewriteLocation(location, namespace.backend().defaultFileSystemUri());
    if (!rewritten.equals(location)) {
      storageDescriptor.setLocation(rewritten);
    }
  }

  private String rewriteLocation(String location, URI targetDefaultFs) throws MetaException {
    URI locationUri = parseLocationUri(location);
    if (shouldQualifyUnqualified(locationUri)) {
      return qualifyToDefaultFs(locationUri, targetDefaultFs);
    }
    if (federationConfig.externalTableLocationRewriteMode()
        != ExternalTableLocationRewriteMode.REWRITE_IF_SOURCE_DEFAULT_FS) {
      return location;
    }
    if (sourceDefaultFsUri == null || !sameFileSystem(locationUri, sourceDefaultFsUri)
        || sameFileSystem(locationUri, targetDefaultFs)) {
      return location;
    }
    return qualifyToDefaultFs(locationUri, targetDefaultFs);
  }

  private boolean shouldQualifyUnqualified(URI locationUri) {
    String scheme = blankToNull(locationUri.getScheme());
    if (scheme == null) {
      return isAbsolutePath(locationUri.getPath());
    }
    return "hdfs".equalsIgnoreCase(scheme) && blankToNull(locationUri.getAuthority()) == null
        && isAbsolutePath(locationUri.getPath());
  }

  private String qualifyToDefaultFs(URI locationUri, URI targetDefaultFs) {
    String path = blankToNull(locationUri.getPath());
    if (path == null) {
      path = "/";
    }
    return new Path(targetDefaultFs.getScheme(), targetDefaultFs.getAuthority(), path).toString();
  }

  private static boolean isExternalTable(Table table) {
    if (table == null) {
      return false;
    }
    if (EXTERNAL_TABLE.equalsIgnoreCase(blankToNull(table.getTableType()))) {
      return true;
    }
    Map<String, String> parameters = table.getParameters();
    return parameters != null && "TRUE".equalsIgnoreCase(parameters.get("EXTERNAL"));
  }

  private static boolean sameFileSystem(URI left, URI right) {
    if (left == null || right == null) {
      return false;
    }
    return normalizeUriComponent(left.getScheme()).equals(normalizeUriComponent(right.getScheme()))
        && normalizeUriComponent(left.getAuthority()).equals(normalizeUriComponent(right.getAuthority()));
  }

  private static String normalizeUriComponent(String value) {
    return value == null ? "" : value.toLowerCase(Locale.ROOT);
  }

  private static boolean isAbsolutePath(String path) {
    return path != null && path.startsWith("/");
  }

  private static URI parseLocationUri(String location) throws MetaException {
    try {
      return new Path(location).toUri();
    } catch (IllegalArgumentException e) {
      MetaException error =
          new MetaException("Invalid external table LOCATION for rewrite: " + location);
      error.initCause(e);
      throw error;
    }
  }

  private static URI parseDefaultFsUri(String value, String propertyName) {
    String normalized = blankToNull(value);
    if (normalized == null) {
      return null;
    }
    try {
      URI uri = new Path(normalized).toUri();
      if (blankToNull(uri.getScheme()) == null) {
        throw new IllegalArgumentException("scheme is required");
      }
      return uri;
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid value for " + propertyName + ": " + value, e);
    }
  }

  private static String blankToNull(String value) {
    if (value == null) {
      return null;
    }
    String normalized = value.trim();
    return normalized.isEmpty() ? null : normalized;
  }
}
