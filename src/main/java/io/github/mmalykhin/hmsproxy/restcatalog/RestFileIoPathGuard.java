package io.github.mmalykhin.hmsproxy.restcatalog;

import io.github.mmalykhin.hmsproxy.util.PathPrefixAllowlist;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.iceberg.exceptions.ForbiddenException;

/**
 * Validates and qualifies Iceberg FileIO paths before any Hadoop {@link FileSystem} instantiation.
 *
 * <p>Prevents SSRF / {@code FileSystem.CACHE} leaks / arbitrary local file reads via foreign schemes
 * or authorities (such as {@code file:///etc/passwd} on an HDFS catalog or {@code hdfs://evil:8020})
 * and verifies resolved real paths against symbolic-link escapes.
 */
final class RestFileIoPathGuard {
  private final Configuration conf;
  private final URI defaultFsUri;
  private final List<String> allowedPrefixes;
  private final List<URI> federatedReadFsUris;

  RestFileIoPathGuard(Configuration conf, List<String> allowedPrefixes) {
    this(conf, allowedPrefixes, List.of());
  }

  RestFileIoPathGuard(
      Configuration conf,
      List<String> allowedPrefixes,
      List<URI> federatedReadFsUris
  ) {
    this.conf = Objects.requireNonNull(conf, "conf");
    this.defaultFsUri = FileSystem.getDefaultUri(conf);
    this.allowedPrefixes = allowedPrefixes == null ? List.of() : List.copyOf(allowedPrefixes);
    this.federatedReadFsUris = federatedReadFsUris == null
        ? List.of()
        : federatedReadFsUris.stream().filter(Objects::nonNull).toList();
  }

  Configuration conf() {
    return conf;
  }

  /**
   * Lexically validates scheme, authority, userinfo, and traversal syntax BEFORE calling {@link
   * org.apache.hadoop.fs.Path#getFileSystem(Configuration)}.
   */
  void validateBeforeFsAccess(String rawPath) {
    validateBeforeFsAccess(rawPath, false);
  }

  /**
   * Read-path variant that additionally trusts federated catalog default filesystems configured
   * on the proxy so default-catalog federated reads can read foreign-catalog table metadata
   * without allowing writes, deletes, or local {@code file:} escapes.
   */
  void validateBeforeReadAccess(String rawPath) {
    validateBeforeFsAccess(rawPath, true);
  }

  private void validateBeforeFsAccess(String rawPath, boolean allowFederatedReadFs) {
    if (PathPrefixAllowlist.containsUnsafePathSyntax(rawPath)) {
      throw new ForbiddenException("Storage path contains unsafe syntax");
    }
    URI uri = parsePathUri(rawPath);
    if (uri.getUserInfo() != null) {
      throw new ForbiddenException("Storage path must not contain userinfo");
    }
    if (uri.getQuery() != null || uri.getFragment() != null) {
      throw new ForbiddenException("Storage path must not contain URI query or fragment");
    }
    String scheme = uri.getScheme();
    if (scheme == null) {
      if (uri.getAuthority() != null) {
        throw new ForbiddenException("Storage path authority without scheme is not allowed");
      }
      return;
    }
    if (matchesFsAuthority(uri, defaultFsUri)) {
      return;
    }
    if (allowFederatedReadFs && matchesFederatedReadFs(uri)) {
      return;
    }
    if (matchesAllowedPrefix(uri, rawPath)) {
      return;
    }
    throw new ForbiddenException(
        "Storage path scheme or authority is not allowed for this catalog");
  }

  private boolean matchesFederatedReadFs(URI uri) {
    for (URI federatedUri : federatedReadFsUris) {
      if (matchesFsAuthority(uri, federatedUri)) {
        return true;
      }
    }
    return false;
  }

  /** Lexically validates {@code rawPath}, qualifies it with Hadoop conf, and re-checks syntax. */
  String qualifySafely(String rawPath) {
    validateBeforeFsAccess(rawPath);
    String qualified;
    try {
      qualified = PurgePathQualifier.qualify(rawPath, conf);
    } catch (IOException | IllegalArgumentException e) {
      throw new ForbiddenException("Storage path cannot be qualified");
    }
    if (PathPrefixAllowlist.containsUnsafePathSyntax(qualified)) {
      throw new ForbiddenException("Qualified storage path contains unsafe syntax");
    }
    return qualified;
  }

  /**
   * Resolves symlinks on local ({@code file:}) or remote Hadoop filesystems and enforces strict
   * descendant containment when {@code strictAllowlistOrNull} is provided.
   */
  void verifyNoSymlinkEscape(String qualifiedPath, List<String> strictAllowlistOrNull) {
    verifyNoSymlinkEscape(qualifiedPath, strictAllowlistOrNull, true);
  }

  void verifyNoSymlinkEscape(
      String qualifiedPath,
      List<String> allowlistOrNull,
      boolean requireStrictDescendant
  ) {
    URI uri = parsePathUri(qualifiedPath);
    String scheme = uri.getScheme() != null ? uri.getScheme() : defaultFsUri.getScheme();
    if ("file".equalsIgnoreCase(scheme)) {
      verifyLocalNoSymlinkEscape(uri, allowlistOrNull, requireStrictDescendant);
    } else {
      verifyHadoopNoSymlinkEscape(qualifiedPath, allowlistOrNull, requireStrictDescendant);
    }
  }

  /** Best-effort version returning {@code false} instead of throwing, for skip-on-refusal purge. */
  boolean isStrictDescendantSafely(String rawPath, List<String> qualifiedPurgeAllowlist) {
    try {
      String qualified = qualifySafely(rawPath);
      if (!PathPrefixAllowlist.matchesStrictDescendant(qualified, qualifiedPurgeAllowlist)) {
        return false;
      }
      verifyNoSymlinkEscape(qualified, qualifiedPurgeAllowlist);
      return true;
    } catch (RuntimeException e) {
      return false;
    }
  }

  private void verifyLocalNoSymlinkEscape(
      URI targetUri,
      List<String> allowlistOrNull,
      boolean requireStrictDescendant
  ) {
    Path localPath = toLocalPath(targetUri);
    if (Files.isSymbolicLink(localPath)) {
      throw new ForbiddenException("Refusing direct symbolic link storage path");
    }
    String realTargetUri = resolveRealLocalUri(localPath);
    if (PathPrefixAllowlist.containsUnsafePathSyntax(realTargetUri)) {
      throw new ForbiddenException("Resolved storage path contains unsafe syntax");
    }
    if (allowlistOrNull != null) {
      List<String> resolvedAllowlist = new ArrayList<>(allowlistOrNull.size());
      for (String prefix : allowlistOrNull) {
        resolvedAllowlist.add(resolveLocalPrefixIfFile(prefix));
      }
      boolean allowed = requireStrictDescendant
          ? PathPrefixAllowlist.matchesStrictDescendant(realTargetUri, resolvedAllowlist)
          : PathPrefixAllowlist.matches(realTargetUri, resolvedAllowlist);
      if (!allowed) {
        throw new ForbiddenException("Storage path escapes allowlist via symbolic link");
      }
    }
  }

  private void verifyHadoopNoSymlinkEscape(
      String qualifiedPath,
      List<String> allowlistOrNull,
      boolean requireStrictDescendant
  ) {
    try {
      org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(qualifiedPath);
      FileSystem fs = hadoopPath.getFileSystem(conf);
      try {
        if (fs.getFileLinkStatus(hadoopPath).isSymlink()) {
          throw new ForbiddenException("Refusing direct symbolic link storage path");
        }
      } catch (UnsupportedOperationException | IOException ignored) {
        // FileSystem without symlink support or missing target file; ancestor check below handles it.
      }
      String resolved = resolveHadoopRealPath(fs, hadoopPath);
      if (PathPrefixAllowlist.containsUnsafePathSyntax(resolved)) {
        throw new ForbiddenException("Resolved storage path contains unsafe syntax");
      }
      if (allowlistOrNull != null) {
        List<String> resolvedAllowlist = new ArrayList<>(allowlistOrNull.size());
        for (String prefix : allowlistOrNull) {
          resolvedAllowlist.add(resolvePrefixSafely(prefix));
        }
        boolean allowed = requireStrictDescendant
            ? PathPrefixAllowlist.matchesStrictDescendant(resolved, resolvedAllowlist)
            : PathPrefixAllowlist.matches(resolved, resolvedAllowlist);
        if (!allowed) {
          throw new ForbiddenException("Storage path escapes allowlist");
        }
      }
    } catch (ForbiddenException e) {
      throw e;
    } catch (IOException | IllegalArgumentException e) {
      throw new ForbiddenException("Unable to verify storage path");
    }
  }

  private String resolvePrefixSafely(String prefix) {
    if (prefix == null || PathPrefixAllowlist.containsUnsafePathSyntax(prefix)) {
      return "";
    }
    try {
      URI uri = parsePathUri(prefix);
      if ("file".equalsIgnoreCase(uri.getScheme())) {
        return resolveRealLocalUri(toLocalPath(uri));
      }
      org.apache.hadoop.fs.Path p = new org.apache.hadoop.fs.Path(prefix);
      if (!matchesFsAuthority(uri, defaultFsUri) && !matchesAllowedPrefix(uri, prefix)) {
        return prefix;
      }
      FileSystem fs = p.getFileSystem(conf);
      return resolveHadoopRealPath(fs, p);
    } catch (RuntimeException | IOException e) {
      return prefix;
    }
  }

  private String resolveLocalPrefixIfFile(String prefix) {
    if (prefix == null || PathPrefixAllowlist.containsUnsafePathSyntax(prefix)) {
      return "";
    }
    try {
      URI uri = parsePathUri(prefix);
      String scheme = uri.getScheme() != null ? uri.getScheme() : defaultFsUri.getScheme();
      if ("file".equalsIgnoreCase(scheme)) {
        return resolveRealLocalUri(toLocalPath(uri));
      }
    } catch (RuntimeException ignored) {
      // Non-local or unparseable prefix simply won't match local target URIs.
    }
    return prefix;
  }

  private static String resolveRealLocalUri(Path target) {
    Deque<String> missingSegments = new ArrayDeque<>();
    Path probe = target;
    while (probe != null && !Files.exists(probe, LinkOption.NOFOLLOW_LINKS)) {
      Path name = probe.getFileName();
      if (name != null) {
        missingSegments.addFirst(name.toString());
      }
      probe = probe.getParent();
    }
    if (probe == null) {
      throw new ForbiddenException("Storage path has no existing filesystem root");
    }
    Path realBase;
    try {
      realBase = probe.toRealPath();
    } catch (IOException e) {
      throw new ForbiddenException("Unable to resolve storage path");
    }
    for (String segment : missingSegments) {
      realBase = realBase.resolve(segment);
    }
    return new org.apache.hadoop.fs.Path(realBase.toUri()).toString();
  }

  private static String resolveHadoopRealPath(
      FileSystem fs, org.apache.hadoop.fs.Path target) throws IOException {
    Deque<String> missingSegments = new ArrayDeque<>();
    org.apache.hadoop.fs.Path probe = target;
    while (probe != null && !fs.exists(probe)) {
      String name = probe.getName();
      if (!name.isEmpty()) {
        missingSegments.addFirst(name);
      }
      probe = probe.getParent();
    }
    if (probe == null) {
      return target.toString();
    }
    org.apache.hadoop.fs.Path resolved = fs.resolvePath(probe);
    for (String segment : missingSegments) {
      resolved = new org.apache.hadoop.fs.Path(resolved, segment);
    }
    return resolved.toString();
  }

  private boolean matchesAllowedPrefix(URI uri, String rawPath) {
    if (allowedPrefixes.isEmpty()) {
      return false;
    }
    for (String prefix : allowedPrefixes) {
      if (prefix == null || PathPrefixAllowlist.containsUnsafePathSyntax(prefix)) {
        continue;
      }
      URI prefixUri;
      try {
        prefixUri = parsePathUri(prefix);
      } catch (ForbiddenException ignored) {
        continue;
      }
      if (prefixUri.getScheme() == null) {
        continue;
      }
      if (!matchesFsAuthority(uri, prefixUri)) {
        continue;
      }
      if ("file".equalsIgnoreCase(uri.getScheme())) {
        if (PathPrefixAllowlist.matchesStrictDescendant(rawPath, List.of(prefix))
            || rawPath.equals(prefix)) {
          return true;
        }
        String candUri = toLocalPath(uri).toUri().toString();
        String prefUri = toLocalPath(prefixUri).toUri().toString();
        if (PathPrefixAllowlist.matches(candUri, List.of(prefUri))) {
          return true;
        }
      } else if (PathPrefixAllowlist.matches(rawPath, List.of(prefix))) {
        return true;
      }
    }
    return false;
  }

  private static boolean matchesFsAuthority(URI candidate, URI allowed) {
    if (candidate == null || allowed == null) {
      return false;
    }
    String candScheme = normalizeLower(candidate.getScheme());
    String allowedScheme = normalizeLower(allowed.getScheme());
    if (candScheme == null || !candScheme.equals(allowedScheme)) {
      return false;
    }
    String candAuth = normalizeAuthority(candidate.getAuthority());
    String allowedAuth = normalizeAuthority(allowed.getAuthority());
    if ("file".equals(candScheme)) {
      return candAuth == null && allowedAuth == null;
    }
    return candAuth != null && candAuth.equals(allowedAuth);
  }

  private static String normalizeAuthority(String authority) {
    if (authority == null || authority.isEmpty()) {
      return null;
    }
    return authority.toLowerCase(Locale.ROOT);
  }

  private static String normalizeLower(String value) {
    return value == null ? null : value.toLowerCase(Locale.ROOT);
  }

  private static URI parsePathUri(String path) {
    try {
      return new org.apache.hadoop.fs.Path(path).toUri();
    } catch (IllegalArgumentException e) {
      throw new ForbiddenException("Invalid storage path syntax");
    }
  }

  private static Path toLocalPath(URI uri) {
    try {
      String path = uri.getPath();
      if (path == null || path.isEmpty()) {
        throw new ForbiddenException("Invalid local storage path");
      }
      return Path.of(URI.create("file://" + uri.getRawPath()));
    } catch (IllegalArgumentException e) {
      throw new ForbiddenException("Invalid local storage path");
    }
  }
}
