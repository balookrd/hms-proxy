package io.github.mmalykhin.hmsproxy.security.groups;

import java.util.List;
import java.util.Optional;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unified resolver for user group memberships with support for on-disk caching,
 * instant cold start, and fallback resilience against directory service outages.
 */
public final class UserGroupResolver implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(UserGroupResolver.class);

  private final GroupDiskCache diskCache;

  public UserGroupResolver() {
    this(null);
  }

  public UserGroupResolver(GroupDiskCache diskCache) {
    this.diskCache = diskCache;
  }

  public GroupDiskCache diskCache() {
    return diskCache;
  }

  /**
   * Resolves the list of groups for the specified user name.
   *
   * <p>Resolution order:
   * <ol>
   *   <li>Thread-local / process UGI context (e.g. active {@code doAs} user or test harness)</li>
   *   <li>Persistent disk cache (instant cold start: 0 network round-trips)</li>
   *   <li>Hadoop {@link UserGroupInformation#createRemoteUser(String)} group mapping service (AD / LDAP / OS)</li>
   *   <li>Stale cache fallback if directory service lookup fails</li>
   * </ol>
   *
   * @param userName user name to resolve groups for
   * @return list of resolved group names, or empty list if none could be resolved
   */
  public List<String> resolveGroups(String userName) {
    if (userName == null || userName.isBlank()) {
      return List.of();
    }

    // 1. Thread-local or process UGI context (preserves doAs and unit test isolation)
    try {
      UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
      if (currentUser != null && userName.equals(currentUser.getShortUserName())) {
        String[] groupNames = currentUser.getGroupNames();
        if (groupNames != null && groupNames.length > 0) {
          List<String> groups = List.of(groupNames);
          if (diskCache != null && diskCache.isEnabled()) {
            diskCache.put(userName, groups);
          }
          return groups;
        }
      }
    } catch (Exception ignored) {
      // Current user context unavailable
    }

    // 2. Persistent disk cache lookup (0 ms on cold start)
    if (diskCache != null && diskCache.isEnabled()) {
      Optional<List<String>> cached = diskCache.get(userName);
      if (cached.isPresent()) {
        return cached.get();
      }
    }

    // 3. Remote user group mapping query (Hadoop Groups -> LdapGroupsMapping or ShellBasedUnixGroupsMapping)
    try {
      UserGroupInformation ugi = UserGroupInformation.createRemoteUser(userName);
      String[] groupNames = ugi.getGroupNames();
      List<String> groups = groupNames != null && groupNames.length > 0 ? List.of(groupNames) : List.of();
      if (diskCache != null && diskCache.isEnabled()) {
        diskCache.put(userName, groups);
      }
      return groups;
    } catch (Exception e) {
      LOG.warn("Unable to resolve groups from Hadoop UGI for authenticated user '{}': {}",
          userName, e.getMessage());

      // 4. Fallback (serve-stale): if AD/LDAP is temporarily unreachable, serve last known groups from disk
      if (diskCache != null && diskCache.isEnabled()) {
        Optional<List<String>> stale = diskCache.getStale(userName);
        if (stale.isPresent()) {
          LOG.info("Serving stale cached groups for user '{}' due to directory lookup failure: {}",
              userName, stale.get());
          return stale.get();
        }
      }
    }

    return List.of();
  }

  @Override
  public void close() {
    if (diskCache != null) {
      diskCache.close();
    }
  }
}
