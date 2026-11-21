package io.github.mmalykhin.hmsproxy.security.groups;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.mmalykhin.hmsproxy.config.security.GroupDiskCacheConfig;
import io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * High-performance on-disk group cache providing instant cold-start resolution
 * for Active Directory / LDAP groups and resilience against directory service outages.
 */
public final class GroupDiskCache implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(GroupDiskCache.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  static final long DEFAULT_MAX_CACHE_FILE_BYTES = 64L * 1024L * 1024L;
  private static final boolean POSIX_SUPPORTED =
      FileSystems.getDefault().supportedFileAttributeViews().contains("posix");
  private static final Set<PosixFilePermission> PRIVATE_DIR_PERMS =
      PosixFilePermissions.fromString("rwx------");
  private static final Set<PosixFilePermission> PRIVATE_FILE_PERMS =
      PosixFilePermissions.fromString("rw-------");

  private final GroupDiskCacheConfig config;
  private final PrometheusMetrics metrics;
  private final LongSupplier clock;
  private final long maxCacheFileBytes;
  private final ConcurrentHashMap<String, CachedUserGroups> cache = new ConcurrentHashMap<>();
  private final AtomicLong mutationGeneration = new AtomicLong(0L);
  private final AtomicLong persistedGeneration = new AtomicLong(0L);
  private final ScheduledExecutorService scheduler;

  public GroupDiskCache(GroupDiskCacheConfig config, PrometheusMetrics metrics) {
    this(config, metrics, System::currentTimeMillis);
  }

  public GroupDiskCache(GroupDiskCacheConfig config, PrometheusMetrics metrics, LongSupplier clock) {
    this(config, metrics, clock, DEFAULT_MAX_CACHE_FILE_BYTES);
  }

  GroupDiskCache(
      GroupDiskCacheConfig config,
      PrometheusMetrics metrics,
      LongSupplier clock,
      long maxCacheFileBytes
  ) {
    this.config = config != null ? config : GroupDiskCacheConfig.disabled();
    this.metrics = metrics;
    this.clock = clock != null ? clock : System::currentTimeMillis;
    this.maxCacheFileBytes = maxCacheFileBytes > 0L ? maxCacheFileBytes : DEFAULT_MAX_CACHE_FILE_BYTES;

    if (this.config.enabled()) {
      loadFromDisk();
      if (this.config.persistIntervalSeconds() > 0) {
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
          Thread thread = new Thread(r, "hms-proxy-group-disk-cache-flusher");
          thread.setDaemon(true);
          return thread;
        });
        this.scheduler.scheduleWithFixedDelay(
            this::persistIfDirty,
            this.config.persistIntervalSeconds(),
            this.config.persistIntervalSeconds(),
            TimeUnit.SECONDS
        );
      } else {
        this.scheduler = null;
      }
    } else {
      this.scheduler = null;
    }
  }

  public boolean isEnabled() {
    return config.enabled();
  }

  public int size() {
    return cache.size();
  }

  public Optional<List<String>> get(String userName) {
    if (!config.enabled() || userName == null) {
      return Optional.empty();
    }
    CachedUserGroups entry = cache.get(userName);
    if (entry == null) {
      recordMetric("miss");
      return Optional.empty();
    }

    long now = clock.getAsLong();
    if (isExpired(entry, now)) {
      recordMetric("miss");
      return Optional.empty();
    }

    recordMetric("hit");
    return Optional.of(entry.groups());
  }

  public Optional<List<String>> getStale(String userName) {
    if (!config.enabled() || userName == null) {
      return Optional.empty();
    }
    CachedUserGroups entry = cache.get(userName);
    return entry != null ? Optional.of(entry.groups()) : Optional.empty();
  }

  public void put(String userName, List<String> groups) {
    if (!config.enabled() || userName == null) {
      return;
    }
    List<String> normalizedGroups = groups == null ? List.of() : List.copyOf(groups);
    long now = clock.getAsLong();
    CachedUserGroups existing = cache.get(userName);
    if (existing != null && existing.groups().equals(normalizedGroups)) {
      if (!isExpired(existing, now)) {
        return;
      }
      // Refresh the expiration timestamp even when group membership did not change so the entry
      // does not stay permanently expired after its first TTL window.
      cache.put(userName, new CachedUserGroups(normalizedGroups, now));
      mutationGeneration.incrementAndGet();
      return;
    }

    if (existing == null) {
      makeRoomForNewUser();
    }
    cache.put(userName, new CachedUserGroups(normalizedGroups, now));
    mutationGeneration.incrementAndGet();
    updateEntriesMetric();
  }

  private boolean isExpired(CachedUserGroups entry, long nowMs) {
    return config.entryTtlSeconds() > 0 && nowMs - entry.updatedAtEpochMs() > config.entryTtlSeconds() * 1000L;
  }

  /**
   * User names reach this cache from clients (under SIMPLE auth a client names itself through
   * set_ugi), so it is capped: a full cache drops the entry refreshed longest ago. Racing writers
   * may overshoot the cap by the number of concurrent new users, never more.
   */
  private void makeRoomForNewUser() {
    while (cache.size() >= config.maxEntries()) {
      String oldestUser = null;
      long oldestUpdate = Long.MAX_VALUE;
      for (Map.Entry<String, CachedUserGroups> entry : cache.entrySet()) {
        if (entry.getValue().updatedAtEpochMs() < oldestUpdate) {
          oldestUpdate = entry.getValue().updatedAtEpochMs();
          oldestUser = entry.getKey();
        }
      }
      if (oldestUser == null || cache.remove(oldestUser) == null) {
        return;
      }
      recordMetric("evicted");
    }
  }

  public synchronized boolean persistToDisk() {
    if (!config.enabled()) {
      return false;
    }
    Path targetPath = Path.of(config.path());
    Path parentDir = targetPath.getParent();
    Path tempPath = null;

    try {
      if (parentDir != null && !Files.exists(parentDir)) {
        createPrivateDirectories(parentDir);
      }

      long targetGeneration = mutationGeneration.get();
      long now = clock.getAsLong();
      DiskCachePayload payload = new DiskCachePayload(
          DiskCachePayload.CURRENT_VERSION,
          now,
          Map.copyOf(cache)
      );

      byte[] bytes = MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(payload);
      String tempFileName = targetPath.getFileName().toString() + ".tmp." + UUID.randomUUID();
      tempPath = parentDir != null ? parentDir.resolve(tempFileName) : Path.of(tempFileName);

      writePrivateFile(tempPath, bytes);

      try {
        Files.move(tempPath, targetPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      } catch (AtomicMoveNotSupportedException e) {
        Files.move(tempPath, targetPath, StandardCopyOption.REPLACE_EXISTING);
      }
      tempPath = null;

      persistedGeneration.accumulateAndGet(targetGeneration, Math::max);
      if (metrics != null) {
        metrics.recordCacheRefresh("group_disk", "global", "success");
      }
      LOG.debug("Persisted {} user group entries to disk cache at '{}'", cache.size(), targetPath);
      return true;
    } catch (Exception e) {
      if (metrics != null) {
        metrics.recordCacheRefresh("group_disk", "global", "error");
      }
      LOG.warn("Failed to persist group disk cache to '{}': {}", targetPath, e.getMessage());
      return false;
    } finally {
      if (tempPath != null) {
        try {
          Files.deleteIfExists(tempPath);
        } catch (IOException cleanupError) {
          LOG.debug("Failed to clean up temporary group disk cache file '{}': {}",
              tempPath, cleanupError.getMessage());
        }
      }
    }
  }

  boolean isDirty() {
    return mutationGeneration.get() > persistedGeneration.get();
  }

  private void persistIfDirty() {
    if (isDirty()) {
      persistToDisk();
    }
  }

  private void loadFromDisk() {
    Path targetPath = Path.of(config.path());
    if (!Files.exists(targetPath)) {
      LOG.debug("Group disk cache file '{}' does not exist yet", targetPath);
      return;
    }

    try {
      long fileSize = Files.size(targetPath);
      if (fileSize > maxCacheFileBytes) {
        LOG.warn("Group disk cache file '{}' size ({} bytes) exceeds limit ({} bytes); ignoring cache file",
            targetPath, fileSize, maxCacheFileBytes);
        return;
      }
      byte[] bytes = Files.readAllBytes(targetPath);
      if (bytes.length == 0) {
        return;
      }
      DiskCachePayload payload = MAPPER.readValue(bytes, DiskCachePayload.class);
      if (payload == null) {
        return;
      }
      if (payload.version() != DiskCachePayload.CURRENT_VERSION) {
        LOG.warn("Unsupported group disk cache version {} in '{}' (expected {}); ignoring cache file",
            payload.version(), targetPath, DiskCachePayload.CURRENT_VERSION);
        return;
      }
      if (payload.entries() == null) {
        return;
      }

      long now = clock.getAsLong();
      long maxAgeMs = config.entryTtlSeconds() > 0 ? config.entryTtlSeconds() * 1000L : Long.MAX_VALUE;
      int loaded = 0;
      int expired = 0;

      // Newest first, so a file written under a larger cap (or by an older version without one)
      // loads the most recently refreshed users.
      List<Map.Entry<String, CachedUserGroups>> candidates = new ArrayList<>(payload.entries().entrySet());
      candidates.sort(Comparator.comparingLong(
          (Map.Entry<String, CachedUserGroups> e) -> e.getValue() == null ? Long.MIN_VALUE : e.getValue().updatedAtEpochMs())
          .reversed());
      for (Map.Entry<String, CachedUserGroups> entry : candidates) {
        String user = entry.getKey();
        CachedUserGroups value = entry.getValue();
        if (value != null && user != null && !user.isBlank()) {
          if (now - value.updatedAtEpochMs() > maxAgeMs) {
            expired++;
          } else if (cache.size() < config.maxEntries()) {
            cache.put(user, value);
            loaded++;
          }
        }
      }

      updateEntriesMetric();
      LOG.info("Loaded {} user group entries from disk cache '{}' (skipped {} expired entries)",
          loaded, targetPath, expired);
    } catch (Exception e) {
      LOG.warn("Failed to load group disk cache from '{}': {}", targetPath, e.getMessage());
    }
  }

  private static void createPrivateDirectories(Path dir) throws IOException {
    if (POSIX_SUPPORTED) {
      Files.createDirectories(dir, PosixFilePermissions.asFileAttribute(PRIVATE_DIR_PERMS));
      try {
        Files.setPosixFilePermissions(dir, PRIVATE_DIR_PERMS);
      } catch (UnsupportedOperationException | IOException ignored) {
        // Non-POSIX underlying store; best effort.
      }
    } else {
      Files.createDirectories(dir);
    }
  }

  private static void writePrivateFile(Path file, byte[] bytes) throws IOException {
    if (POSIX_SUPPORTED) {
      try {
        FileAttribute<Set<PosixFilePermission>> attrs =
            PosixFilePermissions.asFileAttribute(PRIVATE_FILE_PERMS);
        Files.createFile(file, attrs);
        Files.write(file, bytes, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        return;
      } catch (UnsupportedOperationException ignored) {
        // Fall through to standard write when store rejects POSIX attributes.
      }
    }
    Files.write(file, bytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
  }

  private void recordMetric(String result) {
    if (metrics != null) {
      metrics.recordCacheRequest("group_disk", "global", result);
    }
  }

  private void updateEntriesMetric() {
    if (metrics != null) {
      metrics.setCacheEntries("group_disk", "global", cache.size());
    }
  }

  @Override
  public void close() {
    if (scheduler != null) {
      scheduler.shutdown();
      try {
        if (!scheduler.awaitTermination(2, TimeUnit.SECONDS)) {
          scheduler.shutdownNow();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    if (config.enabled() && config.persistOnShutdown() && isDirty()) {
      persistToDisk();
    }
  }
}
