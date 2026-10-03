package io.github.mmalykhin.hmsproxy.security.groups;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.mmalykhin.hmsproxy.config.security.GroupDiskCacheConfig;
import io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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

  private final GroupDiskCacheConfig config;
  private final PrometheusMetrics metrics;
  private final LongSupplier clock;
  private final ConcurrentHashMap<String, CachedUserGroups> cache = new ConcurrentHashMap<>();
  private final AtomicBoolean dirty = new AtomicBoolean(false);
  private final ScheduledExecutorService scheduler;

  public GroupDiskCache(GroupDiskCacheConfig config, PrometheusMetrics metrics) {
    this(config, metrics, System::currentTimeMillis);
  }

  public GroupDiskCache(GroupDiskCacheConfig config, PrometheusMetrics metrics, LongSupplier clock) {
    this.config = config != null ? config : GroupDiskCacheConfig.disabled();
    this.metrics = metrics;
    this.clock = clock != null ? clock : System::currentTimeMillis;

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
    if (config.entryTtlSeconds() > 0) {
      long maxAgeMs = config.entryTtlSeconds() * 1000L;
      if (now - entry.updatedAtEpochMs() > maxAgeMs) {
        recordMetric("miss");
        return Optional.empty();
      }
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
    CachedUserGroups existing = cache.get(userName);
    if (existing != null && existing.groups().equals(normalizedGroups)) {
      return;
    }

    cache.put(userName, new CachedUserGroups(normalizedGroups, clock.getAsLong()));
    dirty.set(true);
    updateEntriesMetric();
  }

  public synchronized boolean persistToDisk() {
    if (!config.enabled()) {
      return false;
    }
    Path targetPath = Path.of(config.path());
    Path parentDir = targetPath.getParent();

    try {
      if (parentDir != null && !Files.exists(parentDir)) {
        Files.createDirectories(parentDir);
      }

      long now = clock.getAsLong();
      DiskCachePayload payload = new DiskCachePayload(
          DiskCachePayload.CURRENT_VERSION,
          now,
          Map.copyOf(cache)
      );

      byte[] bytes = MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(payload);
      String tempFileName = targetPath.getFileName().toString() + ".tmp." + UUID.randomUUID();
      Path tempPath = parentDir != null ? parentDir.resolve(tempFileName) : Path.of(tempFileName);

      Files.write(tempPath, bytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);

      try {
        Files.move(tempPath, targetPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      } catch (AtomicMoveNotSupportedException e) {
        Files.move(tempPath, targetPath, StandardCopyOption.REPLACE_EXISTING);
      }

      dirty.set(false);
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
    }
  }

  private void persistIfDirty() {
    if (dirty.get()) {
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
      byte[] bytes = Files.readAllBytes(targetPath);
      if (bytes.length == 0) {
        return;
      }
      DiskCachePayload payload = MAPPER.readValue(bytes, DiskCachePayload.class);
      if (payload == null || payload.entries() == null) {
        return;
      }

      long now = clock.getAsLong();
      long maxAgeMs = config.entryTtlSeconds() > 0 ? config.entryTtlSeconds() * 1000L : Long.MAX_VALUE;
      int loaded = 0;
      int expired = 0;

      for (Map.Entry<String, CachedUserGroups> entry : payload.entries().entrySet()) {
        String user = entry.getKey();
        CachedUserGroups value = entry.getValue();
        if (value != null && user != null && !user.isBlank()) {
          if (now - value.updatedAtEpochMs() <= maxAgeMs) {
            cache.put(user, value);
            loaded++;
          } else {
            expired++;
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

    if (config.enabled() && config.persistOnShutdown() && dirty.get()) {
      persistToDisk();
    }
  }
}
