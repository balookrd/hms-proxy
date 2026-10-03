package io.github.mmalykhin.hmsproxy.security.groups;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.mmalykhin.hmsproxy.config.security.GroupDiskCacheConfig;
import io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics;
import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class GroupDiskCacheTest {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void disabledCacheReturnsEmptyAndDoesNotPersist() {
    GroupDiskCacheConfig config = GroupDiskCacheConfig.disabled();
    PrometheusMetrics metrics = new PrometheusMetrics();
    try (GroupDiskCache cache = new GroupDiskCache(config, metrics)) {
      Assert.assertFalse(cache.isEnabled());
      Assert.assertEquals(0, cache.size());

      cache.put("alice", List.of("sales"));
      Assert.assertEquals(0, cache.size());
      Assert.assertEquals(Optional.empty(), cache.get("alice"));
      Assert.assertFalse(cache.persistToDisk());
    }
  }

  @Test
  public void loadsValidEntriesFromDiskOnStartup() throws Exception {
    File cacheFile = temp.newFile("group-cache.json");
    long now = 1_000_000L;
    DiskCachePayload payload = new DiskCachePayload(
        1,
        now,
        Map.of(
            "alice", new CachedUserGroups(List.of("analytics", "bi"), now),
            "bob", new CachedUserGroups(List.of("sales"), now)
        )
    );
    MAPPER.writeValue(cacheFile, payload);

    GroupDiskCacheConfig config = new GroupDiskCacheConfig(
        true, cacheFile.getAbsolutePath(), 3600L, 0L, true);
    PrometheusMetrics metrics = new PrometheusMetrics();

    try (GroupDiskCache cache = new GroupDiskCache(config, metrics, () -> now + 100L)) {
      Assert.assertTrue(cache.isEnabled());
      Assert.assertEquals(2, cache.size());

      Optional<List<String>> aliceGroups = cache.get("alice");
      Assert.assertTrue(aliceGroups.isPresent());
      Assert.assertEquals(List.of("analytics", "bi"), aliceGroups.get());

      Optional<List<String>> bobGroups = cache.get("bob");
      Assert.assertTrue(bobGroups.isPresent());
      Assert.assertEquals(List.of("sales"), bobGroups.get());

      Assert.assertEquals(Optional.empty(), cache.get("charlie"));
    }
  }

  @Test
  public void filtersExpiredEntriesOnStartup() throws Exception {
    File cacheFile = temp.newFile("group-cache-expired.json");
    long now = 2_000_000L;
    long oldTime = now - 5_000_000L; // 5000 seconds ago
    DiskCachePayload payload = new DiskCachePayload(
        1,
        now,
        Map.of(
            "alice", new CachedUserGroups(List.of("analytics"), now),
            "bob", new CachedUserGroups(List.of("sales"), oldTime)
        )
    );
    MAPPER.writeValue(cacheFile, payload);

    // TTL is 300 seconds (300_000 ms)
    GroupDiskCacheConfig config = new GroupDiskCacheConfig(
        true, cacheFile.getAbsolutePath(), 300L, 0L, true);
    PrometheusMetrics metrics = new PrometheusMetrics();

    try (GroupDiskCache cache = new GroupDiskCache(config, metrics, () -> now)) {
      Assert.assertEquals(1, cache.size());
      Assert.assertTrue(cache.get("alice").isPresent());
      Assert.assertEquals(Optional.empty(), cache.get("bob"));
    }
  }

  @Test
  public void returnsEmptyOnExpiredEntryWhilePreservingStale() throws Exception {
    File cacheFile = temp.newFile("group-cache-evict.json");
    AtomicLong clock = new AtomicLong(100_000L);
    GroupDiskCacheConfig config = new GroupDiskCacheConfig(
        true, cacheFile.getAbsolutePath(), 60L, 0L, true);
    PrometheusMetrics metrics = new PrometheusMetrics();

    try (GroupDiskCache cache = new GroupDiskCache(config, metrics, clock::get)) {
      cache.put("alice", List.of("engineers"));
      Assert.assertEquals(1, cache.size());
      Assert.assertTrue(cache.get("alice").isPresent());

      // Advance clock by 61 seconds
      clock.addAndGet(61_000L);

      Assert.assertEquals(Optional.empty(), cache.get("alice"));
      Assert.assertEquals(1, cache.size());
      // But stale record remains accessible via getStale
      Assert.assertEquals(Optional.of(List.of("engineers")), cache.getStale("alice"));
    }
  }

  @Test
  public void persistsToDiskAndReloadsInNewInstance() throws Exception {
    File cacheDir = temp.newFolder("sub", "cache");
    File cacheFile = new File(cacheDir, "groups.json");

    GroupDiskCacheConfig config = new GroupDiskCacheConfig(
        true, cacheFile.getAbsolutePath(), 86400L, 0L, true);
    PrometheusMetrics metrics = new PrometheusMetrics();
    long now = 500_000L;

    try (GroupDiskCache cache = new GroupDiskCache(config, metrics, () -> now)) {
      cache.put("user1", List.of("groupA", "groupB"));
      cache.put("user2", List.of("groupC"));
      boolean persisted = cache.persistToDisk();
      Assert.assertTrue(persisted);
      Assert.assertTrue(cacheFile.exists());
    }

    // Load from disk in a fresh instance
    try (GroupDiskCache reloaded = new GroupDiskCache(config, metrics, () -> now + 1000L)) {
      Assert.assertEquals(2, reloaded.size());
      Assert.assertEquals(Optional.of(List.of("groupA", "groupB")), reloaded.get("user1"));
      Assert.assertEquals(Optional.of(List.of("groupC")), reloaded.get("user2"));
    }
  }

  @Test
  public void persistsOnCloseWhenDirty() throws Exception {
    File cacheFile = new File(temp.getRoot(), "shutdown-cache.json");
    GroupDiskCacheConfig config = new GroupDiskCacheConfig(
        true, cacheFile.getAbsolutePath(), 86400L, 0L, true);
    PrometheusMetrics metrics = new PrometheusMetrics();

    GroupDiskCache cache = new GroupDiskCache(config, metrics, () -> 12345L);
    cache.put("david", List.of("finance"));
    Assert.assertFalse("File must not exist before persist/close", cacheFile.exists());

    cache.close();
    Assert.assertTrue("File must exist after close when persistOnShutdown=true", cacheFile.exists());

    try (GroupDiskCache reloaded = new GroupDiskCache(config, metrics, () -> 12345L)) {
      Assert.assertEquals(Optional.of(List.of("finance")), reloaded.get("david"));
    }
  }

  @Test
  public void handlesCorruptedDiskFileGracefully() throws Exception {
    File cacheFile = temp.newFile("corrupt.json");
    Files.writeString(cacheFile.toPath(), "{ not valid json at all ... !!!");

    GroupDiskCacheConfig config = new GroupDiskCacheConfig(
        true, cacheFile.getAbsolutePath(), 3600L, 0L, true);
    PrometheusMetrics metrics = new PrometheusMetrics();

    try (GroupDiskCache cache = new GroupDiskCache(config, metrics, () -> 1000L)) {
      Assert.assertTrue(cache.isEnabled());
      Assert.assertEquals(0, cache.size());

      // Can continue normal operations
      cache.put("alice", List.of("dev"));
      Assert.assertEquals(Optional.of(List.of("dev")), cache.get("alice"));
    }
  }
}
