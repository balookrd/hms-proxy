package io.github.mmalykhin.hmsproxy.security.groups;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.mmalykhin.hmsproxy.config.security.GroupDiskCacheConfig;
import io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class UserGroupResolverTest {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void fastColdStartResolutionFromDisk() throws Exception {
    File cacheFile = temp.newFile("cold-start-groups.json");
    long now = 1_000_000L;
    DiskCachePayload payload = new DiskCachePayload(
        1,
        now,
        Map.of(
            "alice", new CachedUserGroups(List.of("data_engineering", "admin"), now),
            "bob", new CachedUserGroups(List.of("bi_users"), now)
        )
    );
    MAPPER.writeValue(cacheFile, payload);

    GroupDiskCacheConfig config = new GroupDiskCacheConfig(
        true, cacheFile.getAbsolutePath(), 86400L, 0L, true);
    PrometheusMetrics metrics = new PrometheusMetrics();

    try (GroupDiskCache diskCache = new GroupDiskCache(config, metrics, () -> now + 500L);
         UserGroupResolver resolver = new UserGroupResolver(diskCache)) {
      List<String> aliceGroups = resolver.resolveGroups("alice");
      Assert.assertEquals(List.of("data_engineering", "admin"), aliceGroups);

      List<String> bobGroups = resolver.resolveGroups("bob");
      Assert.assertEquals(List.of("bi_users"), bobGroups);
    }
  }

  @Test
  public void populatesDiskCacheOnResolution() throws Exception {
    File cacheFile = new File(temp.getRoot(), "populate-groups.json");
    GroupDiskCacheConfig config = new GroupDiskCacheConfig(
        true, cacheFile.getAbsolutePath(), 86400L, 0L, true);
    PrometheusMetrics metrics = new PrometheusMetrics();

    try (GroupDiskCache diskCache = new GroupDiskCache(config, metrics);
         UserGroupResolver resolver = new UserGroupResolver(diskCache)) {
      Assert.assertEquals(0, diskCache.size());

      // Resolving any user (e.g. current or nonexistent) populates the cache
      UserGroupInformation ugi = UserGroupInformation.createUserForTesting("charlie", new String[]{"sales", "marketing"});
      UserGroupInformation.setLoginUser(ugi);
      try {
        List<String> groups = resolver.resolveGroups("charlie");
        Assert.assertEquals(List.of("sales", "marketing"), groups);
        Assert.assertEquals(1, diskCache.size());
        Assert.assertEquals(List.of("sales", "marketing"), diskCache.get("charlie").orElse(List.of()));
      } finally {
        UserGroupInformation.setLoginUser(null);
      }
    }
  }

  @Test
  public void resolvesEmptyWhenUserBlankOrNull() {
    UserGroupResolver resolver = new UserGroupResolver();
    Assert.assertTrue(resolver.resolveGroups(null).isEmpty());
    Assert.assertTrue(resolver.resolveGroups("").isEmpty());
    Assert.assertTrue(resolver.resolveGroups("   ").isEmpty());
  }

  @Test
  public void lookupFailureServesStaleCacheWithoutPoisoningDiskCache() throws Exception {
    File cacheFile = temp.newFile("stale-fallback-groups.json");
    AtomicLong clock = new AtomicLong(100_000L);
    GroupDiskCacheConfig config = new GroupDiskCacheConfig(
        true, cacheFile.getAbsolutePath(), 60L, 0L, true);
    try (GroupDiskCache diskCache = new GroupDiskCache(config, new PrometheusMetrics(), clock::get);
         UserGroupResolver resolver = new UserGroupResolver(diskCache, userName -> {
           throw new IOException("LDAP connection refused");
         })) {
      diskCache.put("alice", List.of("sales", "finance"));
      clock.addAndGet(61_000L);
      Assert.assertEquals(Optional.empty(), diskCache.get("alice"));

      List<String> resolved = resolver.resolveGroups("alice");

      Assert.assertEquals(List.of("sales", "finance"), resolved);
      Assert.assertEquals(Optional.of(List.of("sales", "finance")), diskCache.getStale("alice"));
    }
  }

  @Test
  public void concurrentCacheMissesForSameUserShareSingleDirectoryLookup() throws Exception {
    File cacheFile = temp.newFile("singleflight-groups.json");
    GroupDiskCacheConfig config = new GroupDiskCacheConfig(
        true, cacheFile.getAbsolutePath(), 3600L, 0L, true);
    AtomicInteger lookupCalls = new AtomicInteger();
    CountDownLatch lookupEntered = new CountDownLatch(1);
    CountDownLatch releaseLookup = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(4);
    try (GroupDiskCache diskCache = new GroupDiskCache(config, new PrometheusMetrics());
         UserGroupResolver resolver = new UserGroupResolver(diskCache, userName -> {
           lookupCalls.incrementAndGet();
           lookupEntered.countDown();
           try {
             if (!releaseLookup.await(5, TimeUnit.SECONDS)) {
               throw new IOException("Timed out waiting in test");
             }
           } catch (InterruptedException e) {
             Thread.currentThread().interrupt();
             throw new IOException("Interrupted waiting in test", e);
           }
           return List.of("analytics");
         })) {
      List<Future<List<String>>> futures = new ArrayList<>();
      for (int i = 0; i < 4; i++) {
        futures.add(pool.submit(() -> resolver.resolveGroups("alice")));
      }
      Assert.assertTrue(lookupEntered.await(5, TimeUnit.SECONDS));
      Thread.sleep(100L);
      releaseLookup.countDown();
      for (Future<List<String>> future : futures) {
        Assert.assertEquals(List.of("analytics"), future.get(5, TimeUnit.SECONDS));
      }
      Assert.assertEquals(1, lookupCalls.get());
    } finally {
      pool.shutdownNow();
    }
  }
}
