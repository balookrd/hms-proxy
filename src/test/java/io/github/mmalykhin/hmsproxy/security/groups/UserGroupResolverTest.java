package io.github.mmalykhin.hmsproxy.security.groups;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.mmalykhin.hmsproxy.config.security.GroupDiskCacheConfig;
import io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics;
import java.io.File;
import java.util.List;
import java.util.Map;
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
}
