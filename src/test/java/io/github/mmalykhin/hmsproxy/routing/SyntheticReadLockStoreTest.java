package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.compatibility.CompatibilityConfig;
import io.github.mmalykhin.hmsproxy.config.management.ManagementConfig;
import io.github.mmalykhin.hmsproxy.config.routing.BackendConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityMode;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;
import java.util.List;
import java.util.Map;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.Assert;
import org.junit.Test;
import static io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreProxyTestSupport.catalogConfig;
import static io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreProxyTestSupport.startTestingServerOrSkip;
import static io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreProxyTestSupport.syntheticReadLockStoreConfig;

/**
 * Store-level tests for the synthetic read-lock state stores. The ZooKeeper cases assert the
 * txn index layout directly because it is what keeps commit_txn off the O(N) scan path.
 */
public class SyntheticReadLockStoreTest {
  private static final String LOCKS_ROOT = "/hms-proxy-test-synthetic-read-locks/locks";
  private static final String TXN_INDEX_ROOT = "/hms-proxy-test-synthetic-read-locks/txns";

  @Test
  public void inMemoryStoreReleasesOnlyLocksOfRequestedTxn() throws Exception {
    try (InMemorySyntheticReadLockStore store = new InMemorySyntheticReadLockStore()) {
      SyntheticReadLockManager.SyntheticLockState first = create(store, 11L, 1_000L);
      SyntheticReadLockManager.SyntheticLockState second = create(store, 11L, 1_000L);
      SyntheticReadLockManager.SyntheticLockState other = create(store, 12L, 1_000L);

      SyntheticReadLockStore.ReleaseSummary summary = store.releaseTxn(11L, "instance-a");

      Assert.assertEquals(2L, summary.releasedCount());
      Assert.assertEquals(0L, summary.remoteOwnerCount());
      Assert.assertNull(store.get(first.lockId()));
      Assert.assertNull(store.get(second.lockId()));
      Assert.assertNotNull(store.get(other.lockId()));
    }
  }

  @Test
  public void inMemoryStoreKeepsLockThatWasRenewedConcurrently() throws Exception {
    try (InMemorySyntheticReadLockStore store = new InMemorySyntheticReadLockStore()) {
      SyntheticReadLockManager.SyntheticLockState state = create(store, 21L, 1_000L);
      store.touch(state.lockId(), 10_000L);

      SyntheticReadLockManager.SyntheticLockState refreshed =
          store.releaseIfExpired(state.lockId(), 10_500L, 5_000L);

      Assert.assertNotNull(refreshed);
      Assert.assertEquals(10_000L, refreshed.lastTouchedAtMs());
      Assert.assertNotNull(store.get(state.lockId()));
    }
  }

  @Test
  public void inMemoryStoreRemovesExpiredLockAndTxnIndexEntry() throws Exception {
    try (InMemorySyntheticReadLockStore store = new InMemorySyntheticReadLockStore()) {
      SyntheticReadLockManager.SyntheticLockState state = create(store, 22L, 1_000L);

      Assert.assertNull(store.releaseIfExpired(state.lockId(), 10_000L, 5_000L));
      Assert.assertNull(store.get(state.lockId()));
      Assert.assertEquals(0L, store.releaseTxn(22L, "instance-a").releasedCount());
      Assert.assertEquals(0L, store.activeLockCount());
    }
  }

  @Test
  public void inMemoryCleanupReportsRemainingActiveLocks() throws Exception {
    try (InMemorySyntheticReadLockStore store = new InMemorySyntheticReadLockStore()) {
      SyntheticReadLockManager.SyntheticLockState expired = create(store, 31L, 1_000L);
      SyntheticReadLockManager.SyntheticLockState alive = create(store, 32L, 9_000L);

      SyntheticReadLockStore.CleanupSummary summary = store.cleanupExpiredLocks(10_000L, 5_000L, "instance-a");

      Assert.assertEquals(1L, summary.expiredCount());
      Assert.assertEquals(1L, summary.activeCount());
      Assert.assertNull(store.get(expired.lockId()));
      Assert.assertNotNull(store.get(alive.lockId()));
    }
  }

  @Test
  public void zooKeeperStoreIndexesLocksByTxnId() throws Exception {
    try (TestingServer zooKeeper = startTestingServerOrSkip();
         CuratorFramework probe = newProbe(zooKeeper.getConnectString());
         ZooKeeperSyntheticReadLockStore store = newZooKeeperStore(zooKeeper.getConnectString())) {
      SyntheticReadLockManager.SyntheticLockState first = create(store, 41L, 1_000L);
      SyntheticReadLockManager.SyntheticLockState second = create(store, 41L, 1_000L);
      SyntheticReadLockManager.SyntheticLockState other = create(store, 42L, 1_000L);

      List<String> indexed = probe.getChildren().forPath(TXN_INDEX_ROOT + "/41");
      Assert.assertEquals(2, indexed.size());
      Assert.assertTrue(indexed.contains(Long.toString(first.lockId())));
      Assert.assertTrue(indexed.contains(Long.toString(second.lockId())));

      SyntheticReadLockStore.ReleaseSummary summary = store.releaseTxn(41L, "instance-a");

      Assert.assertEquals(2L, summary.releasedCount());
      Assert.assertNull(store.get(first.lockId()));
      Assert.assertNull(store.get(second.lockId()));
      Assert.assertNotNull(store.get(other.lockId()));
      Assert.assertNull(probe.checkExists().forPath(TXN_INDEX_ROOT + "/41"));
      Assert.assertNotNull(probe.checkExists().forPath(TXN_INDEX_ROOT + "/42"));
    }
  }

  @Test
  public void zooKeeperReleaseTxnWithoutSyntheticLocksDoesNotTouchLockNodes() throws Exception {
    try (TestingServer zooKeeper = startTestingServerOrSkip();
         CuratorFramework probe = newProbe(zooKeeper.getConnectString());
         ZooKeeperSyntheticReadLockStore store = newZooKeeperStore(zooKeeper.getConnectString())) {
      SyntheticReadLockManager.SyntheticLockState unrelated = create(store, 51L, 1_000L);

      SyntheticReadLockStore.ReleaseSummary summary = store.releaseTxn(52L, "instance-a");

      Assert.assertEquals(0L, summary.releasedCount());
      Assert.assertEquals(0L, summary.remoteOwnerCount());
      Assert.assertNotNull(store.get(unrelated.lockId()));
      Assert.assertEquals(1, probe.getChildren().forPath(LOCKS_ROOT).size());
    }
  }

  @Test
  public void zooKeeperReleaseLockDropsTxnIndexEntry() throws Exception {
    try (TestingServer zooKeeper = startTestingServerOrSkip();
         CuratorFramework probe = newProbe(zooKeeper.getConnectString());
         ZooKeeperSyntheticReadLockStore store = newZooKeeperStore(zooKeeper.getConnectString())) {
      SyntheticReadLockManager.SyntheticLockState state = create(store, 61L, 1_000L);

      store.releaseLock(state);

      Assert.assertNull(store.get(state.lockId()));
      Assert.assertNull(probe.checkExists().forPath(TXN_INDEX_ROOT + "/61/" + state.lockId()));
      Assert.assertEquals(0L, store.releaseTxn(61L, "instance-a").releasedCount());
    }
  }

  @Test
  public void zooKeeperExpiryDoesNotDeleteLockRenewedByOwner() throws Exception {
    try (TestingServer zooKeeper = startTestingServerOrSkip();
         ZooKeeperSyntheticReadLockStore store = newZooKeeperStore(zooKeeper.getConnectString())) {
      SyntheticReadLockManager.SyntheticLockState state = create(store, 71L, 1_000L);
      // Owner heartbeat lands after another worker already observed the stale state.
      store.touch(state.lockId(), 10_000L);

      SyntheticReadLockManager.SyntheticLockState refreshed =
          store.releaseIfExpired(state.lockId(), 10_500L, 5_000L);

      Assert.assertNotNull(refreshed);
      Assert.assertEquals(10_000L, refreshed.lastTouchedAtMs());
      Assert.assertNotNull(store.get(state.lockId()));
    }
  }

  @Test
  public void zooKeeperExpiryRemovesStaleLockAndIndexEntry() throws Exception {
    try (TestingServer zooKeeper = startTestingServerOrSkip();
         CuratorFramework probe = newProbe(zooKeeper.getConnectString());
         ZooKeeperSyntheticReadLockStore store = newZooKeeperStore(zooKeeper.getConnectString())) {
      SyntheticReadLockManager.SyntheticLockState state = create(store, 72L, 1_000L);

      Assert.assertNull(store.releaseIfExpired(state.lockId(), 10_000L, 5_000L));
      Assert.assertNull(store.get(state.lockId()));
      Assert.assertNull(probe.checkExists().forPath(TXN_INDEX_ROOT + "/72/" + state.lockId()));
    }
  }

  @Test
  public void zooKeeperCleanupExpiresLocksAndCollectsOrphanIndexEntries() throws Exception {
    try (TestingServer zooKeeper = startTestingServerOrSkip();
         CuratorFramework probe = newProbe(zooKeeper.getConnectString());
         ZooKeeperSyntheticReadLockStore store = newZooKeeperStore(zooKeeper.getConnectString())) {
      SyntheticReadLockManager.SyntheticLockState expired = create(store, 81L, 1_000L);
      SyntheticReadLockManager.SyntheticLockState alive = create(store, 82L, 9_500L);
      // Index entry left behind by a proxy that died between the lock delete and the index delete.
      probe.create().creatingParentsIfNeeded().forPath(TXN_INDEX_ROOT + "/83/" + (alive.lockId() + 1000L));

      SyntheticReadLockStore.CleanupSummary summary = store.cleanupExpiredLocks(10_000L, 5_000L, "instance-a");

      Assert.assertEquals(1L, summary.expiredCount());
      Assert.assertEquals(1L, summary.activeCount());
      Assert.assertNull(store.get(expired.lockId()));
      Assert.assertNotNull(store.get(alive.lockId()));
      Assert.assertNull(probe.checkExists().forPath(TXN_INDEX_ROOT + "/81"));
      Assert.assertNull(probe.checkExists().forPath(TXN_INDEX_ROOT + "/83"));
      Assert.assertNotNull(probe.checkExists().forPath(TXN_INDEX_ROOT + "/82/" + alive.lockId()));
    }
  }

  @Test
  public void zooKeeperStoreSurvivesLocksCreatedByPreviousLayout() throws Exception {
    try (TestingServer zooKeeper = startTestingServerOrSkip();
         CuratorFramework probe = newProbe(zooKeeper.getConnectString());
         ZooKeeperSyntheticReadLockStore store = newZooKeeperStore(zooKeeper.getConnectString())) {
      SyntheticReadLockManager.SyntheticLockState legacy = create(store, 101L, 1_000L);
      // Simulate a lock written by a proxy that predates the txn index.
      probe.delete().forPath(TXN_INDEX_ROOT + "/101/" + legacy.lockId());

      Assert.assertEquals(0L, store.releaseTxn(101L, "instance-a").releasedCount());
      Assert.assertNotNull(store.get(legacy.lockId()));

      SyntheticReadLockStore.CleanupSummary summary = store.cleanupExpiredLocks(10_000L, 5_000L, "instance-a");

      Assert.assertEquals(1L, summary.expiredCount());
      Assert.assertEquals(0L, summary.activeCount());
      Assert.assertNull(store.get(legacy.lockId()));
    }
  }

  private static SyntheticReadLockManager.SyntheticLockState create(
      SyntheticReadLockStore store,
      long txnId,
      long createdAtMs
  ) throws Exception {
    return store.create(txnId, "catalog2", "sales", "catalog2__sales", "instance-a", createdAtMs);
  }

  private static ZooKeeperSyntheticReadLockStore newZooKeeperStore(String connectString) throws Exception {
    return new ZooKeeperSyntheticReadLockStore(zooKeeperConfig(connectString));
  }

  private static CuratorFramework newProbe(String connectString) {
    CuratorFramework probe = CuratorFrameworkFactory.builder()
        .connectString(connectString)
        .connectionTimeoutMs(15_000)
        .sessionTimeoutMs(60_000)
        .retryPolicy(new ExponentialBackoffRetry(250, 3))
        .build();
    probe.start();
    return probe;
  }

  static ProxyConfig zooKeeperConfig(String connectString) {
    return storeConfig(syntheticReadLockStoreConfig(connectString), Map.of());
  }

  static ProxyConfig storeConfig(SyntheticReadLockStoreConfig storeConfig, Map<String, String> defaultCatalogHiveConf) {
    return ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, defaultCatalogHiveConf)))
        .backend(new BackendConfig(Map.of()))
        .compatibility(new CompatibilityConfig(false))
        .management(new ManagementConfig(false, "127.0.0.1", 10083))
        .syntheticReadLockStore(storeConfig)
        .build();
  }
}
