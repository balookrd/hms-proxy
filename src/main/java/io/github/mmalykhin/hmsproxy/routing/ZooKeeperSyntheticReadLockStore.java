package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.security.KerberosPrincipalUtil;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreZooKeeperConfig;

/**
 * ZooKeeper-backed synthetic read-lock state, laid out as two subtrees under the configured znode:
 *
 * <ul>
 *   <li>{@code <root>/locks/lock-<sequence>} holds the serialized state and is keyed by lock id,
 *       because check_lock/unlock/heartbeat only carry a lock id;</li>
 *   <li>{@code <root>/txns/<txnId>/<lockId>} is an empty pointer node that lets commit_txn and
 *       abort_txn find the locks of one transaction without scanning every live lock.</li>
 * </ul>
 *
 * Lock nodes are always created before their index entry, so an index entry whose lock node is
 * missing is provably an orphan and can be collected by the sweep.
 */
final class ZooKeeperSyntheticReadLockStore implements SyntheticReadLockStore {
  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperSyntheticReadLockStore.class);
  private static final int SERIALIZATION_VERSION = 1;
  private static final int MAX_CAS_ATTEMPTS = 8;

  private final CuratorFramework client;
  private final String locksRootPath;
  private final String txnIndexRootPath;

  ZooKeeperSyntheticReadLockStore(ProxyConfig config) throws Exception {
    SyntheticReadLockStoreZooKeeperConfig zooKeeper = config.syntheticReadLockStore().zooKeeper();
    configureSecurity(config);
    this.client = CuratorFrameworkFactory.builder()
        .connectString(zooKeeper.connectString())
        .connectionTimeoutMs(zooKeeper.connectionTimeoutMs())
        .sessionTimeoutMs(zooKeeper.sessionTimeoutMs())
        .retryPolicy(new ExponentialBackoffRetry(zooKeeper.baseSleepMs(), zooKeeper.maxRetries()))
        .build();
    this.locksRootPath = normalizedZnode(zooKeeper.znode()) + "/locks";
    this.txnIndexRootPath = normalizedZnode(zooKeeper.znode()) + "/txns";
    client.start();
    if (!client.blockUntilConnected(zooKeeper.connectionTimeoutMs(), TimeUnit.MILLISECONDS)) {
      client.close();
      throw new IOException("Timed out connecting to ZooKeeper synthetic read-lock store at "
          + zooKeeper.connectString());
    }
    createPathIfMissing(locksRootPath);
    createPathIfMissing(txnIndexRootPath);
    LOG.info("Synthetic read-lock store started in ZooKeeper mode with connectString='{}', znode='{}'",
        zooKeeper.connectString(), normalizedZnode(zooKeeper.znode()));
  }

  @Override
  public SyntheticReadLockManager.SyntheticLockState create(
      long txnId,
      String catalogName,
      String backendDbName,
      String externalDbName,
      String ownerInstanceId,
      long createdAtMs
  ) throws Exception {
    SyntheticReadLockManager.SyntheticLockState provisional = new SyntheticReadLockManager.SyntheticLockState(
        0L,
        txnId,
        catalogName,
        backendDbName,
        externalDbName,
        ownerInstanceId,
        createdAtMs);
    String createdPath = client.create()
        .creatingParentContainersIfNeeded()
        .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
        .forPath(locksRootPath + "/lock-", serialize(provisional));
    long lockId = SyntheticReadLockManager.lockIdForSequence(parseSequence(createdPath));
    SyntheticReadLockManager.SyntheticLockState state = provisional.withLockId(lockId);
    // Publish the txn index entry before the lock becomes visible, so a commit that races with
    // this create can never miss the lock it is supposed to release.
    createTxnIndexEntry(txnId, lockId);
    // version(0): the node was just created, so its ZK version is guaranteed to be 0.
    client.setData().withVersion(0).forPath(createdPath, serialize(state));
    return state;
  }

  @Override
  public SyntheticReadLockManager.SyntheticLockState get(long lockId) throws Exception {
    String path = lockPath(lockId);
    try {
      SyntheticReadLockManager.SyntheticLockState state = deserialize(client.getData().forPath(path));
      // Guard against provisional nodes left by a proxy that crashed between create() and setData().
      // Such nodes contain lockId=0 and must be treated as non-existent.
      if (state.lockId() != lockId) {
        return null;
      }
      return state;
    } catch (KeeperException.NoNodeException ignored) {
      return null;
    }
  }

  @Override
  public void touch(long lockId, long nowMs) throws Exception {
    String path = lockPath(lockId);
    for (int attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
      Stat stat = new Stat();
      byte[] data;
      try {
        data = client.getData().storingStatIn(stat).forPath(path);
      } catch (KeeperException.NoNodeException ignored) {
        return;
      }
      SyntheticReadLockManager.SyntheticLockState current = deserialize(data);
      SyntheticReadLockManager.SyntheticLockState updated = current.touched(nowMs);
      try {
        client.setData().withVersion(stat.getVersion()).forPath(path, serialize(updated));
        return;
      } catch (KeeperException.BadVersionException ignored) {
        // Another proxy already refreshed the same synthetic lock; retry with the new version.
      }
    }
    LOG.warn("Synthetic read lock {} heartbeat was not persisted after {} attempts due to concurrent updates",
        lockId, MAX_CAS_ATTEMPTS);
  }

  @Override
  public void releaseLock(SyntheticReadLockManager.SyntheticLockState state) throws Exception {
    if (state == null) {
      return;
    }
    deleteIfPresent(lockPath(state.lockId()));
    deleteTxnIndexEntry(state.txnId(), state.lockId());
  }

  @Override
  public SyntheticReadLockManager.SyntheticLockState releaseIfExpired(long lockId, long nowMs, long timeoutMs)
      throws Exception {
    String path = lockPath(lockId);
    for (int attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
      Stat stat = new Stat();
      byte[] data;
      try {
        data = client.getData().storingStatIn(stat).forPath(path);
      } catch (KeeperException.NoNodeException ignored) {
        return null;
      }
      SyntheticReadLockManager.SyntheticLockState state = deserialize(data);
      if (state.lockId() != lockId) {
        // Provisional node left by a proxy that crashed mid-create; the sweep collects it.
        return null;
      }
      if (!state.isExpired(nowMs, timeoutMs)) {
        return state;
      }
      try {
        // Version check: a heartbeat that renewed this lock bumps the version and wins the race.
        client.delete().withVersion(stat.getVersion()).forPath(path);
      } catch (KeeperException.NoNodeException ignored) {
        deleteTxnIndexEntry(state.txnId(), lockId);
        return null;
      } catch (KeeperException.BadVersionException ignored) {
        continue;
      }
      deleteTxnIndexEntry(state.txnId(), lockId);
      return null;
    }
    LOG.warn("Synthetic read lock {} expiry gave up after {} concurrent updates", lockId, MAX_CAS_ATTEMPTS);
    return get(lockId);
  }

  @Override
  public ReleaseSummary releaseTxn(long txnId, String currentInstanceId) throws Exception {
    if (txnId <= 0) {
      return new ReleaseSummary(0L, 0L);
    }
    String txnPath = txnIndexPath(txnId);
    long releasedCount = 0L;
    long remoteOwnerCount = 0L;
    for (String child : childrenOf(txnPath)) {
      long lockId = parseIndexedLockId(child);
      if (lockId <= 0) {
        deleteIfPresent(txnPath + "/" + child);
        continue;
      }
      SyntheticReadLockManager.SyntheticLockState state = readState(lockPath(lockId));
      if (state != null && state.lockId() != lockId) {
        // Provisional node: the create is still in flight, so leave both nodes to the sweep.
        continue;
      }
      if (state != null && state.txnId() == txnId) {
        releasedCount++;
        if (!state.ownerInstanceId().equals(currentInstanceId)) {
          remoteOwnerCount++;
        }
        deleteIfPresent(lockPath(lockId));
      }
      deleteIfPresent(txnPath + "/" + child);
    }
    deleteTxnIndexParentIfEmpty(txnPath);
    return new ReleaseSummary(releasedCount, remoteOwnerCount);
  }

  @Override
  public CleanupSummary cleanupExpiredLocks(long nowMs, long timeoutMs, String currentInstanceId) throws Exception {
    long expiredCount = 0L;
    long remoteOwnerCount = 0L;
    Set<Long> aliveLockIds = new HashSet<>();
    for (String child : children()) {
      String path = locksRootPath + "/" + child;
      // The node name carries the lock id even for provisional nodes whose payload is not final yet.
      long lockId = parseLockIdFromNodeName(child);
      if (lockId <= 0) {
        continue;
      }
      Stat stat = new Stat();
      byte[] data;
      try {
        data = client.getData().storingStatIn(stat).forPath(path);
      } catch (KeeperException.NoNodeException ignored) {
        continue;
      }
      SyntheticReadLockManager.SyntheticLockState state = deserialize(data);
      if (!state.isExpired(nowMs, timeoutMs)) {
        aliveLockIds.add(lockId);
        continue;
      }
      try {
        client.delete().withVersion(stat.getVersion()).forPath(path);
      } catch (KeeperException.NoNodeException ignored) {
        continue;
      } catch (KeeperException.BadVersionException ignored) {
        // Another proxy refreshed the node while we were cleaning up.
        aliveLockIds.add(lockId);
        continue;
      }
      expiredCount++;
      if (!state.ownerInstanceId().equals(currentInstanceId)) {
        remoteOwnerCount++;
      }
      deleteTxnIndexEntry(state.txnId(), lockId);
    }
    collectOrphanTxnIndexEntries(aliveLockIds);
    return new CleanupSummary(expiredCount, remoteOwnerCount, aliveLockIds.size());
  }

  @Override
  public long activeLockCount() throws Exception {
    return children().size();
  }

  @Override
  public void close() {
    client.close();
  }

  private void configureSecurity(ProxyConfig config) throws IOException {
    if (!config.security().kerberosEnabled()) {
      return;
    }
    Configuration securityConf = new Configuration(false);
    securityConf.set("hadoop.security.authentication", config.security().mode().hadoopAuthValue());
    UserGroupInformation.setConfiguration(securityConf);
    String principal = KerberosPrincipalUtil.resolveForLocalHost(config.security().serverPrincipal());
    SecurityUtils.setZookeeperClientKerberosJaasConfig(principal, config.security().keytab());
    UserGroupInformation.loginUserFromKeytab(principal, config.security().keytab());
    LOG.info("Configured ZooKeeper SASL client JAAS entry '{}' for synthetic read-lock store principal {}",
        System.getProperty("zookeeper.sasl.clientconfig", "<unset>"),
        principal);
  }

  private List<String> children() throws Exception {
    return childrenOf(locksRootPath);
  }

  private List<String> childrenOf(String path) throws Exception {
    try {
      return client.getChildren().forPath(path);
    } catch (KeeperException.NoNodeException ignored) {
      return List.of();
    }
  }

  private void createTxnIndexEntry(long txnId, long lockId) throws Exception {
    if (txnId <= 0) {
      return;
    }
    String path = txnIndexPath(txnId, lockId);
    for (int attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
      try {
        client.create().creatingParentContainersIfNeeded().forPath(path);
        return;
      } catch (KeeperException.NodeExistsException ignored) {
        return;
      } catch (KeeperException.NoNodeException ignored) {
        // A sweep removed the now-empty txn parent between the parent create and this create.
      }
    }
    throw new IOException("Unable to create synthetic read-lock txn index entry " + path);
  }

  private void deleteTxnIndexEntry(long txnId, long lockId) throws Exception {
    if (txnId <= 0) {
      return;
    }
    // The empty txn parent is left for the sweep so that unlock stays at two ZooKeeper writes.
    deleteIfPresent(txnIndexPath(txnId, lockId));
  }

  private void deleteTxnIndexParentIfEmpty(String txnPath) throws Exception {
    try {
      client.delete().forPath(txnPath);
    } catch (KeeperException.NoNodeException | KeeperException.NotEmptyException ignored) {
      // Already gone, or another proxy indexed a new lock under the same transaction.
    }
  }

  private void collectOrphanTxnIndexEntries(Set<Long> aliveLockIds) throws Exception {
    for (String txnChild : childrenOf(txnIndexRootPath)) {
      String txnPath = txnIndexRootPath + "/" + txnChild;
      boolean allEntriesRemoved = true;
      for (String indexChild : childrenOf(txnPath)) {
        long lockId = parseIndexedLockId(indexChild);
        // Lock nodes are created before their index entry, so a missing lock node means the entry
        // is stale rather than racing with an in-flight create.
        if (lockId > 0
            && (aliveLockIds.contains(lockId) || client.checkExists().forPath(lockPath(lockId)) != null)) {
          allEntriesRemoved = false;
          continue;
        }
        deleteIfPresent(txnPath + "/" + indexChild);
      }
      if (allEntriesRemoved) {
        deleteTxnIndexParentIfEmpty(txnPath);
      }
    }
  }

  private long parseLockIdFromNodeName(String lockNodeName) {
    if (!lockNodeName.startsWith("lock-")) {
      return -1L;
    }
    try {
      return SyntheticReadLockManager.lockIdForSequence(
          Long.parseLong(lockNodeName.substring("lock-".length())));
    } catch (NumberFormatException ignored) {
      return -1L;
    }
  }

  private long parseIndexedLockId(String indexNodeName) {
    try {
      long lockId = Long.parseLong(indexNodeName);
      return lockId > SyntheticReadLockManager.SYNTHETIC_LOCK_ID_FLOOR ? lockId : -1L;
    } catch (NumberFormatException ignored) {
      return -1L;
    }
  }

  private String txnIndexPath(long txnId) {
    return txnIndexRootPath + "/" + txnId;
  }

  private String txnIndexPath(long txnId, long lockId) {
    return txnIndexPath(txnId) + "/" + lockId;
  }

  private SyntheticReadLockManager.SyntheticLockState readState(String path) throws Exception {
    try {
      return deserialize(client.getData().forPath(path));
    } catch (KeeperException.NoNodeException ignored) {
      return null;
    }
  }

  private void deleteIfPresent(String path) throws Exception {
    try {
      client.delete().guaranteed().forPath(path);
    } catch (KeeperException.NoNodeException ignored) {
    }
  }

  private void createPathIfMissing(String path) throws Exception {
    try {
      client.create().creatingParentContainersIfNeeded().forPath(path);
    } catch (KeeperException.NodeExistsException ignored) {
    }
  }

  private String lockPath(long lockId) {
    long sequence = SyntheticReadLockManager.sequenceForLockId(lockId);
    return locksRootPath + "/lock-" + String.format("%010d", sequence);
  }

  private long parseSequence(String path) {
    String nodeName = path.substring(path.lastIndexOf('/') + 1);
    return Long.parseLong(nodeName.substring("lock-".length()));
  }

  private String normalizedZnode(String znode) {
    if (znode == null || znode.isBlank() || "/".equals(znode)) {
      return "/hms-proxy-synthetic-read-locks";
    }
    return znode.startsWith("/") ? znode : "/" + znode;
  }

  private byte[] serialize(SyntheticReadLockManager.SyntheticLockState state) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try (DataOutputStream data = new DataOutputStream(output)) {
      data.writeInt(SERIALIZATION_VERSION);
      data.writeLong(state.lockId());
      data.writeLong(state.txnId());
      writeString(data, state.catalogName());
      writeString(data, state.backendDbName());
      writeString(data, state.externalDbName());
      writeString(data, state.ownerInstanceId());
      data.writeLong(state.lastTouchedAtMs());
    }
    return output.toByteArray();
  }

  private SyntheticReadLockManager.SyntheticLockState deserialize(byte[] data) throws IOException {
    try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(data))) {
      int version = input.readInt();
      if (version != SERIALIZATION_VERSION) {
        throw new IOException("Unsupported synthetic read-lock state version: " + version);
      }
      return new SyntheticReadLockManager.SyntheticLockState(
          input.readLong(),
          input.readLong(),
          readString(input),
          readString(input),
          readString(input),
          readString(input),
          input.readLong());
    }
  }

  private void writeString(DataOutputStream output, String value) throws IOException {
    byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
    output.writeInt(encoded.length);
    output.write(encoded);
  }

  private String readString(DataInputStream input) throws IOException {
    int length = input.readInt();
    byte[] encoded = input.readNBytes(length);
    if (encoded.length != length) {
      throw new IOException("Unexpected end of synthetic read-lock state while reading string field");
    }
    return new String(encoded, StandardCharsets.UTF_8);
  }
}
