package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.security.KerberosPrincipalUtil;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
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

final class ZooKeeperSyntheticReadLockStore implements SyntheticReadLockStore {
  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperSyntheticReadLockStore.class);
  private static final int SERIALIZATION_VERSION = 1;

  private final CuratorFramework client;
  private final String locksRootPath;

  ZooKeeperSyntheticReadLockStore(ProxyConfig config) throws Exception {
    ProxyConfig.SyntheticReadLockStoreZooKeeperConfig zooKeeper = config.syntheticReadLockStore().zooKeeper();
    configureSecurity(config);
    this.client = CuratorFrameworkFactory.builder()
        .connectString(zooKeeper.connectString())
        .connectionTimeoutMs(zooKeeper.connectionTimeoutMs())
        .sessionTimeoutMs(zooKeeper.sessionTimeoutMs())
        .retryPolicy(new ExponentialBackoffRetry(zooKeeper.baseSleepMs(), zooKeeper.maxRetries()))
        .build();
    this.locksRootPath = normalizedZnode(zooKeeper.znode()) + "/locks";
    client.start();
    if (!client.blockUntilConnected(zooKeeper.connectionTimeoutMs(), TimeUnit.MILLISECONDS)) {
      client.close();
      throw new IOException("Timed out connecting to ZooKeeper synthetic read-lock store at "
          + zooKeeper.connectString());
    }
    createPathIfMissing(locksRootPath);
    LOG.info("Synthetic read-lock store started in ZooKeeper mode with connectString='{}', znode='{}'",
        zooKeeper.connectString(), normalizedZnode(zooKeeper.znode()));
  }

  @Override
  public SyntheticReadLockManager.SyntheticLockState create(
      long txnId,
      String catalogName,
      String backendDbName,
      String externalDbName,
      long createdAtMs
  ) throws Exception {
    SyntheticReadLockManager.SyntheticLockState provisional = new SyntheticReadLockManager.SyntheticLockState(
        0L,
        txnId,
        catalogName,
        backendDbName,
        externalDbName,
        createdAtMs);
    String createdPath = client.create()
        .creatingParentContainersIfNeeded()
        .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
        .forPath(locksRootPath + "/lock-", serialize(provisional));
    long lockId = SyntheticReadLockManager.lockIdForSequence(parseSequence(createdPath));
    SyntheticReadLockManager.SyntheticLockState state = provisional.withLockId(lockId);
    client.setData().forPath(createdPath, serialize(state));
    return state;
  }

  @Override
  public SyntheticReadLockManager.SyntheticLockState get(long lockId) throws Exception {
    String path = lockPath(lockId);
    try {
      return deserialize(client.getData().forPath(path));
    } catch (KeeperException.NoNodeException ignored) {
      return null;
    }
  }

  @Override
  public void touch(long lockId, long nowMs) throws Exception {
    String path = lockPath(lockId);
    for (int attempt = 0; attempt < 4; attempt++) {
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
  }

  @Override
  public void releaseLock(long lockId) throws Exception {
    deleteIfPresent(lockPath(lockId));
  }

  @Override
  public void releaseTxn(long txnId) throws Exception {
    if (txnId <= 0) {
      return;
    }
    for (String child : children()) {
      String path = locksRootPath + "/" + child;
      SyntheticReadLockManager.SyntheticLockState state = readState(path);
      if (state != null && state.txnId() == txnId) {
        deleteIfPresent(path);
      }
    }
  }

  @Override
  public void cleanupExpiredLocks(long nowMs, long timeoutMs) throws Exception {
    for (String child : children()) {
      String path = locksRootPath + "/" + child;
      Stat stat = new Stat();
      byte[] data;
      try {
        data = client.getData().storingStatIn(stat).forPath(path);
      } catch (KeeperException.NoNodeException ignored) {
        continue;
      }
      SyntheticReadLockManager.SyntheticLockState state = deserialize(data);
      if (!state.isExpired(nowMs, timeoutMs)) {
        continue;
      }
      try {
        client.delete().withVersion(stat.getVersion()).forPath(path);
      } catch (KeeperException.NoNodeException | KeeperException.BadVersionException ignored) {
        // Another proxy refreshed or removed the node while we were cleaning up.
      }
    }
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
    try {
      return client.getChildren().forPath(locksRootPath);
    } catch (KeeperException.NoNodeException ignored) {
      return List.of();
    }
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
