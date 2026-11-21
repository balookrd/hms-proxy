package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.routing.DistributedCacheInvalidationZooKeeperConfig;
import io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics;
import io.github.mmalykhin.hmsproxy.security.KerberosPrincipalUtil;
import io.github.mmalykhin.hmsproxy.security.ProcessKerberosConfiguration;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZooKeeper-backed distributed cache invalidator.
 *
 * <p>Coordinates DDL cache invalidations across multiple proxy instances using a shared ZooKeeper event tree.
 * When DDL is executed on this instance, local caches are immediately cleared and an invalidation event is
 * published as a sequential znode. Other instances watch the event subtree via Curator {@link PathChildrenCache}
 * and invalidate their local caches upon receipt.
 */
final class ZooKeeperCacheInvalidator implements CacheInvalidator {
  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperCacheInvalidator.class);
  private static final int PUBLISH_QUEUE_CAPACITY = 1024;

  private final CuratorFramework client;
  private final boolean ownsClient;
  private final String eventsRootPath;
  private final String instanceId;
  private final long eventRetentionMs;
  private final LocalCacheInvalidator localInvalidator;
  private final PrometheusMetrics metrics;
  private final LongSupplier clock;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final ExecutorService publishExecutor;
  private PathChildrenCache pathChildrenCache;
  private ConnectionStateListener connectionStateListener;
  private ScheduledExecutorService cleanupExecutor;

  ZooKeeperCacheInvalidator(
      ProxyConfig config,
      LocalCacheInvalidator localInvalidator,
      PrometheusMetrics metrics
  ) throws Exception {
    DistributedCacheInvalidationZooKeeperConfig zk =
        config.latencyRouting().distributedCacheInvalidation().zooKeeper();
    configureSecurity(config);
    this.client = CuratorFrameworkFactory.builder()
        .connectString(zk.connectString())
        .connectionTimeoutMs(zk.connectionTimeoutMs())
        .sessionTimeoutMs(zk.sessionTimeoutMs())
        .retryPolicy(new ExponentialBackoffRetry(zk.baseSleepMs(), zk.maxRetries()))
        .build();
    this.ownsClient = true;
    this.instanceId = UUID.randomUUID().toString();
    this.eventRetentionMs = zk.eventRetentionMs();
    this.eventsRootPath = buildEventsRootPath(zk.znode());
    this.localInvalidator = localInvalidator;
    this.metrics = metrics;
    this.clock = System::currentTimeMillis;
    this.publishExecutor = startPublishExecutor();

    try {
      client.start();
      if (!client.blockUntilConnected(zk.connectionTimeoutMs(), TimeUnit.MILLISECONDS)) {
        throw new IOException("Timed out connecting to ZooKeeper distributed cache invalidation bus at "
            + zk.connectString());
      }
      initBus();
      LOG.info("ZooKeeper distributed cache invalidation bus started with instanceId='{}', eventsRootPath='{}'",
          instanceId, eventsRootPath);
    } catch (Exception e) {
      cleanupResourcesQuietly(e);
      throw e;
    }
  }

  /** Package-private constructor for unit/integration tests with an existing Curator client. */
  ZooKeeperCacheInvalidator(
      CuratorFramework client,
      String rootZnode,
      long eventRetentionMs,
      String instanceId,
      LocalCacheInvalidator localInvalidator,
      PrometheusMetrics metrics,
      LongSupplier clock
  ) throws Exception {
    this.client = client;
    this.ownsClient = false;
    this.instanceId = instanceId != null ? instanceId : UUID.randomUUID().toString();
    this.eventRetentionMs = eventRetentionMs > 0 ? eventRetentionMs : DistributedCacheInvalidationZooKeeperConfig.DEFAULT_EVENT_RETENTION_MS;
    this.eventsRootPath = buildEventsRootPath(rootZnode);
    this.localInvalidator = localInvalidator;
    this.metrics = metrics;
    this.clock = clock != null ? clock : System::currentTimeMillis;
    this.publishExecutor = startPublishExecutor();

    try {
      initBus();
    } catch (Exception e) {
      cleanupResourcesQuietly(e);
      throw e;
    }
  }

  private void initBus() throws Exception {
    createPathIfMissing(eventsRootPath);
    this.connectionStateListener = (c, state) -> handleConnectionState(state);
    client.getConnectionStateListenable().addListener(connectionStateListener);

    this.pathChildrenCache = new PathChildrenCache(client, eventsRootPath, true);
    this.pathChildrenCache.getListenable().addListener((c, event) -> handleCacheEvent(event));
    this.pathChildrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);

    this.cleanupExecutor = startCleanupExecutor();
  }

  void attachIcebergTablePointerGuard(IcebergTablePointerGuard guard) {
    localInvalidator.attachIcebergTablePointerGuard(guard);
  }

  String eventsRootPath() {
    return eventsRootPath;
  }

  String instanceId() {
    return instanceId;
  }

  boolean awaitPendingPublications(long timeoutMs) {
    if (closed.get()) {
      return false;
    }
    CompletableFuture<Void> barrier = new CompletableFuture<>();
    try {
      publishExecutor.execute(() -> barrier.complete(null));
      barrier.get(Math.max(1L, timeoutMs), TimeUnit.MILLISECONDS);
      return true;
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      return false;
    }
  }

  @Override
  public void invalidateTable(String catalogName, String backendDbName, String tableName) {
    localInvalidator.invalidateTable(catalogName, backendDbName, tableName);
    publishEvent(new CacheInvalidationEvent(
        CacheInvalidationEvent.Type.TABLE,
        catalogName,
        backendDbName,
        tableName,
        instanceId,
        clock.getAsLong()
    ));
  }

  @Override
  public void invalidateDatabase(String catalogName, String backendDbName) {
    localInvalidator.invalidateDatabase(catalogName, backendDbName);
    publishEvent(new CacheInvalidationEvent(
        CacheInvalidationEvent.Type.DATABASE,
        catalogName,
        backendDbName,
        null,
        instanceId,
        clock.getAsLong()
    ));
  }

  @Override
  public void invalidateCatalog(String catalogName) {
    localInvalidator.invalidateCatalog(catalogName);
    publishEvent(new CacheInvalidationEvent(
        CacheInvalidationEvent.Type.CATALOG,
        catalogName,
        null,
        null,
        instanceId,
        clock.getAsLong()
    ));
  }

  @Override
  public void invalidateAll() {
    localInvalidator.invalidateAll();
    publishEvent(new CacheInvalidationEvent(
        CacheInvalidationEvent.Type.ALL,
        null,
        null,
        null,
        instanceId,
        clock.getAsLong()
    ));
  }

  private void publishEvent(CacheInvalidationEvent event) {
    if (closed.get()) {
      return;
    }
    try {
      publishExecutor.execute(() -> sendEvent(event));
    } catch (RejectedExecutionException e) {
      if (!closed.get()) {
        LOG.warn("Dropped distributed cache invalidation event {} because publish queue is full", event);
        if (metrics != null) {
          metrics.recordDistributedCacheInvalidationError("publish_queue_full");
        }
      }
    }
  }

  private void sendEvent(CacheInvalidationEvent event) {
    try {
      client.create()
          .creatingParentContainersIfNeeded()
          .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
          .forPath(eventsRootPath + "/evt-", event.serialize());
      if (metrics != null) {
        metrics.recordDistributedCacheInvalidationPublished(event.type().name().toLowerCase(Locale.ROOT));
      }
    } catch (Exception e) {
      if (!closed.get()) {
        LOG.warn("Failed to publish distributed cache invalidation event {} to ZooKeeper", event, e);
        if (metrics != null) {
          metrics.recordDistributedCacheInvalidationError("publish");
        }
      }
    }
  }

  private void handleCacheEvent(PathChildrenCacheEvent event) {
    if (event.getType() != PathChildrenCacheEvent.Type.CHILD_ADDED) {
      return;
    }
    ChildData data = event.getData();
    if (data == null || data.getData() == null || data.getData().length == 0) {
      return;
    }
    try {
      CacheInvalidationEvent payload = CacheInvalidationEvent.deserialize(data.getData());
      if (instanceId.equals(payload.originInstanceId())) {
        return; // Suppress echo of self-published event
      }
      if (clock.getAsLong() - payload.timestampMs() > eventRetentionMs) {
        return; // Stale event
      }
      switch (payload.type()) {
        case TABLE -> localInvalidator.invalidateTableFromRemote(payload.catalogName(), payload.backendDbName(), payload.tableName());
        case DATABASE -> localInvalidator.invalidateDatabase(payload.catalogName(), payload.backendDbName());
        case CATALOG -> localInvalidator.invalidateCatalog(payload.catalogName());
        case ALL -> localInvalidator.invalidateAll();
      }
      if (metrics != null) {
        metrics.recordDistributedCacheInvalidationReceived(payload.type().name().toLowerCase(Locale.ROOT));
      }
    } catch (Exception e) {
      LOG.warn("Failed to process distributed cache invalidation event from path '{}'",
          data.getPath(), e);
      if (metrics != null) {
        metrics.recordDistributedCacheInvalidationError("receive");
      }
    }
  }

  private void handleConnectionState(ConnectionState newState) {
    if (newState == ConnectionState.LOST
        || newState == ConnectionState.SUSPENDED
        || newState == ConnectionState.RECONNECTED) {
      LOG.warn("ZooKeeper cache invalidation connection state changed to '{}', flushing local metadata caches", newState);
      localInvalidator.invalidateAll();
      if (metrics != null && newState != ConnectionState.RECONNECTED) {
        metrics.recordDistributedCacheInvalidationError("connection_" + newState.name().toLowerCase(Locale.ROOT));
      }
    }
  }

  void purgeExpiredEvents() {
    long cutoffMs = clock.getAsLong() - eventRetentionMs;
    try {
      List<String> children = client.getChildren().forPath(eventsRootPath);
      for (String child : children) {
        String childPath = eventsRootPath + "/" + child;
        Stat stat = client.checkExists().forPath(childPath);
        if (stat != null && stat.getCtime() < cutoffMs) {
          try {
            client.delete().guaranteed().forPath(childPath);
          } catch (KeeperException.NoNodeException ignored) {
            // Already cleaned up by another proxy replica
          }
        }
      }
    } catch (Exception e) {
      LOG.warn("Failed to purge expired distributed cache invalidation events from ZooKeeper", e);
      if (metrics != null) {
        metrics.recordDistributedCacheInvalidationError("cleanup");
      }
    }
  }

  private static ExecutorService startPublishExecutor() {
    ThreadFactory threadFactory = r -> {
      Thread t = new Thread(r, "hms-proxy-zk-invalidation-pub");
      t.setDaemon(true);
      return t;
    };
    return new ThreadPoolExecutor(
        1, 1, 0L, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(PUBLISH_QUEUE_CAPACITY),
        threadFactory,
        new ThreadPoolExecutor.AbortPolicy());
  }

  private ScheduledExecutorService startCleanupExecutor() {
    ThreadFactory threadFactory = r -> {
      Thread t = new Thread(r, "hms-proxy-zk-cache-invalidation-cleaner");
      t.setDaemon(true);
      return t;
    };
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(threadFactory);
    long periodMs = Math.max(30_000L, eventRetentionMs / 4);
    executor.scheduleWithFixedDelay(this::purgeExpiredEvents, periodMs, periodMs, TimeUnit.MILLISECONDS);
    return executor;
  }

  private void createPathIfMissing(String path) throws Exception {
    try {
      if (client.checkExists().forPath(path) == null) {
        client.create().creatingParentsIfNeeded().forPath(path);
      }
    } catch (KeeperException.NodeExistsException ignored) {
      // Created concurrently by another instance
    }
  }

  private static String buildEventsRootPath(String znode) {
    String normalized = normalizedZnode(znode);
    return "/".equals(normalized) ? "/events" : normalized + "/events";
  }

  private static String normalizedZnode(String znode) {
    String trimmed = znode.trim();
    if (!trimmed.startsWith("/")) {
      trimmed = "/" + trimmed;
    }
    if (trimmed.length() > 1 && trimmed.endsWith("/")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1);
    }
    return trimmed;
  }

  private void configureSecurity(ProxyConfig config) throws IOException {
    if (!config.security().kerberosEnabled()) {
      return;
    }
    ProcessKerberosConfiguration kerberos = ProcessKerberosConfiguration.processWide();
    kerberos.ensureConfigured(config.security().mode().hadoopAuthValue());
    String principal = KerberosPrincipalUtil.resolveForLocalHost(config.security().serverPrincipal());
    SecurityUtils.setZookeeperClientKerberosJaasConfig(principal, config.security().keytab());
    kerberos.ensureLoginUserFromKeytab(principal, config.security().keytab());
    LOG.info("Configured ZooKeeper SASL client JAAS entry for distributed cache invalidation principal {}",
        principal);
  }

  private void cleanupResourcesQuietly(Throwable failure) {
    try {
      close();
    } catch (Exception closeError) {
      failure.addSuppressed(closeError);
    }
  }

  @Override
  public void close() throws Exception {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    if (cleanupExecutor != null) {
      cleanupExecutor.shutdownNow();
    }
    if (publishExecutor != null) {
      publishExecutor.shutdown();
      try {
        if (!publishExecutor.awaitTermination(1L, TimeUnit.SECONDS)) {
          publishExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        publishExecutor.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
    if (client != null && connectionStateListener != null) {
      client.getConnectionStateListenable().removeListener(connectionStateListener);
    }
    if (pathChildrenCache != null) {
      pathChildrenCache.close();
    }
    if (ownsClient && client != null) {
      client.close();
    }
  }
}
