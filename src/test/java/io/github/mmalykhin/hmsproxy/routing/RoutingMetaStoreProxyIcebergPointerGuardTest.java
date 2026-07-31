package io.github.mmalykhin.hmsproxy.routing;

import static io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreProxyTestSupport.catalogConfig;
import static io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreProxyTestSupport.newBackend;
import static io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreProxyTestSupport.newBackendRuntime;
import static io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreProxyTestSupport.newSession;

import io.github.mmalykhin.hmsproxy.backend.ApacheBackendAdapter;
import io.github.mmalykhin.hmsproxy.backend.BackendAdapter;
import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.backend.Hive4BackendAdapter;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.routing.IcebergPointerGuardConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityMode;
import io.github.mmalykhin.hmsproxy.config.server.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;
import io.github.mmalykhin.hmsproxy.federation.FederationLayer;
import io.github.mmalykhin.hmsproxy.observability.ProxyObservability;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntConsumer;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.junit.Assert;
import org.junit.Test;

/**
 * A Hive client's own {@code alter_table} must not roll an Iceberg table's pointer back.
 *
 * <p>The stand caught this as real data loss (TEST-MATRIX section I, "I4 in detail"): a
 * HiveServer2 {@code INSERT} opens with an {@code alter_table_with_environment_context} carrying
 * {@code alterTableOpType=DROPPROPS} and the {@code Table} object it snapshotted when the query
 * was compiled. A metastore applies those parameters wholesale, {@code metadata_location}
 * included, so a REST commit that landed in between is erased - and none of it happens under the
 * Iceberg lock, so nothing serializes the two. The proxy is the only place both paths meet.
 *
 * <p>The shape that alter actually has on the wire carries no {@code metadata_location} at all,
 * so whether a request concerns an Iceberg table is decided from the metastore's own record, and
 * the price of that read is bounded by a negative cache of names known not to be Iceberg tables.
 */
public class RoutingMetaStoreProxyIcebergPointerGuardTest {
  private static final String CURRENT = "hdfs://nn/warehouse/sales/events/metadata/00006-current.metadata.json";
  private static final String PREVIOUS = "hdfs://nn/warehouse/sales/events/metadata/00005-previous.metadata.json";
  private static final String STALE = "hdfs://nn/warehouse/sales/events/metadata/00005-stale.metadata.json";

  /**
   * The shape the stand proved is real: HiveServer2 opens an {@code INSERT} with the table's
   * statistics and {@code EXTERNAL}, and not one Iceberg key. Applied wholesale it would erase
   * the whole Iceberg state of the record, the pointer included.
   */
  @Test
  public void alterWithoutAnyPointerKeepsTheIcebergStateTheMetastoreHolds() throws Throwable {
    Stand stand = newStand(icebergRecord(CURRENT, PREVIOUS));

    invokeAlter(stand, hiveInsertAlter(), dropPropsContext());

    Map<String, String> forwarded = stand.forwarded.get().getParameters();
    Assert.assertEquals("an alter that carries no pointer must not erase the one in the metastore",
        CURRENT, forwarded.get("metadata_location"));
    Assert.assertEquals("the base pointer belongs to the metastore's record too",
        PREVIOUS, forwarded.get("previous_metadata_location"));
    Assert.assertEquals("ICEBERG", forwarded.get("table_type"));
    Assert.assertEquals("org.apache.iceberg.mr.hive.HiveIcebergStorageHandler", forwarded.get("storage_handler"));
    Assert.assertEquals("every Iceberg key the record holds survives, not a hand-picked list",
        "7051323893436947220", forwarded.get("current-snapshot-id"));
    Assert.assertEquals("everything the client actually meant to change must still go through",
        "17", forwarded.get("numRows"));
    Assert.assertEquals("TRUE", forwarded.get("EXTERNAL"));
  }

  @Test
  public void staleAlterTableKeepsTheMetadataLocationTheMetastoreCurrentlyHolds() throws Throwable {
    Stand stand = newStand(icebergRecord(CURRENT, PREVIOUS));

    Table stale = icebergTable(STALE, null);
    stale.getParameters().put("numRows", "17");
    invokeAlter(stand, stale, dropPropsContext());

    Assert.assertEquals("a stale alter_table must not move the Iceberg pointer backwards",
        CURRENT, stand.forwarded.get().getParameters().get("metadata_location"));
    Assert.assertEquals("everything the client actually meant to change must still go through",
        "17", stand.forwarded.get().getParameters().get("numRows"));
  }

  @Test
  public void icebergCommitThatBuildsOnTheCurrentPointerIsLeftAlone() throws Throwable {
    Stand stand = newStand(icebergRecord(CURRENT, PREVIOUS));

    // What an Iceberg commit sends: a new metadata file, with the pointer it read as the base.
    String next = "hdfs://nn/warehouse/sales/events/metadata/00007-next.metadata.json";
    invokeAlter(stand, icebergTable(next, CURRENT), null);

    Assert.assertEquals("a commit built on the current pointer must pass through untouched",
        next, stand.forwarded.get().getParameters().get("metadata_location"));
    Assert.assertEquals("a forward commit keeps the base pointer it committed against",
        CURRENT, stand.forwarded.get().getParameters().get("previous_metadata_location"));
    Assert.assertNull("nothing of the record is merged into a request the guard does not touch",
        stand.forwarded.get().getParameters().get("current-snapshot-id"));
  }

  // --- The Hive-engine storage descriptor a committing engine can strip ---

  /**
   * The defect measured on the stand: a Hive 4 {@code STORED BY ICEBERG} create leaves no
   * {@code engine.hive.enabled} in the Iceberg metadata, so a 3.1 engine committing its own
   * {@code INSERT} onto that table falls back to {@code iceberg.engine.hive.enabled} in its own
   * Hadoop configuration - unset, and false by default. Iceberg then rebuilds the descriptor with
   * the abstract {@code FileInputFormat} and drops {@code storage_handler}, and the table stops
   * being readable by the very line that just wrote it.
   *
   * <p>That request is a legitimate forward commit - its base pointer is the one the metastore
   * holds - so nothing about the pointer is wrong with it and it is not repaired. The descriptor
   * still has to survive it.
   */
  @Test
  public void aForwardCommitThatStripsTheHiveEngineDescriptorKeepsTheRecordsOwn() throws Throwable {
    Stand stand = newStand(icebergRecord(CURRENT, PREVIOUS));

    String next = "hdfs://nn/warehouse/sales/events/metadata/00007-next.metadata.json";
    Table commit = icebergTable(next, CURRENT);
    commit.setSd(plainFilesDescriptor());
    invokeAlter(stand, commit, null);

    Table forwarded = stand.forwarded.get();
    Assert.assertEquals("a forward commit still moves the pointer",
        next, forwarded.getParameters().get("metadata_location"));
    Assert.assertEquals("the input format a Hive 3.1 planner can instantiate must survive",
        "org.apache.iceberg.mr.hive.HiveIcebergInputFormat", forwarded.getSd().getInputFormat());
    Assert.assertEquals("org.apache.iceberg.mr.hive.HiveIcebergOutputFormat",
        forwarded.getSd().getOutputFormat());
    Assert.assertEquals("org.apache.iceberg.mr.hive.HiveIcebergSerDe",
        forwarded.getSd().getSerdeInfo().getSerializationLib());
    Assert.assertEquals("Iceberg drops storage_handler in the same branch that degrades the"
            + " descriptor, so it has to be kept in the same place",
        "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler",
        forwarded.getParameters().get("storage_handler"));
    Assert.assertEquals("nothing else about the descriptor is the proxy's business",
        "hdfs://nn/warehouse/sales/events", forwarded.getSd().getLocation());
  }

  /** The same has to hold on the path that does repair the pointer, not only on forward commits. */
  @Test
  public void aRepairedAlterKeepsTheHiveEngineDescriptorToo() throws Throwable {
    Stand stand = newStand(icebergRecord(CURRENT, PREVIOUS));

    Table stale = icebergTable(STALE, null);
    stale.setSd(plainFilesDescriptor());
    invokeAlter(stand, stale, dropPropsContext());

    Assert.assertEquals(CURRENT, stand.forwarded.get().getParameters().get("metadata_location"));
    Assert.assertEquals("org.apache.iceberg.mr.hive.HiveIcebergInputFormat",
        stand.forwarded.get().getSd().getInputFormat());
  }

  /**
   * The guard keeps what the record holds; it never imposes a descriptor. A table the metastore
   * records without a storage handler - an Iceberg table whose owner turned the Hive engine off -
   * is left exactly as the client sent it.
   */
  @Test
  public void anIcebergTableWithoutAStorageHandlerIsLeftWithTheDescriptorAsSent() throws Throwable {
    Table record = icebergTable(CURRENT, PREVIOUS);
    record.setSd(plainFilesDescriptor());
    Stand stand = newStand(record);

    String next = "hdfs://nn/warehouse/sales/events/metadata/00007-next.metadata.json";
    Table commit = icebergTable(next, CURRENT);
    commit.setSd(plainFilesDescriptor());
    invokeAlter(stand, commit, null);

    Assert.assertEquals("a table that never had the Hive engine descriptor must not grow one",
        "org.apache.hadoop.mapred.FileInputFormat", stand.forwarded.get().getSd().getInputFormat());
    Assert.assertNull(stand.forwarded.get().getParameters().get("storage_handler"));
  }

  /**
   * Turning the Hive engine off on a table that has it is a deliberate act, and the only thing the
   * guard would otherwise stand in the way of, so it is switchable.
   */
  @Test
  public void theDescriptorIsLeftAloneWhenTheGuardIsToldNotToKeepIt() throws Throwable {
    Stand stand = newStand(
        icebergRecord(CURRENT, PREVIOUS),
        MetastoreRuntimeProfile.APACHE_3_1_3,
        new IcebergPointerGuardConfig(true, 30_000L, 10_000, true, 10_000L, false));

    String next = "hdfs://nn/warehouse/sales/events/metadata/00007-next.metadata.json";
    Table commit = icebergTable(next, CURRENT);
    commit.setSd(plainFilesDescriptor());
    invokeAlter(stand, commit, null);

    Assert.assertEquals("org.apache.hadoop.mapred.FileInputFormat",
        stand.forwarded.get().getSd().getInputFormat());
    Assert.assertNull("the pointer guard itself keeps working; only the descriptor is let go",
        stand.forwarded.get().getParameters().get("storage_handler"));
  }

  /**
   * Ordinary Hive tables are where the volume is, so the answer "this name is not an Iceberg
   * table" is cached: the first alter reads the record, the next one inside the TTL does not.
   */
  @Test
  public void anOrdinaryHiveTableIsReadOnceAndItsAlterPassesThroughUnchanged() throws Throwable {
    Stand stand = newStand(hiveRecord());

    invokeAlter(stand, hiveInsertAlter(), dropPropsContext());
    Assert.assertEquals(1, stand.reads.get());
    Assert.assertNull("an ordinary Hive table must not grow an Iceberg pointer",
        stand.forwarded.get().getParameters().get("metadata_location"));
    Assert.assertEquals("17", stand.forwarded.get().getParameters().get("numRows"));

    invokeAlter(stand, hiveInsertAlter(), dropPropsContext());
    Assert.assertEquals("a second alter inside the TTL must not read the record again",
        1, stand.reads.get());
  }

  @Test
  public void theNotAnIcebergTableAnswerIsForgottenWhenItsTtlExpires() throws Throwable {
    Stand stand = newStand(
        hiveRecord(),
        MetastoreRuntimeProfile.APACHE_3_1_3,
        new IcebergPointerGuardConfig(true, 1L, 10_000, true, 10_000L, true));

    invokeAlter(stand, hiveInsertAlter(), dropPropsContext());
    Assert.assertEquals(1, stand.reads.get());

    Thread.sleep(20L);
    invokeAlter(stand, hiveInsertAlter(), dropPropsContext());

    Assert.assertEquals("once the entry expired the record has to be read again", 2, stand.reads.get());
  }

  /**
   * An Iceberg table's pointer has to be the one the metastore holds right now, so it is read on
   * every alter and never cached - a cached pointer would be exactly the stale copy this guard
   * exists to reject.
   */
  @Test
  public void anIcebergTableIsNeverCachedSoItsPointerIsReadOnEveryAlter() throws Throwable {
    Stand stand = newStand(icebergRecord(CURRENT, PREVIOUS));

    invokeAlter(stand, hiveInsertAlter(), dropPropsContext());
    invokeAlter(stand, hiveInsertAlter(), dropPropsContext());

    Assert.assertEquals("two alters, and each repair reads twice - once to decide, once under the"
            + " lock it takes to make the repair atomic",
        4, stand.reads.get());
  }

  /**
   * A table that becomes an Iceberg table is protected again as soon as the proxy sees it happen,
   * without waiting for the cached answer to expire.
   */
  @Test
  public void creatingAnIcebergTableOverACachedNameDropsTheCachedAnswer() throws Throwable {
    Stand stand = newStand(hiveRecord());

    invokeAlter(stand, hiveInsertAlter(), dropPropsContext());
    Assert.assertEquals(1, stand.reads.get());

    Method create = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);
    invoke(stand.handler, create, icebergTable(CURRENT, null));
    stand.record.set(icebergRecord(CURRENT, PREVIOUS));
    invokeAlter(stand, hiveInsertAlter(), dropPropsContext());

    Assert.assertEquals("the cached 'not an Iceberg table' answer must not survive the create; the"
            + " alter that follows reads once to decide and once under the lock",
        3, stand.reads.get());
    Assert.assertEquals(CURRENT, stand.forwarded.get().getParameters().get("metadata_location"));
  }

  @Test
  public void aDisabledGuardReadsNothingAndRewritesNothing() throws Throwable {
    Stand stand = newStand(
        icebergRecord(CURRENT, PREVIOUS),
        MetastoreRuntimeProfile.APACHE_3_1_3,
        new IcebergPointerGuardConfig(false, 30_000L, 10_000, true, 10_000L, true));

    invokeAlter(stand, hiveInsertAlter(), dropPropsContext());

    Assert.assertEquals("a disabled guard must not cost a single round trip", 0, stand.reads.get());
    Assert.assertNull("a disabled guard must leave the request exactly as the client sent it",
        stand.forwarded.get().getParameters().get("metadata_location"));
  }

  /**
   * Stitching the current pointer in narrows the race but does not close it: the guard's read and
   * the backend's write are two calls, and a commit landing between them would still be
   * overwritten. Where the metastore supports it, the alter is therefore made conditional -
   * `expected_parameter_key`/`expected_parameter_value` make the metastore apply it only while
   * the pointer is still the one that was read, so a pointer that moved fails the alter loudly
   * instead of silently discarding a snapshot.
   */
  @Test
  public void repairedAlterCarriesTheCompareAndSwapTheMetastoreWillCheck() throws Throwable {
    Stand stand = newStand(icebergRecord(CURRENT, PREVIOUS), MetastoreRuntimeProfile.APACHE_4_1_0);

    invokeAlter(stand, hiveInsertAlter(), dropPropsContext());

    Assert.assertEquals("a Hive 4 backend has no positional get_table, and neither read must be"
            + " lost to that - the guard has to reach the record through the adapter",
        2, stand.reads.get());
    Assert.assertEquals(CURRENT, stand.forwarded.get().getParameters().get("metadata_location"));
    Assert.assertNotNull("the alter never reached the backend", stand.context.get());
    Map<String, String> properties = stand.context.get().getProperties();
    Assert.assertEquals("metadata_location", properties.get("expected_parameter_key"));
    Assert.assertEquals("the metastore must only apply this while the pointer is the one we read",
        CURRENT, properties.get("expected_parameter_value"));
    Assert.assertEquals("the client's own context must survive",
        "true", properties.get("DO_NOT_UPDATE_STATS"));
  }

  /**
   * The 3.1 line's metastore ignores those keys entirely, so sending them there would buy nothing
   * but false confidence - the run would look protected while the window stayed open.
   */
  @Test
  public void noCompareAndSwapIsSentToAMetastoreThatCannotCheckIt() throws Throwable {
    Stand stand = newStand(icebergRecord(CURRENT, PREVIOUS), MetastoreRuntimeProfile.APACHE_3_1_3);

    invokeAlter(stand, hiveInsertAlter(), dropPropsContext());

    Assert.assertEquals("the pointer is still repaired on every backend",
        CURRENT, stand.forwarded.get().getParameters().get("metadata_location"));
    Assert.assertNull("a metastore that cannot check the condition must not be told one",
        stand.context.get().getProperties().get("expected_parameter_key"));
  }

  /**
   * Both the reads the guard adds and the reads its cache saves have to be visible without
   * reading logs - that is what makes its cost measurable on the stand.
   */
  @Test
  public void everyGuardOutcomeIsCounted() throws Throwable {
    Stand iceberg = newStand(icebergRecord(CURRENT, PREVIOUS));
    invokeAlter(iceberg, hiveInsertAlter(), dropPropsContext());
    invokeAlter(iceberg, icebergTable("hdfs://nn/next.json", CURRENT), null);

    Stand hive = newStand(hiveRecord());
    invokeAlter(hive, hiveInsertAlter(), dropPropsContext());
    invokeAlter(hive, hiveInsertAlter(), dropPropsContext());

    String icebergMetrics = iceberg.observability.metrics().render();
    Assert.assertTrue(icebergMetrics,
        icebergMetrics.contains("hms_proxy_iceberg_pointer_guard_events_total"
            + "{catalog=\"catalog1\",outcome=\"repaired\"} 1"));
    Assert.assertTrue(icebergMetrics,
        icebergMetrics.contains("hms_proxy_iceberg_pointer_guard_events_total"
            + "{catalog=\"catalog1\",outcome=\"forward_commit\"} 1"));
    // Both alters arrive without the storage handler the record holds, so both have it kept -
    // the counter is per event, not per call.
    Assert.assertTrue(icebergMetrics,
        icebergMetrics.contains("hms_proxy_iceberg_pointer_guard_events_total"
            + "{catalog=\"catalog1\",outcome=\"hive_descriptor_kept\"} 2"));
    String hiveMetrics = hive.observability.metrics().render();
    Assert.assertTrue(hiveMetrics,
        hiveMetrics.contains("hms_proxy_iceberg_pointer_guard_events_total"
            + "{catalog=\"catalog1\",outcome=\"not_iceberg\"} 1"));
    Assert.assertTrue(hiveMetrics,
        hiveMetrics.contains("hms_proxy_iceberg_pointer_guard_events_total"
            + "{catalog=\"catalog1\",outcome=\"cache_suppressed\"} 1"));
  }

  // --- The lock the repair holds across read and write ---

  /**
   * The defect the lock exists for, reproduced end to end: a commit lands after the guard has read
   * the record and before the backend applies the alter. Reading again under the lock is what
   * turns it from a lost update into a merge over the pointer that actually committed.
   */
  @Test
  public void aCommitThatLandsBeforeTheLockIsMergedOverInsteadOfBeingOverwritten() throws Throwable {
    Stand stand = newStand(icebergRecord(CURRENT, PREVIOUS));
    String next = "hdfs://nn/warehouse/sales/events/metadata/00007-next.metadata.json";
    landCommitDuringRead(stand, 1, next);

    invokeAlter(stand, hiveInsertAlter(), dropPropsContext());

    Assert.assertEquals("the repair must be built on the record read under the lock, not before it",
        next, stand.forwarded.get().getParameters().get("metadata_location"));
    Assert.assertEquals("the committed snapshot must still be the one the metastore holds",
        next, stand.record.get().getParameters().get("metadata_location"));
    Assert.assertEquals("the client's own change still goes through",
        "17", stand.forwarded.get().getParameters().get("numRows"));
  }

  /** The same run without the lock: the commit is overwritten. This is the 3.1 line before this change. */
  @Test
  public void withoutTheLockTheSameCommitIsLost() throws Throwable {
    Stand stand = newStand(
        icebergRecord(CURRENT, PREVIOUS),
        MetastoreRuntimeProfile.APACHE_3_1_3,
        new IcebergPointerGuardConfig(true, 30_000L, 10_000, false, 10_000L, true));
    String next = "hdfs://nn/warehouse/sales/events/metadata/00007-next.metadata.json";
    landCommitDuringRead(stand, 1, next);

    invokeAlter(stand, hiveInsertAlter(), dropPropsContext());

    Assert.assertEquals("without the lock the guard repairs from a pointer that is already stale",
        CURRENT, stand.record.get().getParameters().get("metadata_location"));
    Assert.assertTrue("no lock was requested at all", stand.lockRequests.isEmpty());
  }

  /**
   * Mutual exclusion, not just a pair of RPCs: a commit that starts while a repair holds the lock
   * has to wait for it, and lands after it.
   */
  @Test
  public void aCommitAttemptedWhileTheRepairHoldsTheLockWaitsForIt() throws Throwable {
    Stand stand = newStand(icebergRecord(CURRENT, PREVIOUS));
    String next = "hdfs://nn/warehouse/sales/events/metadata/00007-next.metadata.json";
    CountDownLatch committed = new CountDownLatch(1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    AtomicReference<Thread> competitor = new AtomicReference<>();
    // The second read is the one under the lock, so the competitor starts while it is held.
    stand.onRead = read -> {
      if (read != 2) {
        return;
      }
      Thread thread = new Thread(() -> {
        try {
          commitLikeIceberg(stand, next, CURRENT);
        } catch (Throwable throwable) {
          failure.set(throwable);
        } finally {
          committed.countDown();
        }
      }, "competing-commit");
      competitor.set(thread);
      thread.start();
    };

    invokeAlter(stand, hiveInsertAlter(), dropPropsContext());

    Assert.assertTrue("the competing commit never finished", committed.await(5L, TimeUnit.SECONDS));
    Assert.assertNull(String.valueOf(failure.get()), failure.get());
    competitor.get().join(5_000L);
    Assert.assertEquals(
        "the repair must land inside the lock and the waiting commit after it, not the other way round",
        List.of("alter:" + CURRENT, "alter:" + next),
        stand.events.stream().filter(event -> event.startsWith("alter:")).toList());
    Assert.assertEquals("the commit that waited is the state the metastore ends up with",
        next, stand.record.get().getParameters().get("metadata_location"));
    Assert.assertFalse("the guard must not leave the table locked", stand.holdsLock());
  }

  /**
   * A genuine Iceberg commit sends its {@code alter_table} from inside the table lock it already
   * holds. Asking for that lock here would mean waiting for the caller of this very call, so the
   * guard must not ask for it - this is the regression test for that self-deadlock.
   */
  @Test
  public void aForwardCommitNeverAsksForTheLockItsCallerIsHolding() throws Throwable {
    Stand stand = newStand(icebergRecord(CURRENT, PREVIOUS));

    invokeAlter(stand, icebergTable("hdfs://nn/next.json", CURRENT), null);

    Assert.assertTrue("a forward commit must not request the lock its own caller holds",
        stand.lockRequests.isEmpty());
  }

  @Test
  public void anOrdinaryHiveTableIsNeverLocked() throws Throwable {
    Stand stand = newStand(hiveRecord());

    invokeAlter(stand, hiveInsertAlter(), dropPropsContext());

    Assert.assertTrue("ordinary Hive traffic must not pay for a lock", stand.lockRequests.isEmpty());
  }

  /**
   * The lock is only mutual exclusion if it is the same object Iceberg locks. Iceberg 1.6.1 (inside
   * HiveServer2) and 1.9.2 (the proxy's REST path) both send exactly this request.
   */
  @Test
  public void theLockRequestHasTheShapeIcebergItselfSends() throws Throwable {
    Stand stand = newStand(icebergRecord(CURRENT, PREVIOUS));

    invokeAlter(stand, hiveInsertAlter(), dropPropsContext());

    Assert.assertEquals(1, stand.lockRequests.size());
    LockRequest request = stand.lockRequests.get(0);
    Assert.assertEquals(1, request.getComponent().size());
    LockComponent component = request.getComponent().get(0);
    Assert.assertEquals(LockType.EXCLUSIVE, component.getType());
    Assert.assertEquals(LockLevel.TABLE, component.getLevel());
    Assert.assertEquals("the backend database name is what both writers' locks name",
        "sales", component.getDbname());
    Assert.assertEquals("events", component.getTablename());
    Assert.assertFalse("Iceberg's lock carries no transaction, and neither may this one",
        request.isSetTxnid());
    Assert.assertTrue("a stranded lock has to name what took it", request.isSetAgentInfo());
  }

  @Test
  public void theLockIsReleasedWhenTheBackendFailsTheAlter() throws Throwable {
    Stand stand = newStand(icebergRecord(CURRENT, PREVIOUS));
    stand.onRead = read -> {
      if (read == 2) {
        stand.failNextAlter = true;
      }
    };

    try {
      invokeAlter(stand, hiveInsertAlter(), dropPropsContext());
      Assert.fail("the backend failure must reach the client");
    } catch (Throwable expected) {
      // The client sees the metastore's own failure; what matters here is what happens to the lock.
    }

    Assert.assertFalse("a failed alter must not strand the table lock", stand.holdsLock());
  }

  /**
   * A lock that is not granted must never turn into a refused write: an ordinary Hive
   * {@code INSERT} failing because the metastore's lock table is busy is a worse failure than the
   * one being prevented. The repair still goes through, unprotected, and says so.
   */
  @Test
  public void aLockThatIsNeverGrantedStillLetsTheRepairedAlterThrough() throws Throwable {
    Stand stand = newStand(
        icebergRecord(CURRENT, PREVIOUS),
        MetastoreRuntimeProfile.APACHE_3_1_3,
        new IcebergPointerGuardConfig(true, 30_000L, 10_000, true, 120L, true));
    stand.lockNeverGranted = true;

    invokeAlter(stand, hiveInsertAlter(), dropPropsContext());

    Assert.assertEquals("the alter must still be repaired and forwarded",
        CURRENT, stand.forwarded.get().getParameters().get("metadata_location"));
    Assert.assertTrue("the lock that was never granted has to be given back",
        stand.events.stream().anyMatch(event -> event.startsWith("unlock:"))
            || !stand.holdsLock());
    String metrics = stand.observability.metrics().render();
    Assert.assertTrue(metrics, metrics.contains("hms_proxy_iceberg_pointer_guard_events_total"
        + "{catalog=\"catalog1\",outcome=\"repair_lock_timeout\"} 1"));
  }

  @Test
  public void aFailingLockCallStillLetsTheRepairedAlterThrough() throws Throwable {
    Stand stand = newStand(icebergRecord(CURRENT, PREVIOUS));
    stand.lockCallFails = true;

    invokeAlter(stand, hiveInsertAlter(), dropPropsContext());

    Assert.assertEquals(CURRENT, stand.forwarded.get().getParameters().get("metadata_location"));
    String metrics = stand.observability.metrics().render();
    Assert.assertTrue(metrics, metrics.contains("hms_proxy_iceberg_pointer_guard_events_total"
        + "{catalog=\"catalog1\",outcome=\"repair_lock_failed\"} 1"));
  }

  /**
   * A commit lands between the two reads, and the request turns out to be built on exactly what
   * the table holds now - an honest commit after all. It passes through untouched, and the lock is
   * given back rather than held over someone else's write.
   */
  @Test
  public void aRequestThatBecomesAForwardCommitUnderTheLockPassesThroughAndUnlocks() throws Throwable {
    Stand stand = newStand(icebergRecord(CURRENT, PREVIOUS));
    String next = "hdfs://nn/warehouse/sales/events/metadata/00007-next.metadata.json";
    landCommitDuringRead(stand, 1, next);

    invokeAlter(stand, icebergTable("hdfs://nn/00008.json", next), null);

    Assert.assertEquals("a commit built on what the table now holds must not be rewritten",
        "hdfs://nn/00008.json", stand.forwarded.get().getParameters().get("metadata_location"));
    Assert.assertFalse("the lock must not be held over a request the guard decided not to repair",
        stand.holdsLock());
  }

  /**
   * A lock on a non-default catalog's backend would be real and pointless: writers of those
   * catalogs are served by the synthetic shim, which grants locks without checking conflicts and
   * never forwards them to the backend, so nothing contends for the object it would hold. The
   * repair still happens; only the two RPCs for an illusion of mutual exclusion are dropped.
   */
  @Test
  public void aNonDefaultCatalogIsRepairedWithoutALockBecauseItsWritersAreNotLocked() throws Throwable {
    Stand stand = newStand(
        icebergRecord(CURRENT, PREVIOUS),
        MetastoreRuntimeProfile.APACHE_3_1_3,
        IcebergPointerGuardConfig.defaults(),
        true);

    Method alter = ThriftHiveMetastore.Iface.class.getMethod(
        "alter_table_with_environment_context", String.class, String.class, Table.class, EnvironmentContext.class);
    Table incoming = hiveInsertAlter();
    incoming.setDbName("catalog2__sales");
    invoke(stand.handler, alter, "catalog2__sales", "events", incoming, dropPropsContext());

    Assert.assertEquals("the pointer is still kept on every catalog",
        CURRENT, stand.forwarded.get().getParameters().get("metadata_location"));
    Assert.assertTrue("a lock nobody else takes must not be requested", stand.lockRequests.isEmpty());
    String metrics = stand.observability.metrics().render();
    Assert.assertTrue(metrics, metrics.contains("hms_proxy_iceberg_pointer_guard_events_total"
        + "{catalog=\"catalog2\",outcome=\"repair_lock_skipped\"} 1"));
  }

  /** Iceberg's commit, as the proxy sees it: lock the table, alter, unlock. */
  private static void commitLikeIceberg(Stand stand, String metadataLocation, String base) throws Throwable {
    Method lock = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);
    Method checkLock = ThriftHiveMetastore.Iface.class.getMethod("check_lock", CheckLockRequest.class);
    Method unlock = ThriftHiveMetastore.Iface.class.getMethod("unlock", UnlockRequest.class);
    LockComponent component = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, "sales");
    component.setTablename("events");
    LockResponse response = (LockResponse) invoke(
        stand.handler, lock, new LockRequest(List.of(component), "hive", "host"));
    long lockId = response.getLockid();
    while (response.getState() == LockState.WAITING) {
      Thread.sleep(10L);
      response = (LockResponse) invoke(stand.handler, checkLock, new CheckLockRequest(lockId));
    }
    try {
      invokeAlter(stand, icebergTable(metadataLocation, base), null);
    } finally {
      invoke(stand.handler, unlock, new UnlockRequest(lockId));
    }
  }

  /** Lands a committed snapshot while the guard is reading the record for the n-th time. */
  private static void landCommitDuringRead(Stand stand, int read, String metadataLocation) {
    stand.onRead = observed -> {
      if (observed != read) {
        return;
      }
      stand.onRead = ignored -> { };
      Table committed = icebergRecord(metadataLocation, CURRENT);
      stand.record.set(committed);
    };
  }

  private static void invokeAlter(Stand stand, Table table, EnvironmentContext context) throws Throwable {
    Method alter = ThriftHiveMetastore.Iface.class.getMethod(
        "alter_table_with_environment_context", String.class, String.class, Table.class, EnvironmentContext.class);
    invoke(stand.handler, alter, "sales", "events", table, context);
  }

  private static Object invoke(ThriftHiveMetastore.Iface handler, Method method, Object... args) throws Throwable {
    return ((RoutingMetaStoreProxy) java.lang.reflect.Proxy.getInvocationHandler(handler))
        .invoke(null, method, args);
  }

  /**
   * The proxy under test with the metastore record it serves and the calls it received.
   *
   * <p>The fake keeps one EXCLUSIVE table lock, the way a metastore does: a second request for it
   * is answered WAITING until the holder unlocks. That is what makes the guard's lock more than a
   * pair of RPCs in these tests - a competing commit really does have to wait for it.
   */
  private static final class Stand {
    private final AtomicReference<Table> record = new AtomicReference<>();
    private final AtomicReference<Table> forwarded = new AtomicReference<>();
    private final AtomicReference<EnvironmentContext> context = new AtomicReference<>();
    private final AtomicInteger reads = new AtomicInteger();
    private final AtomicLong nextLockId = new AtomicLong();
    private final AtomicLong lockHolder = new AtomicLong();
    private final List<LockRequest> lockRequests = new CopyOnWriteArrayList<>();
    private final List<String> events = new CopyOnWriteArrayList<>();
    /** Answers every lock request WAITING, the way a metastore does while someone else holds it. */
    private volatile boolean lockNeverGranted;
    /** Fails the lock call itself, the way a metastore without its ACID tables does. */
    private volatile boolean lockCallFails;
    /** Run when the guard reads the record for the n-th time; lets a test land a commit mid-repair. */
    private volatile IntConsumer onRead = read -> { };
    /** Fails the next alter, to check what happens to a lock held across a failing backend call. */
    private volatile boolean failNextAlter;
    private ProxyObservability observability;
    private ThriftHiveMetastore.Iface handler;

    private boolean holdsLock() {
      return lockHolder.get() != 0L;
    }
  }

  private static Stand newStand(Table record) throws Exception {
    return newStand(record, MetastoreRuntimeProfile.APACHE_3_1_3);
  }

  private static Stand newStand(Table record, MetastoreRuntimeProfile runtimeProfile) throws Exception {
    return newStand(record, runtimeProfile, IcebergPointerGuardConfig.defaults());
  }

  private static Stand newStand(
      Table record,
      MetastoreRuntimeProfile runtimeProfile,
      IcebergPointerGuardConfig guardConfig
  ) throws Exception {
    return newStand(record, runtimeProfile, guardConfig, false);
  }

  private static Stand newStand(
      Table record,
      MetastoreRuntimeProfile runtimeProfile,
      IcebergPointerGuardConfig guardConfig,
      boolean withNonDefaultCatalog
  ) throws Exception {
    Stand stand = new Stand();
    stand.record.set(record);
    Map<String, io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig> catalogs = new LinkedHashMap<>();
    catalogs.put("catalog1",
        catalogConfig("catalog1", "c1", runtimeProfile, null, Map.of("hive.metastore.uris", "thrift://one")));
    if (withNonDefaultCatalog) {
      catalogs.put("catalog2",
          catalogConfig("catalog2", "c2", runtimeProfile, null, Map.of("hive.metastore.uris", "thrift://two")));
    }
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(catalogs)
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .icebergPointerGuard(guardConfig)
        .build();

    java.lang.reflect.InvocationHandler metastore = (proxy, method, args) -> {
          switch (method.getName()) {
            case "get_table": {
              // Hive 4 dropped the positional read from its IDL; its isolated client answers a
              // call for it exactly like this, so a guard that bypasses the adapter is caught here.
              if (runtimeProfile == MetastoreRuntimeProfile.APACHE_4_1_0) {
                throw new NoSuchMethodException("get_table");
              }
              int read = stand.reads.incrementAndGet();
              // A real read is a fresh deserialization, never the stored object.
              Table answer = copyOf(stand.record.get());
              stand.onRead.accept(read);
              return answer;
            }
            case "get_table_req": {
              int read = stand.reads.incrementAndGet();
              Table answer = copyOf(stand.record.get());
              stand.onRead.accept(read);
              return new GetTableResult(answer);
            }
            case "alter_table_with_environment_context":
              if (stand.failNextAlter) {
                stand.failNextAlter = false;
                throw new MetaException("the metastore refused this alter");
              }
              stand.forwarded.set((Table) args[2]);
              stand.context.set((EnvironmentContext) args[3]);
              stand.events.add("alter:" + ((Table) args[2]).getParameters().get("metadata_location"));
              // A metastore applies the alter, so a rolled-back pointer stays visible to the next
              // reader - which is the whole defect under test.
              stand.record.set(copyOf((Table) args[2]));
              return null;
            case "create_table":
              return null;
            case "lock": {
              if (stand.lockCallFails) {
                throw new MetaException("no lock tables in this metastore");
              }
              stand.lockRequests.add((LockRequest) args[0]);
              long lockId = stand.nextLockId.incrementAndGet();
              if (!stand.lockNeverGranted && stand.lockHolder.compareAndSet(0L, lockId)) {
                stand.events.add("lock:" + lockId);
                return new LockResponse(lockId, LockState.ACQUIRED);
              }
              return new LockResponse(lockId, LockState.WAITING);
            }
            case "check_lock": {
              long lockId = ((CheckLockRequest) args[0]).getLockid();
              if (!stand.lockNeverGranted && stand.lockHolder.compareAndSet(0L, lockId)) {
                stand.events.add("lock:" + lockId);
                return new LockResponse(lockId, LockState.ACQUIRED);
              }
              return new LockResponse(lockId, LockState.WAITING);
            }
            case "unlock": {
              long lockId = ((UnlockRequest) args[0]).getLockid();
              if (stand.lockHolder.compareAndSet(lockId, 0L)) {
                stand.events.add("unlock:" + lockId);
              }
              return null;
            }
            default:
              throw new UnsupportedOperationException(method.getName());
          }
    };
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    for (String catalog : catalogs.keySet()) {
      backends.put(catalog, newBackend(
          config,
          config.catalogs().get(catalog),
          adapterFor(runtimeProfile),
          newBackendRuntime(config, config.catalogs().get(catalog), newSession(metastore))));
    }
    CatalogRouter router = new CatalogRouter(config, backends);
    stand.observability = new ProxyObservability(config);
    RoutingMetaStoreProxy proxy = new RoutingMetaStoreProxy(
        config, router, new FederationLayer(config, router), null, stand.observability);
    stand.handler = (ThriftHiveMetastore.Iface) java.lang.reflect.Proxy.newProxyInstance(
        ThriftHiveMetastore.Iface.class.getClassLoader(),
        new Class<?>[] {ThriftHiveMetastore.Iface.class},
        proxy);
    return stand;
  }

  /**
   * The real adapters, not stubs: the guard has to read the record through a path each backend
   * line actually implements. Hive 4 dropped the positional {@code get_table} from its IDL, so
   * only {@code Hive4BackendAdapter} - which upgrades it to {@code get_table_req} - can serve the
   * read there, and a read that bypasses the adapter fails with {@code NoSuchMethodException} on
   * the very backend where the compare-and-swap matters. Its constructor is package-private, so
   * the test reaches it the same way it reaches {@code CatalogBackend}'s.
   */
  private static BackendAdapter adapterFor(MetastoreRuntimeProfile runtimeProfile) throws Exception {
    if (runtimeProfile != MetastoreRuntimeProfile.APACHE_4_1_0) {
      return new ApacheBackendAdapter();
    }
    Constructor<Hive4BackendAdapter> ctor = Hive4BackendAdapter.class.getDeclaredConstructor();
    ctor.setAccessible(true);
    return ctor.newInstance();
  }

  /** The Iceberg record a metastore holds: the pointer plus everything Iceberg wrote next to it. */
  private static Table icebergRecord(String metadataLocation, String previousMetadataLocation) {
    Table table = icebergTable(metadataLocation, previousMetadataLocation);
    table.setSd(hiveEngineDescriptor());
    table.getParameters().put("storage_handler", "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler");
    table.getParameters().put("current-snapshot-id", "7051323893436947220");
    table.getParameters().put("current-snapshot-summary", "{\"added-records\":\"1\"}");
    table.getParameters().put("EXTERNAL", "TRUE");
    return table;
  }

  private static Table hiveRecord() {
    Table table = new Table();
    table.setDbName("sales");
    table.setTableName("events");
    table.setParameters(new HashMap<>(Map.of("EXTERNAL", "TRUE", "numRows", "3")));
    return table;
  }

  /**
   * What HiveServer2 actually sends when an {@code INSERT} opens, verified on the wire: the
   * statistics it snapshotted at compile time and not one Iceberg key.
   */
  private static Table hiveInsertAlter() {
    Table table = new Table();
    table.setDbName("sales");
    table.setTableName("events");
    Map<String, String> parameters = new HashMap<>();
    parameters.put("EXTERNAL", "TRUE");
    parameters.put("numFiles", "2");
    parameters.put("numRows", "17");
    parameters.put("totalSize", "4096");
    parameters.put("transient_lastDdlTime", "1753900000");
    table.setParameters(parameters);
    return table;
  }

  private static Table icebergTable(String metadataLocation, String previousMetadataLocation) {
    Table table = new Table();
    table.setDbName("sales");
    table.setTableName("events");
    Map<String, String> parameters = new HashMap<>();
    parameters.put("table_type", "ICEBERG");
    parameters.put("metadata_location", metadataLocation);
    if (previousMetadataLocation != null) {
      parameters.put("previous_metadata_location", previousMetadataLocation);
    }
    table.setParameters(parameters);
    return table;
  }

  /**
   * What Iceberg writes when the Hive engine is enabled: the concrete classes a Hive 3.1 planner
   * can instantiate. This is the shape a Hive 4 {@code STORED BY ICEBERG} create leaves behind,
   * measured on the stand.
   */
  private static StorageDescriptor hiveEngineDescriptor() {
    StorageDescriptor descriptor = new StorageDescriptor();
    descriptor.setLocation("hdfs://nn/warehouse/sales/events");
    descriptor.setInputFormat("org.apache.iceberg.mr.hive.HiveIcebergInputFormat");
    descriptor.setOutputFormat("org.apache.iceberg.mr.hive.HiveIcebergOutputFormat");
    SerDeInfo serde = new SerDeInfo();
    serde.setSerializationLib("org.apache.iceberg.mr.hive.HiveIcebergSerDe");
    serde.setParameters(new HashMap<>());
    descriptor.setSerdeInfo(serde);
    return descriptor;
  }

  /**
   * What Iceberg writes when it computes the Hive engine to be disabled - the abstract base
   * classes Hive 3.1 dies on with "Cannot create an instance of InputFormat class
   * org.apache.hadoop.mapred.FileInputFormat".
   */
  private static StorageDescriptor plainFilesDescriptor() {
    StorageDescriptor descriptor = new StorageDescriptor();
    descriptor.setLocation("hdfs://nn/warehouse/sales/events");
    descriptor.setInputFormat("org.apache.hadoop.mapred.FileInputFormat");
    descriptor.setOutputFormat("org.apache.hadoop.mapred.FileOutputFormat");
    SerDeInfo serde = new SerDeInfo();
    serde.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
    serde.setParameters(new HashMap<>());
    descriptor.setSerdeInfo(serde);
    return descriptor;
  }

  private static Table copyOf(Table table) {
    Table copy = new Table(table);
    copy.setParameters(new HashMap<>(table.getParameters()));
    return copy;
  }

  private static EnvironmentContext dropPropsContext() {
    EnvironmentContext context = new EnvironmentContext();
    context.setProperties(new HashMap<>(Map.of("DO_NOT_UPDATE_STATS", "true", "alterTableOpType", "DROPPROPS")));
    return context;
  }
}
