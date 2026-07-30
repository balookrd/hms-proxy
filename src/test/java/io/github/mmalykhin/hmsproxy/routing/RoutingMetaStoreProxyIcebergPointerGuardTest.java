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
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
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
        hiveRecord(), MetastoreRuntimeProfile.APACHE_3_1_3, new IcebergPointerGuardConfig(true, 1L, 10_000));

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

    Assert.assertEquals(2, stand.reads.get());
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

    Assert.assertEquals("the cached 'not an Iceberg table' answer must not survive the create",
        2, stand.reads.get());
    Assert.assertEquals(CURRENT, stand.forwarded.get().getParameters().get("metadata_location"));
  }

  @Test
  public void aDisabledGuardReadsNothingAndRewritesNothing() throws Throwable {
    Stand stand = newStand(
        icebergRecord(CURRENT, PREVIOUS),
        MetastoreRuntimeProfile.APACHE_3_1_3,
        new IcebergPointerGuardConfig(false, 30_000L, 10_000));

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

    Assert.assertEquals("a Hive 4 backend has no positional get_table, and the read must not be"
            + " lost to that - the guard has to reach the record through the adapter",
        1, stand.reads.get());
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
    String hiveMetrics = hive.observability.metrics().render();
    Assert.assertTrue(hiveMetrics,
        hiveMetrics.contains("hms_proxy_iceberg_pointer_guard_events_total"
            + "{catalog=\"catalog1\",outcome=\"not_iceberg\"} 1"));
    Assert.assertTrue(hiveMetrics,
        hiveMetrics.contains("hms_proxy_iceberg_pointer_guard_events_total"
            + "{catalog=\"catalog1\",outcome=\"cache_suppressed\"} 1"));
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

  /** The proxy under test with the metastore record it serves and the calls it received. */
  private static final class Stand {
    private final AtomicReference<Table> record = new AtomicReference<>();
    private final AtomicReference<Table> forwarded = new AtomicReference<>();
    private final AtomicReference<EnvironmentContext> context = new AtomicReference<>();
    private final AtomicInteger reads = new AtomicInteger();
    private ProxyObservability observability;
    private ThriftHiveMetastore.Iface handler;
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
    Stand stand = new Stand();
    stand.record.set(record);
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1",
            catalogConfig("catalog1", "c1", runtimeProfile, null, Map.of("hive.metastore.uris", "thrift://one"))))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .icebergPointerGuard(guardConfig)
        .build();

    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        adapterFor(runtimeProfile),
        newBackendRuntime(config, config.catalogs().get("catalog1"), newSession((proxy, method, args) -> {
          switch (method.getName()) {
            case "get_table":
              // Hive 4 dropped the positional read from its IDL; its isolated client answers a
              // call for it exactly like this, so a guard that bypasses the adapter is caught here.
              if (runtimeProfile == MetastoreRuntimeProfile.APACHE_4_1_0) {
                throw new NoSuchMethodException("get_table");
              }
              stand.reads.incrementAndGet();
              // A real read is a fresh deserialization, never the stored object.
              return copyOf(stand.record.get());
            case "get_table_req":
              stand.reads.incrementAndGet();
              return new GetTableResult(copyOf(stand.record.get()));
            case "alter_table_with_environment_context":
              stand.forwarded.set((Table) args[2]);
              stand.context.set((EnvironmentContext) args[3]);
              return null;
            case "create_table":
              return null;
            default:
              throw new UnsupportedOperationException(method.getName());
          }
        })));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", backend);
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
