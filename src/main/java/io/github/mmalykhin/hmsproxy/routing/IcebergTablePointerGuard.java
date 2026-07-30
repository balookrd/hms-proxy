package io.github.mmalykhin.hmsproxy.routing;


import io.github.mmalykhin.hmsproxy.config.routing.IcebergPointerGuardConfig;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps a Hive client's own {@code alter_table} from erasing an Iceberg table's state.
 *
 * <p>A HiveServer2 {@code INSERT} into an Iceberg table opens with an
 * {@code alter_table_with_environment_context} carrying {@code alterTableOpType=DROPPROPS} and the
 * {@code Table} object the query snapshotted at compile time. A metastore applies those parameters
 * wholesale, so everything the record holds and the request omits is dropped -
 * {@code metadata_location} included - and the call travels outside the Iceberg lock (Hive holds
 * only its own txn lock, on the {@code _dummy_database} placeholder), so nothing serializes it
 * against a committing writer. The stand reproduced exactly that as lost rows: the pointer went
 * 00006 -> 00005 -> 00006', and the REST writer whose commit sat at the first 00006 had reported
 * success.
 *
 * <p>Whether a request concerns an Iceberg table is decided from <b>the metastore's record, not
 * from the request</b>: the shape HiveServer2 actually sends carries no Iceberg key at all
 * ({@code EXTERNAL}, {@code numFiles}, {@code numRows}, {@code totalSize},
 * {@code transient_lastDdlTime} and nothing else), so a guard keyed off the incoming table is a
 * no-op for the very shape that loses data. This is the only place that decision is made; no other
 * class should grow its own "is this Iceberg" check.
 *
 * <p>The repair is a merge: the record's current parameters as the base, the incoming ones applied
 * on top, and both pointers forced back to what the metastore holds. Everything the client meant
 * to change still goes through, and nothing it silently omitted is lost. Merging rather than
 * restoring a fixed list of Iceberg keys is deliberate - the key set Iceberg writes into HMS
 * varies by version, and a list would rot. Iceberg's own commits are told apart the way Iceberg
 * itself defines a commit: the {@code previous_metadata_location} it sends is the pointer it read
 * as the base, so a request whose base is the current pointer is moving the table forward and is
 * passed through untouched.
 *
 * <p>Keying off the metastore means a {@code get_table} per {@code alter_table}, ordinary Hive
 * tables included. That is bounded by a negative cache of names the metastore answered are not
 * Iceberg tables; Iceberg tables are never cached, because their pointer has to be read fresh.
 */
final class IcebergTablePointerGuard {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergTablePointerGuard.class);
  private static final String METADATA_LOCATION = "metadata_location";
  private static final String PREVIOUS_METADATA_LOCATION = "previous_metadata_location";
  private static final String EXPECTED_PARAMETER_KEY = "expected_parameter_key";
  private static final String EXPECTED_PARAMETER_VALUE = "expected_parameter_value";
  private static final String OUTCOME_REPAIRED = "repaired";
  private static final String OUTCOME_FORWARD_COMMIT = "forward_commit";
  private static final String OUTCOME_NOT_ICEBERG = "not_iceberg";
  private static final String OUTCOME_CACHE_SUPPRESSED = "cache_suppressed";
  private static final String OUTCOME_READ_FAILED = "read_failed";
  private static final Method GET_TABLE = getTableMethod();

  private final RoutingSupport support;
  private final IcebergPointerGuardConfig config;
  /** Names known not to be Iceberg tables, each valid until its {@code System.nanoTime} deadline. */
  private final ConcurrentMap<TableKey, Long> notIcebergUntilNanos = new ConcurrentHashMap<>();

  IcebergTablePointerGuard(RoutingSupport support) {
    this.support = support;
    this.config = support.config.icebergPointerGuard();
  }

  static boolean appliesTo(String methodName) {
    return methodName != null && methodName.startsWith("alter_table");
  }

  private static boolean createsTable(String methodName) {
    return methodName != null && methodName.startsWith("create_table");
  }

  /**
   * Rewrites, in place, the {@code Table} of an {@code alter_table} that would erase the Iceberg
   * state the metastore currently holds. Never throws: a table that cannot be read back is left to
   * the backend to judge, because refusing the alter outright would fail an ordinary Hive write
   * whenever the backend hiccups.
   */
  void protectPointer(Object[] routedArgs, CatalogRouter.ResolvedNamespace namespace, String methodName) {
    if (routedArgs == null || !config.enabled()
        || (!appliesTo(methodName) && !createsTable(methodName))) {
      return;
    }
    Table incoming = tableArgument(routedArgs);
    if (incoming == null) {
      return;
    }
    String backendDbName = argumentOrTableField(routedArgs, 0, incoming.getDbName());
    String tableName = argumentOrTableField(routedArgs, 1, incoming.getTableName());
    if (backendDbName == null || tableName == null) {
      return;
    }
    TableKey key = new TableKey(namespace.catalogName(), backendDbName, tableName);
    if (createsTable(methodName)) {
      // A name that is being created as an Iceberg table must not stay remembered as a plain one.
      if (parameter(incoming, METADATA_LOCATION) != null) {
        notIcebergUntilNanos.remove(key);
      }
      return;
    }

    String incomingLocation = parameter(incoming, METADATA_LOCATION);
    if (incomingLocation != null) {
      // The request itself says this name is an Iceberg table now, whoever made it one.
      notIcebergUntilNanos.remove(key);
    } else if (knownNotToBeIceberg(key)) {
      record(namespace, OUTCOME_CACHE_SUPPRESSED);
      return;
    }

    Table current = readCurrentTable(namespace, backendDbName, tableName);
    if (current == null) {
      record(namespace, OUTCOME_READ_FAILED);
      return;
    }
    String currentLocation = parameter(current, METADATA_LOCATION);
    if (currentLocation == null) {
      rememberNotIceberg(key);
      record(namespace, OUTCOME_NOT_ICEBERG);
      return;
    }
    if (currentLocation.equals(parameter(incoming, PREVIOUS_METADATA_LOCATION))) {
      // A commit built on what the metastore holds now - the table is moving forward.
      record(namespace, OUTCOME_FORWARD_COMMIT);
      return;
    }

    attachCompareAndSwap(routedArgs, namespace, currentLocation);
    mergeOverCurrentRecord(incoming, current, currentLocation);
    record(namespace, OUTCOME_REPAIRED);
    LOG.warn(
        "requestId={} kept the current Iceberg state for catalog '{}' db='{}' table='{}': {} sent"
            + " metadata_location='{}' while the metastore holds '{}'. Applying it as sent would"
            + " have discarded a committed snapshot.",
        RequestContext.currentRequestId(),
        namespace.catalogName(),
        backendDbName,
        tableName,
        methodName,
        incomingLocation == null ? "<absent>" : incomingLocation,
        currentLocation);
  }

  /**
   * Rebuilds the parameters the backend will apply: the metastore's record as the base, the
   * client's own parameters on top, and both pointers forced back to the record's values. The
   * price of the merge is that an {@code alter_table} which legitimately removes a property from
   * an Iceberg table becomes a no-op - acceptable, because Hive's own {@code UNSET TBLPROPERTIES}
   * goes through {@code HiveIcebergMetaHook} and arrives as a forward commit, which is never
   * merged.
   */
  private static void mergeOverCurrentRecord(Table incoming, Table current, String currentLocation) {
    Map<String, String> merged = new LinkedHashMap<>();
    if (current.isSetParameters()) {
      merged.putAll(current.getParameters());
    }
    if (incoming.isSetParameters()) {
      merged.putAll(incoming.getParameters());
    }
    merged.put(METADATA_LOCATION, currentLocation);
    String currentPrevious = parameter(current, PREVIOUS_METADATA_LOCATION);
    if (currentPrevious == null) {
      merged.remove(PREVIOUS_METADATA_LOCATION);
    } else {
      merged.put(PREVIOUS_METADATA_LOCATION, currentPrevious);
    }
    incoming.setParameters(merged);
  }

  /**
   * Makes the repaired alter conditional where the metastore can check the condition: with
   * {@code expected_parameter_key}/{@code expected_parameter_value} set, it applies the alter
   * only while {@code metadata_location} still holds the value that was just read, so a commit
   * that lands between this guard's read and the backend's write fails the alter loudly instead
   * of being silently discarded. Without it the repair narrows that window but cannot close it.
   *
   * <p>Only Hive 4 metastores implement the check - the 3.1 line ignores both keys - so they are
   * sent only to a Hive 4 backend. Sending them to a metastore that drops them would buy nothing
   * but the false impression that the window is closed.
   */
  private void attachCompareAndSwap(
      Object[] routedArgs,
      CatalogRouter.ResolvedNamespace namespace,
      String expectedLocation
  ) {
    if (!namespace.backend().runtimeProfile().isHive4()) {
      return;
    }
    for (Object arg : routedArgs) {
      if (!(arg instanceof EnvironmentContext context)) {
        continue;
      }
      Map<String, String> properties = context.getProperties();
      if (properties == null) {
        properties = new HashMap<>();
        context.setProperties(properties);
      }
      properties.put(EXPECTED_PARAMETER_KEY, METADATA_LOCATION);
      properties.put(EXPECTED_PARAMETER_VALUE, expectedLocation);
      return;
    }
  }

  private boolean knownNotToBeIceberg(TableKey key) {
    Long expiresAtNanos = notIcebergUntilNanos.get(key);
    if (expiresAtNanos == null) {
      return false;
    }
    if (expiresAtNanos - System.nanoTime() <= 0L) {
      notIcebergUntilNanos.remove(key, expiresAtNanos);
      return false;
    }
    return true;
  }

  private void rememberNotIceberg(TableKey key) {
    long ttlMs = config.tableCacheTtlMs();
    if (ttlMs <= 0L) {
      return;
    }
    if (notIcebergUntilNanos.size() >= config.tableCacheMaxEntries()) {
      evict();
    }
    notIcebergUntilNanos.put(key, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(ttlMs));
  }

  /** Expired entries go first; if the cache is still full it is dropped whole rather than grown. */
  private void evict() {
    long now = System.nanoTime();
    notIcebergUntilNanos.values().removeIf(expiresAtNanos -> expiresAtNanos - now <= 0L);
    if (notIcebergUntilNanos.size() >= config.tableCacheMaxEntries()) {
      notIcebergUntilNanos.clear();
    }
  }

  private void record(CatalogRouter.ResolvedNamespace namespace, String outcome) {
    support.observability.metrics().recordIcebergPointerGuardEvent(namespace.catalogName(), outcome);
  }

  private Table readCurrentTable(
      CatalogRouter.ResolvedNamespace namespace,
      String backendDbName,
      String tableName
  ) {
    try {
      // Through the adapter, not by raw method name: Hive 4 has no positional get_table in its
      // IDL, and only the adapter knows to upgrade the read to get_table_req there. A raw
      // by-name call fails with NoSuchMethodException on exactly the backend whose
      // compare-and-swap this guard depends on.
      return (Table) support.invokeDirect(
          namespace.backend(), GET_TABLE, new Object[] {backendDbName, tableName});
    } catch (Throwable throwable) {
      LOG.warn(
          "requestId={} unable to read the current Iceberg pointer for catalog '{}' db='{}'"
              + " table='{}', letting the alter through unchanged: {}",
          RequestContext.currentRequestId(),
          namespace.catalogName(),
          backendDbName,
          tableName,
          throwable.toString());
      return null;
    }
  }

  private static Method getTableMethod() {
    try {
      return ThriftHiveMetastore.Iface.class.getMethod("get_table", String.class, String.class);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException("The frontend metastore API has no get_table(db, table)", e);
    }
  }

  private static Table tableArgument(Object[] routedArgs) {
    for (Object arg : routedArgs) {
      if (arg instanceof Table table) {
        return table;
      }
    }
    return null;
  }

  /**
   * {@code alter_table*} names its target in the first two arguments; {@code create_table*} names
   * it only inside the table itself.
   */
  private static String argumentOrTableField(Object[] routedArgs, int index, String tableField) {
    if (routedArgs.length > index && routedArgs[index] instanceof String value) {
      return RoutingSupport.blankToNull(value);
    }
    return RoutingSupport.blankToNull(tableField);
  }

  private static String parameter(Table table, String key) {
    if (table == null || !table.isSetParameters()) {
      return null;
    }
    Map<String, String> parameters = table.getParameters();
    String value = parameters.get(key);
    return value == null || value.isBlank() ? null : value;
  }

  private record TableKey(String catalog, String backendDbName, String tableName) {
  }
}
