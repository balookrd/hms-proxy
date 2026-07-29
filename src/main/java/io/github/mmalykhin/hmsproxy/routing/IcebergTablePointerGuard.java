package io.github.mmalykhin.hmsproxy.routing;


import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps a Hive client's own {@code alter_table} from rolling an Iceberg table's pointer back.
 *
 * <p>A HiveServer2 {@code INSERT} into an Iceberg table opens with an
 * {@code alter_table_with_environment_context} carrying {@code alterTableOpType=DROPPROPS} and the
 * {@code Table} object the query snapshotted at compile time. A metastore applies those parameters
 * wholesale - {@code metadata_location} included - so any commit that landed in between is erased,
 * and the call travels outside the Iceberg lock (Hive holds only its own txn lock, on the
 * {@code _dummy_database} placeholder), so nothing serializes it against a committing writer. The
 * stand reproduced exactly that as lost rows: the pointer went 00006 -> 00005 -> 00006', and the
 * REST writer whose commit sat at the first 00006 had reported success.
 *
 * <p>The proxy is the only place the two paths meet, so the repair happens here: the pointer the
 * metastore currently holds is stitched back into the incoming table, and everything else the
 * client asked to change is passed through. Iceberg's own commits are untouched, and they are told
 * apart the way Iceberg itself defines a commit - the {@code previous_metadata_location} it sends
 * is the pointer it read as the base, so a request whose base is the current pointer is moving the
 * table forward, and one whose base is anything else is carrying a stale copy.
 */
final class IcebergTablePointerGuard {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergTablePointerGuard.class);
  private static final String METADATA_LOCATION = "metadata_location";
  private static final String PREVIOUS_METADATA_LOCATION = "previous_metadata_location";
  private static final String EXPECTED_PARAMETER_KEY = "expected_parameter_key";
  private static final String EXPECTED_PARAMETER_VALUE = "expected_parameter_value";

  private final RoutingSupport support;

  IcebergTablePointerGuard(RoutingSupport support) {
    this.support = support;
  }

  static boolean appliesTo(String methodName) {
    return methodName != null && methodName.startsWith("alter_table");
  }

  /**
   * Rewrites, in place, the Iceberg pointer of an {@code alter_table} argument that carries a
   * stale one. Never throws: a table that cannot be read back is left to the backend to judge,
   * because refusing the alter outright would fail an ordinary Hive write.
   */
  void protectPointer(Object[] routedArgs, CatalogRouter.ResolvedNamespace namespace, String methodName) {
    if (!appliesTo(methodName) || routedArgs == null) {
      return;
    }
    Table incoming = tableArgument(routedArgs);
    String incomingLocation = parameter(incoming, METADATA_LOCATION);
    if (incomingLocation == null) {
      // Not an Iceberg table: nothing here to protect, and no extra round trip for the common case.
      return;
    }
    String backendDbName = routedArgs[0] instanceof String db ? db : null;
    String tableName = routedArgs[1] instanceof String name ? name : null;
    if (backendDbName == null || tableName == null) {
      return;
    }

    Table current = readCurrentTable(namespace, backendDbName, tableName);
    String currentLocation = parameter(current, METADATA_LOCATION);
    if (currentLocation == null || currentLocation.equals(incomingLocation)) {
      return;
    }
    if (currentLocation.equals(parameter(incoming, PREVIOUS_METADATA_LOCATION))) {
      // A commit built on what the metastore holds now - the table is moving forward.
      return;
    }

    attachCompareAndSwap(routedArgs, namespace, currentLocation);
    incoming.getParameters().put(METADATA_LOCATION, currentLocation);
    String currentPrevious = parameter(current, PREVIOUS_METADATA_LOCATION);
    if (currentPrevious == null) {
      incoming.getParameters().remove(PREVIOUS_METADATA_LOCATION);
    } else {
      incoming.getParameters().put(PREVIOUS_METADATA_LOCATION, currentPrevious);
    }
    LOG.warn(
        "requestId={} kept the current Iceberg pointer for catalog '{}' db='{}' table='{}': {} sent"
            + " metadata_location='{}' while the metastore holds '{}'. Applying it would have"
            + " discarded a committed snapshot.",
        RequestContext.currentRequestId(),
        namespace.catalogName(),
        backendDbName,
        tableName,
        methodName,
        incomingLocation,
        currentLocation);
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

  private Table readCurrentTable(
      CatalogRouter.ResolvedNamespace namespace,
      String backendDbName,
      String tableName
  ) {
    try {
      return (Table) support.invokeByReflection(
          namespace.backend(),
          "get_table",
          new Class<?>[] {String.class, String.class},
          new Object[] {backendDbName, tableName});
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

  private static Table tableArgument(Object[] routedArgs) {
    for (Object arg : routedArgs) {
      if (arg instanceof Table table) {
        return table;
      }
    }
    return null;
  }

  private static String parameter(Table table, String key) {
    if (table == null || !table.isSetParameters()) {
      return null;
    }
    Map<String, String> parameters = table.getParameters();
    String value = parameters.get(key);
    return value == null || value.isBlank() ? null : value;
  }
}
