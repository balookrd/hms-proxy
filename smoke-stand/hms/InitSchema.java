import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;

/**
 * Creates the ACID/txn tables (TXNS, HIVE_LOCKS, NEXT_TXN_ID, ...) in the metastore database.
 *
 * <p>The standalone-metastore jars in this repository ship without the schema .sql scripts, so
 * MetastoreSchemaTool cannot initialize a database from them. DataNucleus auto-creates the object
 * tables (DBS, TBLS, SDS) on first use, but never the transaction tables — those exist only in the
 * schema scripts, or through this utility. The smoke suite exercises txn and lock RPCs, so they
 * have to be here before the metastore starts.
 */
public final class InitSchema {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    for (String arg : args) {
      int eq = arg.indexOf('=');
      if (eq > 0) {
        conf.set(arg.substring(0, eq), arg.substring(eq + 1));
      }
    }
    TxnDbUtil.setConfValues(conf);
    try {
      TxnDbUtil.prepDb(conf);
      System.out.println("InitSchema: transaction tables created");
    } catch (Exception e) {
      // prepDb is not idempotent: on a warm volume the tables are already there.
      String message = String.valueOf(e.getMessage());
      if (message.contains("already exists") || message.contains("X0Y32")) {
        System.out.println("InitSchema: transaction tables already present, skipping");
        return;
      }
      throw e;
    }
  }
}
