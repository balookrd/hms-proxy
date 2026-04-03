package io.github.mmalykhin.hmsproxy.tools;

import io.github.mmalykhin.hmsproxy.backend.MetastoreApiClassLoader;
import io.github.mmalykhin.hmsproxy.security.KerberosPrincipalUtil;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatRequest;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.hadoop.security.UserGroupInformation;

public final class HmsMetastoreSmokeCli {
  private static final String MODE_TXN = "txn";
  private static final String MODE_LOCK = "lock";
  private static final String MODE_NOTIFICATION = "notification";
  private static final String AUTH_SIMPLE = "simple";
  private static final String AUTH_KERBEROS = "kerberos";
  private static final String TXN_CLOSE_ABORT = "abort";
  private static final String TXN_CLOSE_COMMIT = "commit";
  private static final String TXN_CLOSE_NONE = "none";

  private HmsMetastoreSmokeCli() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 0 || "--help".equals(args[0]) || "-h".equals(args[0])) {
      printUsage();
      return;
    }

    String mode = args[0];
    CliArgs cli = CliArgs.parse(Arrays.copyOfRange(args, 1, args.length));
    switch (mode) {
      case MODE_TXN -> runTxnSmoke(cli);
      case MODE_LOCK -> runLockSmoke(cli);
      case MODE_NOTIFICATION -> runNotificationSmoke(cli);
      default -> throw new IllegalArgumentException("Unknown mode: " + mode);
    }
  }

  private static void runTxnSmoke(CliArgs cli) throws Exception {
    String uri = cli.required("uri");
    String db = cli.required("db");
    String table = cli.required("table");
    String user = cli.getOrDefault("user", System.getProperty("user.name", "smoke-user"));
    String host = cli.getOrDefault("host", "localhost");
    String agentInfo = cli.getOrDefault("agent-info", "hms-proxy-smoke-cli");
    boolean doLock = cli.getBoolean("lock", true);

    applyOptionalKrbs(cli);
    HiveConf conf = baseConf(cli, uri);

    try (HiveMetaStoreClient client = openApacheClient(cli, conf)) {
      ThriftHiveMetastore.Iface thriftClient = extractThriftClient(client);
      OpenTxnRequest openReq = new OpenTxnRequest(1, user, host);
      openReq.setAgentInfo(agentInfo);
      OpenTxnsResponse openResp = thriftClient.open_txns(openReq);
      long txnId = openResp.getTxn_ids().get(0);
      System.out.println("open_txns txnId=" + txnId);

      AllocateTableWriteIdsRequest allocReq = new AllocateTableWriteIdsRequest(db, table);
      allocReq.setTxnIds(List.of(txnId));
      AllocateTableWriteIdsResponse allocResp = thriftClient.allocate_table_write_ids(allocReq);
      long writeId = allocResp.getTxnToWriteIds().get(0).getWriteId();
      System.out.println("allocate_table_write_ids writeId=" + writeId);

      if (doLock) {
        LockResponse lockResp = thriftClient.lock(buildTxnLockRequest(db, table, user, host, agentInfo, txnId));
        System.out.println("lock lockId=" + lockResp.getLockid() + " state=" + lockResp.getState());

        CheckLockRequest checkReq = new CheckLockRequest(lockResp.getLockid());
        checkReq.setTxnid(txnId);
        LockResponse checkResp = thriftClient.check_lock(checkReq);
        System.out.println("check_lock lockId=" + checkResp.getLockid() + " state=" + checkResp.getState());
        if (checkResp.getState() != LockState.ACQUIRED && checkResp.getState() != LockState.WAITING) {
          throw new IllegalStateException("Unexpected lock state: " + checkResp.getState());
        }
      }

      String validTxnList = cli.getOrDefault("valid-txn-list", "");
      GetValidWriteIdsRequest validReq =
          new GetValidWriteIdsRequest(List.of(db + "." + table), validTxnList);
      var validResp = thriftClient.get_valid_write_ids(validReq);
      System.out.println("get_valid_write_ids entries=" + validResp.getTblValidWriteIdsSize());
      if (validResp.getTblValidWriteIdsSize() > 0) {
        System.out.println("get_valid_write_ids first=" + validResp.getTblValidWriteIds().get(0));
      }

      thriftClient.commit_txn(new CommitTxnRequest(txnId));
      System.out.println("commit_txn txnId=" + txnId);
    }
  }

  private static void runLockSmoke(CliArgs cli) throws Exception {
    String uri = cli.required("uri");
    String db = cli.required("db");
    String user = cli.getOrDefault("user", System.getProperty("user.name", "smoke-user"));
    String host = cli.getOrDefault("host", "localhost");
    String agentInfo = cli.getOrDefault("agent-info", "hms-proxy-smoke-cli");
    boolean doHeartbeat = cli.getBoolean("heartbeat", true);
    boolean doUnlock = cli.getBoolean("unlock", true);
    String closeTxn = cli.getOrDefault("close-txn", TXN_CLOSE_ABORT).toLowerCase(Locale.ROOT);
    LockType lockType = parseEnumOption("lock-type", cli.required("lock-type"), LockType.class);
    LockLevel lockLevel = parseEnumOption("lock-level", cli.required("lock-level"), LockLevel.class);
    DataOperationType operationType =
        parseEnumOption("operation-type", cli.required("operation-type"), DataOperationType.class);
    boolean transactional = cli.getBoolean("transactional", false);

    String table = lockLevel == LockLevel.DB ? cli.get("table") : cli.required("table");
    String partition = lockLevel == LockLevel.PARTITION ? cli.required("partition") : cli.get("partition");

    if (!TXN_CLOSE_ABORT.equals(closeTxn)
        && !TXN_CLOSE_COMMIT.equals(closeTxn)
        && !TXN_CLOSE_NONE.equals(closeTxn)) {
      throw new IllegalArgumentException(
          "Invalid value for --close-txn: " + closeTxn + ". Expected one of: "
              + TXN_CLOSE_ABORT + ", " + TXN_CLOSE_COMMIT + ", " + TXN_CLOSE_NONE);
    }

    applyOptionalKrbs(cli);
    HiveConf conf = baseConf(cli, uri);

    try (HiveMetaStoreClient client = openApacheClient(cli, conf)) {
      ThriftHiveMetastore.Iface thriftClient = extractThriftClient(client);
      OpenTxnRequest openReq = new OpenTxnRequest(1, user, host);
      openReq.setAgentInfo(agentInfo);
      OpenTxnsResponse openResp = thriftClient.open_txns(openReq);
      long txnId = openResp.getTxn_ids().get(0);
      System.out.println("open_txns txnId=" + txnId);

      LockRequest request = buildLockRequest(
          db, table, partition, user, host, agentInfo, txnId, lockType, lockLevel, operationType, transactional);
      LockResponse lockResp = thriftClient.lock(request);
      System.out.println("lock lockId=" + lockResp.getLockid() + " state=" + lockResp.getState());

      CheckLockRequest checkReq = new CheckLockRequest(lockResp.getLockid());
      checkReq.setTxnid(txnId);
      LockResponse checkResp = thriftClient.check_lock(checkReq);
      System.out.println("check_lock lockId=" + checkResp.getLockid() + " state=" + checkResp.getState());
      if (checkResp.getState() != LockState.ACQUIRED && checkResp.getState() != LockState.WAITING) {
        throw new IllegalStateException("Unexpected lock state: " + checkResp.getState());
      }

      if (doHeartbeat) {
        HeartbeatRequest heartbeatRequest = new HeartbeatRequest();
        heartbeatRequest.setTxnid(txnId);
        heartbeatRequest.setLockid(lockResp.getLockid());
        thriftClient.heartbeat(heartbeatRequest);
        System.out.println("heartbeat txnId=" + txnId + " lockId=" + lockResp.getLockid());
      }

      if (doUnlock) {
        thriftClient.unlock(new UnlockRequest(lockResp.getLockid()));
        System.out.println("unlock lockId=" + lockResp.getLockid());
      }

      switch (closeTxn) {
        case TXN_CLOSE_ABORT -> {
          thriftClient.abort_txn(new AbortTxnRequest(txnId));
          System.out.println("abort_txn txnId=" + txnId);
        }
        case TXN_CLOSE_COMMIT -> {
          thriftClient.commit_txn(new CommitTxnRequest(txnId));
          System.out.println("commit_txn txnId=" + txnId);
        }
        default -> System.out.println("txn left open txnId=" + txnId);
      }
    }
  }

  private static LockRequest buildTxnLockRequest(
      String db,
      String table,
      String user,
      String host,
      String agentInfo,
      long txnId
  ) {
    return buildLockRequest(
        db,
        table,
        null,
        user,
        host,
        agentInfo,
        txnId,
        LockType.SHARED_WRITE,
        LockLevel.TABLE,
        DataOperationType.INSERT,
        true);
  }

  private static LockRequest buildLockRequest(
      String db,
      String table,
      String partition,
      String user,
      String host,
      String agentInfo,
      long txnId,
      LockType lockType,
      LockLevel lockLevel,
      DataOperationType operationType,
      boolean transactional
  ) {
    LockComponent component = new LockComponent(lockType, lockLevel, db);
    if (table != null && !table.isBlank()) {
      component.setTablename(table);
    }
    if (partition != null && !partition.isBlank()) {
      component.setPartitionname(partition);
    }
    component.setOperationType(operationType);
    component.setIsTransactional(transactional);

    LockRequest request = new LockRequest(List.of(component), user, host);
    request.setTxnid(txnId);
    request.setAgentInfo(agentInfo);
    return request;
  }

  private static <E extends Enum<E>> E parseEnumOption(String optionName, String value, Class<E> enumClass) {
    try {
      return Enum.valueOf(enumClass, value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      String allowed = Arrays.stream(enumClass.getEnumConstants())
          .map(Enum::name)
          .collect(Collectors.joining(", "));
      throw new IllegalArgumentException(
          "Invalid value for --" + optionName + ": " + value + ". Expected one of: " + allowed, e);
    }
  }

  private static void runNotificationSmoke(CliArgs cli) throws Exception {
    String uri = cli.required("uri");
    String db = cli.required("db");
    String table = cli.required("table");
    long txnId = Long.parseLong(cli.required("txn-id"));
    long writeId = Long.parseLong(cli.required("write-id"));
    String jar = cli.required("hdp-standalone-metastore-jar");
    List<String> filesAdded = cli.requiredList("files-added");
    List<String> partitionValues = cli.list("partition");

    applyOptionalKrbs(cli);

    Path jarPath = Paths.get(jar);
    URLClassLoader classLoader = new MetastoreApiClassLoader(
        MetastoreApiClassLoader.buildIsolatedRuntimeUrls(jarPath),
        HmsMetastoreSmokeCli.class.getClassLoader());

    Class<?> hiveConfClass = Class.forName("org.apache.hadoop.hive.conf.HiveConf", true, classLoader);
    Object conf = withContextClassLoader(classLoader, () -> hiveConfClass.getConstructor().newInstance());
    Method setClassLoader = Configuration.class.getMethod("setClassLoader", ClassLoader.class);
    setClassLoader.invoke(conf, classLoader);
    applyBaseConf(conf, uri, cli);

    Object client = openIsolatedClient(cli, classLoader, hiveConfClass, conf);
    try {
      Field clientField = client.getClass().getDeclaredField("client");
      clientField.setAccessible(true);
      Object thriftClient = clientField.get(client);

      Class<?> insertEventRequestDataClass =
          Class.forName("org.apache.hadoop.hive.metastore.api.InsertEventRequestData", true, classLoader);
      Object fileInfo = insertEventRequestDataClass.getConstructor(List.class).newInstance(filesAdded);

      Class<?> requestClass =
          Class.forName("org.apache.hadoop.hive.metastore.api.WriteNotificationLogRequest", true, classLoader);
      Object request = requestClass
          .getConstructor(long.class, long.class, String.class, String.class, insertEventRequestDataClass)
          .newInstance(txnId, writeId, db, table, fileInfo);

      if (!partitionValues.isEmpty()) {
        Method addToPartitionVals = requestClass.getMethod("addToPartitionVals", String.class);
        for (String value : partitionValues) {
          addToPartitionVals.invoke(request, value);
        }
      }

      Method method = thriftClient.getClass().getMethod("add_write_notification_log", requestClass);
      Object response = withContextClassLoader(classLoader, () -> method.invoke(thriftClient, request));
      System.out.println("add_write_notification_log response=" + response);
    } finally {
      withContextClassLoader(classLoader, () -> {
        client.getClass().getMethod("close").invoke(client);
        return null;
      });
      classLoader.close();
    }
  }

  private static HiveMetaStoreClient openApacheClient(CliArgs cli, HiveConf conf) throws Exception {
    if (!AUTH_KERBEROS.equals(cli.getOrDefault("auth", AUTH_SIMPLE))) {
      return new HiveMetaStoreClient(conf);
    }

    configureKerberosAuthentication();
    String principal = resolvePrincipal(cli.required("client-principal"));
    String keytab = cli.required("keytab");
    UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
    return ugi.doAs((PrivilegedExceptionAction<HiveMetaStoreClient>) () -> new HiveMetaStoreClient(conf));
  }

  private static ThriftHiveMetastore.Iface extractThriftClient(HiveMetaStoreClient client) {
    try {
      Field field = HiveMetaStoreClient.class.getDeclaredField("client");
      field.setAccessible(true);
      return (ThriftHiveMetastore.Iface) field.get(client);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Unable to access HiveMetaStoreClient.client thrift field", e);
    }
  }

  private static Object openIsolatedClient(
      CliArgs cli,
      ClassLoader classLoader,
      Class<?> hiveConfClass,
      Object conf
  ) throws Exception {
    Class<?> clientClass = Class.forName("org.apache.hadoop.hive.metastore.HiveMetaStoreClient", true, classLoader);
    if (!AUTH_KERBEROS.equals(cli.getOrDefault("auth", AUTH_SIMPLE))) {
      return withContextClassLoader(classLoader, () -> clientClass.getConstructor(hiveConfClass).newInstance(conf));
    }

    Method set = hiveConfClass.getMethod("set", String.class, String.class);
    set.invoke(conf, "hadoop.security.authentication", "kerberos");

    Class<?> childUgiClass = Class.forName("org.apache.hadoop.security.UserGroupInformation", true, classLoader);
    Method setConfiguration = childUgiClass.getMethod("setConfiguration", Configuration.class);
    setConfiguration.invoke(null, conf);
    Method loginUserFromKeytabAndReturnUGI =
        childUgiClass.getMethod("loginUserFromKeytabAndReturnUGI", String.class, String.class);
    Object childUgi = loginUserFromKeytabAndReturnUGI.invoke(
        null,
        resolvePrincipal(cli.required("client-principal")),
        cli.required("keytab"));
    Method doAs = childUgiClass.getMethod("doAs", PrivilegedExceptionAction.class);
    return doAs.invoke(childUgi, (PrivilegedExceptionAction<Object>) () ->
        withContextClassLoader(classLoader, () -> clientClass.getConstructor(hiveConfClass).newInstance(conf)));
  }

  private static HiveConf baseConf(CliArgs cli, String uri) {
    HiveConf conf = new HiveConf();
    conf.set("hive.metastore.uris", uri);
    applyBaseConf(conf, uri, cli);
    return conf;
  }

  private static void applyBaseConf(Object conf, String uri, CliArgs cli) {
    setConf(conf, "hive.metastore.uris", uri);
    setConf(conf, "hive.metastore.client.capability.check", "false");

    String auth = cli.getOrDefault("auth", AUTH_SIMPLE);
    if (AUTH_KERBEROS.equals(auth)) {
      setConf(conf, "hive.metastore.sasl.enabled", "true");
      setConf(conf, "hadoop.security.authentication", "kerberos");
      setConf(conf, "hive.metastore.kerberos.principal", cli.required("server-principal"));
    } else {
      setConf(conf, "hive.metastore.sasl.enabled", "false");
      setConf(conf, "hadoop.security.authentication", "simple");
    }

    for (Map.Entry<String, String> entry : cli.conf().entrySet()) {
      setConf(conf, entry.getKey(), entry.getValue());
    }
  }

  private static void setConf(Object conf, String key, String value) {
    try {
      Method set = conf.getClass().getMethod("set", String.class, String.class);
      set.invoke(conf, key, value);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Unable to set HiveConf key " + key, e);
    }
  }

  private static synchronized void configureKerberosAuthentication() {
    Configuration securityConf = new Configuration(false);
    securityConf.set("hadoop.security.authentication", "kerberos");
    UserGroupInformation.setConfiguration(securityConf);
  }

  private static String resolvePrincipal(String principal) {
    return KerberosPrincipalUtil.resolveForLocalHost(principal);
  }

  private static void applyOptionalKrbs(CliArgs cli) {
    String krb5Conf = cli.get("krb5-conf");
    if (krb5Conf != null && !krb5Conf.isBlank()) {
      System.setProperty("java.security.krb5.conf", krb5Conf);
    }
  }

  private static <T> T withContextClassLoader(ClassLoader classLoader, ThrowingSupplier<T> supplier)
      throws Exception {
    Thread thread = Thread.currentThread();
    ClassLoader previous = thread.getContextClassLoader();
    thread.setContextClassLoader(classLoader);
    try {
      return supplier.get();
    } finally {
      thread.setContextClassLoader(previous);
    }
  }

  private static void printUsage() {
    System.out.println("""
        Usage:
          java -cp <classpath> io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli txn [options]
          java -cp <classpath> io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli lock [options]
          java -cp <classpath> io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli notification [options]

        Common options:
          --uri thrift://host:9083
          --auth simple|kerberos                default: simple
          --server-principal hive/_HOST@REALM   required for --auth kerberos
          --client-principal user@REALM         required for --auth kerberos
          --keytab /path/client.keytab          required for --auth kerberos
          --krb5-conf /etc/krb5.conf            optional
          --conf key=value                      repeatable extra HiveConf override

        txn mode:
          --db hdp__default
          --table smoke_txn_tbl
          --user smoke-user                     optional
          --host localhost                      optional
          --agent-info hms-proxy-smoke-cli      optional
          --lock true|false                     default: true
          --valid-txn-list ...                  optional

        lock mode:
          --db dev3__default
          --lock-type SHARED_READ|SHARED_WRITE|EXCLUSIVE
          --lock-level DB|TABLE|PARTITION
          --operation-type SELECT|INSERT|UPDATE|DELETE|UNSET|NO_TXN
          --transactional true|false            default: false
          --table smoke_managed_tbl             required for TABLE/PARTITION
          --partition p=2026-04-01              required for PARTITION
          --user smoke-user                     optional
          --host localhost                      optional
          --agent-info hms-proxy-smoke-cli      optional
          --heartbeat true|false                default: true
          --unlock true|false                   default: true
          --close-txn abort|commit|none         default: abort

        notification mode:
          --db hdp__default
          --table smoke_txn_tbl
          --txn-id 1001
          --write-id 2001
          --files-added hdfs:///path/file.orc   comma-separated or repeated
          --partition p=2026-04-01              repeatable optional partition value
          --hdp-standalone-metastore-jar /opt/hive-standalone-metastore-3.1.0.3.1.0.0-78.jar
        """);
  }

  @FunctionalInterface
  private interface ThrowingSupplier<T> {
    T get() throws Exception;
  }

  private static final class CliArgs {
    private final Map<String, List<String>> values;

    private CliArgs(Map<String, List<String>> values) {
      this.values = values;
    }

    static CliArgs parse(String[] args) {
      Map<String, List<String>> values = new LinkedHashMap<>();
      for (int i = 0; i < args.length; i++) {
        String arg = args[i];
        if (!arg.startsWith("--")) {
          throw new IllegalArgumentException("Expected --key value, got: " + arg);
        }
        String key = arg.substring(2);
        if (i + 1 >= args.length || args[i + 1].startsWith("--")) {
          throw new IllegalArgumentException("Missing value for --" + key);
        }
        values.computeIfAbsent(key, ignored -> new ArrayList<>()).add(args[++i]);
      }
      return new CliArgs(values);
    }

    String get(String key) {
      List<String> list = values.get(key);
      return list == null || list.isEmpty() ? null : list.get(list.size() - 1);
    }

    String getOrDefault(String key, String defaultValue) {
      String value = get(key);
      return value == null ? defaultValue : value;
    }

    String required(String key) {
      String value = get(key);
      if (value == null || value.isBlank()) {
        throw new IllegalArgumentException("Missing required option --" + key);
      }
      return value;
    }

    boolean getBoolean(String key, boolean defaultValue) {
      String value = get(key);
      return value == null ? defaultValue : Boolean.parseBoolean(value);
    }

    List<String> list(String key) {
      List<String> raw = values.get(key);
      if (raw == null) {
        return List.of();
      }
      return raw.stream()
          .flatMap(value -> Arrays.stream(value.split(",")))
          .map(String::trim)
          .filter(value -> !value.isBlank())
          .collect(Collectors.toList());
    }

    List<String> requiredList(String key) {
      List<String> result = list(key);
      if (result.isEmpty()) {
        throw new IllegalArgumentException("Missing required option --" + key);
      }
      return result;
    }

    Map<String, String> conf() {
      Map<String, String> conf = new LinkedHashMap<>();
      for (String entry : list("conf")) {
        int separator = entry.indexOf('=');
        if (separator <= 0 || separator == entry.length() - 1) {
          throw new IllegalArgumentException("Expected --conf key=value, got: " + entry);
        }
        conf.put(entry.substring(0, separator), entry.substring(separator + 1));
      }
      return conf;
    }
  }
}
