package io.github.mmalykhin.hmsproxy.tools;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.junit.Assert;
import org.junit.Test;

public class HmsMetastoreSmokeCliTest {
  @Test
  public void cliArgsParsesRepeatedAndCommaSeparatedValues() throws Exception {
    Object cli = parse("--files-added", "a,b", "--files-added", "c", "--partition", "p=1");

    Assert.assertEquals(List.of("a", "b", "c"), invokeList(cli, "requiredList", "files-added"));
    Assert.assertEquals(List.of("p=1"), invokeList(cli, "list", "partition"));
  }

  @Test
  public void cliArgsParsesExtraConfPairs() throws Exception {
    Object cli = parse("--conf", "a=b", "--conf", "c=d=e");

    @SuppressWarnings("unchecked")
    Method confMethod = cli.getClass().getDeclaredMethod("conf");
    confMethod.setAccessible(true);
    Map<String, String> conf = (Map<String, String>) confMethod.invoke(cli);
    Assert.assertEquals("b", conf.get("a"));
    Assert.assertEquals("d=e", conf.get("c"));
  }

  @Test
  public void buildLockRequestBuildsNoTxnDbLock() throws Exception {
    LockRequest request = buildLockRequest(
        "dev3__default",
        null,
        null,
        "smoke-user",
        "localhost",
        "agent",
        411672L,
        LockType.SHARED_READ,
        LockLevel.DB,
        DataOperationType.NO_TXN,
        false);

    Assert.assertEquals(411672L, request.getTxnid());
    Assert.assertEquals("smoke-user", request.getUser());
    Assert.assertEquals("localhost", request.getHostname());
    Assert.assertEquals("agent", request.getAgentInfo());
    Assert.assertEquals(1, request.getComponentSize());
    Assert.assertEquals(LockType.SHARED_READ, request.getComponent().get(0).getType());
    Assert.assertEquals(LockLevel.DB, request.getComponent().get(0).getLevel());
    Assert.assertEquals("dev3__default", request.getComponent().get(0).getDbname());
    Assert.assertFalse(request.getComponent().get(0).isSetTablename());
    Assert.assertFalse(request.getComponent().get(0).isSetPartitionname());
    Assert.assertEquals(DataOperationType.NO_TXN, request.getComponent().get(0).getOperationType());
    Assert.assertFalse(request.getComponent().get(0).isIsTransactional());
  }

  @Test
  public void buildLockRequestBuildsPartitionNoTxnLock() throws Exception {
    LockRequest request = buildLockRequest(
        "dev3__default",
        "smoke_managed_tbl",
        "p=2026-03-31",
        "smoke-user",
        "localhost",
        "agent",
        411677L,
        LockType.EXCLUSIVE,
        LockLevel.PARTITION,
        DataOperationType.NO_TXN,
        false);

    Assert.assertEquals(1, request.getComponentSize());
    Assert.assertEquals(LockType.EXCLUSIVE, request.getComponent().get(0).getType());
    Assert.assertEquals(LockLevel.PARTITION, request.getComponent().get(0).getLevel());
    Assert.assertEquals("smoke_managed_tbl", request.getComponent().get(0).getTablename());
    Assert.assertEquals("p=2026-03-31", request.getComponent().get(0).getPartitionname());
    Assert.assertEquals(DataOperationType.NO_TXN, request.getComponent().get(0).getOperationType());
    Assert.assertFalse(request.getComponent().get(0).isIsTransactional());
  }

  private static Object parse(String... args) throws Exception {
    Class<?> cliClass = Class.forName("io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli$CliArgs");
    Method parse = cliClass.getDeclaredMethod("parse", String[].class);
    parse.setAccessible(true);
    return parse.invoke(null, (Object) args);
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
  ) throws Exception {
    Method method = HmsMetastoreSmokeCli.class.getDeclaredMethod(
        "buildLockRequest",
        String.class,
        String.class,
        String.class,
        String.class,
        String.class,
        String.class,
        long.class,
        LockType.class,
        LockLevel.class,
        DataOperationType.class,
        boolean.class);
    method.setAccessible(true);
    return (LockRequest) method.invoke(
        null, db, table, partition, user, host, agentInfo, txnId, lockType, lockLevel, operationType, transactional);
  }

  @SuppressWarnings("unchecked")
  private static List<String> invokeList(Object target, String methodName, String key) throws Exception {
    Method method = target.getClass().getDeclaredMethod(methodName, String.class);
    method.setAccessible(true);
    return (List<String>) method.invoke(target, key);
  }
}
