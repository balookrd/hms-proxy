#!/usr/bin/env bash
# Hortonworks HiveServer2 pointed at the proxy's Hortonworks front door (9084), not at the primary
# one. Thrift cannot negotiate versions, so an HDP client has to reach the listener that speaks the
# HDP interface - among other things it is the only one carrying add_write_notification_log, which
# HDP Hive calls on every ACID write.
#
# Unlike the Apache HiveServer2 next door this runs the vendor distribution through its own
# launcher (`hive --service hiveserver2`), so the classpath is assembled exactly as it would be on
# a real cluster.
set -euo pipefail

CONF_DIR=${HIVE_CONF_DIR:-/opt/hs2-hdp/conf}
WAREHOUSE=${WAREHOUSE_DIR:-/opt/hs2-hdp/warehouse}
SCRATCH=${SCRATCH_DIR:-/opt/hs2-hdp/scratch}
METASTORE_URI=${METASTORE_URI:-thrift://proxy:9084}

mkdir -p "$CONF_DIR" "$WAREHOUSE" "$SCRATCH" /opt/hs2-hdp/logs
# HiveServer2 refuses to start unless the scratch dir is group/other writable.
chmod 1777 "$SCRATCH" "$WAREHOUSE"

KERBEROS_PROPS=""
if [[ "${KERBEROS_ENABLED:-false}" == "true" ]]; then
  for _ in $(seq 1 90); do
    [[ -r "${KEYTAB:-/keytabs/hs2-hdp.keytab}" ]] && break
    sleep 1
  done
  # Hadoop reads hadoop.security.authentication from core-site.xml on the classpath, never from -D.
  cat > "$CONF_DIR/core-site.xml" <<XML
<?xml version="1.0"?>
<configuration>
  <property><name>hadoop.security.authentication</name><value>kerberos</value></property>
  <property><name>hadoop.security.authorization</name><value>true</value></property>
  <property><name>dfs.namenode.kerberos.principal</name><value>hdfs/namenode@SMOKE.LOCAL</value></property>
  <property><name>dfs.datanode.kerberos.principal</name><value>hdfs/datanode@SMOKE.LOCAL</value></property>
  <property><name>dfs.data.transfer.protection</name><value>authentication</value></property>
  <property><name>dfs.http.policy</name><value>HTTPS_ONLY</value></property>
  <!-- MapReduce collects HDFS delegation tokens before running a job and needs a principal to name
       as their renewer; without one it dies with "Can't get Master Kerberos principal for use as
       renewer" and LocalJobRunner reports only "return code 2". There is no ResourceManager here,
       so point it at this service. It has to live in core-site.xml: hive-site.xml does not reach
       the job's Configuration. -->
  <property><name>yarn.resourcemanager.principal</name><value>${SERVICE_PRINCIPAL}</value></property>
  <property><name>fs.defaultFS</name><value>${HDFS_DEFAULT_FS:-file:///}</value></property>
  <!-- The second cluster's namenode principal, so this client can authenticate to it as well. -->
  <property><name>dfs.namenode.kerberos.principal.pattern</name><value>*</value></property>
</configuration>
XML
  KERBEROS_PROPS=$(cat <<XML
  <property><name>hive.metastore.sasl.enabled</name><value>true</value></property>
  <property><name>hive.metastore.kerberos.principal</name><value>${METASTORE_PRINCIPAL}</value></property>
  <property><name>hive.server2.authentication</name><value>KERBEROS</value></property>
  <property><name>hive.server2.authentication.kerberos.principal</name><value>${SERVICE_PRINCIPAL}</value></property>
  <property><name>hive.server2.authentication.kerberos.keytab</name><value>${KEYTAB}</value></property>
  <property><name>yarn.resourcemanager.principal</name><value>${SERVICE_PRINCIPAL}</value></property>
  <property><name>mapreduce.jobhistory.principal</name><value>${SERVICE_PRINCIPAL}</value></property>
XML
)
else
  cat > "$CONF_DIR/core-site.xml" <<XML
<?xml version="1.0"?>
<configuration>
  <property><name>fs.defaultFS</name><value>${HDFS_DEFAULT_FS:-file:///}</value></property>
</configuration>
XML
fi

cat > "$CONF_DIR/hive-site.xml" <<XML
<?xml version="1.0"?>
<configuration>
  <!-- The proxy stands in for the metastore, on its Hortonworks listener. -->
  <property><name>hive.metastore.uris</name><value>${METASTORE_URI}</value></property>
  <!-- The proxy manages identity; a second set_ugi on the same connection is refused upstream. -->
  <property><name>hive.metastore.execute.setugi</name><value>false</value></property>
  <property><name>hive.metastore.schema.verification</name><value>false</value></property>

  <property><name>hive.server2.thrift.port</name><value>10000</value></property>
  <property><name>hive.server2.thrift.bind.host</name><value>0.0.0.0</value></property>
  <property><name>hive.server2.webui.port</name><value>10002</value></property>
  <property><name>hive.server2.enable.doAs</name><value>false</value></property>
  <property><name>hive.server2.active.passive.ha.enable</name><value>false</value></property>

  <property><name>hive.metastore.warehouse.dir</name><value>${WAREHOUSE_URI:-file://${WAREHOUSE}}</value></property>
  <property><name>hive.exec.scratchdir</name><value>${SCRATCH}</value></property>
  <property><name>hive.exec.local.scratchdir</name><value>${SCRATCH}</value></property>
  <property><name>hive.querylog.location</name><value>/opt/hs2-hdp/logs</value></property>
  <property><name>fs.defaultFS</name><value>${HDFS_DEFAULT_FS:-file:///}</value></property>
  <property><name>dfs.client.use.datanode.hostname</name><value>true</value></property>
  <!-- Both HDFS clusters must be reachable: the catalogs live on different filesystems, so a
       single query can read from both. Full URIs handle addressing, but a kerberized MapReduce job
       only collects delegation tokens for the filesystems it is told about - anything missing here
       fails the job with "Can't get Master Kerberos principal for use as renewer", surfaced as a
       bare "return code 2". -->
  <property><name>mapreduce.job.hdfs-servers</name><value>${HDFS_SERVERS:-${HDFS_DEFAULT_FS:-file:///}}</value></property>
  <property><name>dfs.namenode.kerberos.principal.pattern</name><value>*</value></property>

  <!-- The execution engine is deliberately NOT set here: the vendor default (tez) stays, and
       clients switch to local MapReduce per session with `set hive.execution.engine=mr;` - the SQL
       smoke does it in its first statement.
       Putting "mr" in this file instead would stop HiveServer2 from starting at all, because
       HiveConf.initialize() runs validateExecutionEngine and Hortonworks builds without MapReduce
       ("mr execution engine is not supported!"). The session-level set is validated too, and that
       check is the reason hive.in.test is on: without it the set is refused with "hive execution
       engine mr is not supported.". So the flag buys exactly one thing - the right to choose the
       engine at runtime - and no longer dictates how the whole server is configured. Tez is not an
       option regardless: it needs a ResourceManager and the Tez tarball in HDFS, and this
       distribution ships neither. -->
  <property><name>hive.in.test</name><value>true</value></property>
  <property><name>hive.exec.dynamic.partition.mode</name><value>nonstrict</value></property>
  <property><name>mapreduce.framework.name</name><value>local</value></property>
  <property><name>hive.exec.mode.local.auto</name><value>true</value></property>
  <property><name>hive.exec.submit.local.task.via.child</name><value>false</value></property>
  <property><name>hive.auto.convert.join</name><value>false</value></property>
  <!-- HDP ships these on by default and both need services this stand does not run. -->
  <property><name>hive.server2.tez.initialize.default.sessions</name><value>false</value></property>
  <property><name>hive.metastore.event.listeners</name><value></value></property>
  <property><name>hive.exec.pre.hooks</name><value></value></property>
  <property><name>hive.exec.post.hooks</name><value></value></property>
  <property><name>hive.exec.failure.hooks</name><value></value></property>
  <property><name>hive.stats.autogather</name><value>false</value></property>

  <!-- ACID writes go to the default catalog through the proxy. -->
  <property><name>hive.support.concurrency</name><value>true</value></property>
  <property><name>hive.txn.manager</name><value>org.apache.hadoop.hive.ql.lockmgr.DbTxnManager</value></property>
  <property><name>hive.compactor.initiator.on</name><value>false</value></property>
  <!-- Makes Hive call add_write_notification_log after an ACID write. That RPC exists only in the
       Hortonworks Thrift interface, so this is the one setup where the proxy's Hortonworks front
       door is driven by Hive itself rather than by the smoke CLI. -->
  <property><name>hive.metastore.dml.events</name><value>true</value></property>
${KERBEROS_PROPS}
</configuration>
XML

echo "[hs2-hdp] waiting for the proxy at ${METASTORE_URI}"
PROXY_HOST=$(echo "$METASTORE_URI" | sed -E 's#thrift://([^:]+):.*#\1#')
PROXY_PORT=$(echo "$METASTORE_URI" | sed -E 's#.*:([0-9]+)$#\1#')
for _ in $(seq 1 90); do
  nc -z "$PROXY_HOST" "$PROXY_PORT" 2>/dev/null && break
  sleep 2
done

echo "[hs2-hdp] starting Hortonworks HiveServer2 against ${METASTORE_URI}"
export HADOOP_CLIENT_OPTS="-Xmx1500m ${HADOOP_CLIENT_OPTS:-}"
exec "${HIVE_HOME}/bin/hive" --service hiveserver2
