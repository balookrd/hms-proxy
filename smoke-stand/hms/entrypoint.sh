#!/usr/bin/env bash
# Start a standalone Hive Metastore backed by an embedded Derby database.
#
# Derby is embedded on purpose: it needs no extra container, the driver is already on the
# classpath, and one database per metastore process is exactly the topology the stand wants.
set -euo pipefail

WAREHOUSE=${WAREHOUSE_DIR:-/opt/hms/warehouse}
DERBY_DIR=${DERBY_DIR:-/opt/hms/metastore_db}
PORT=${METASTORE_PORT:-9083}
# acid-lib goes last on purpose: it is only here so TransactionalValidationListener can resolve
# OrcOutputFormat and accept a transactional table, and hive-exec carries its own metastore classes
# that must never shadow the jar under test.
# override-lib goes first for the opposite reason: it carries vendor jars that must beat the
# Maven-resolved set. It is normally empty, and the metastore jar under test still precedes it.
CP="/opt/hms:/opt/hms/metastore.jar:/opt/hms/override-lib/*:/opt/hms/lib/*:/opt/hms/acid-lib/*"

mkdir -p "$WAREHOUSE" "$(dirname "$DERBY_DIR")" /opt/hms/conf

# Hadoop reads these from core-site.xml on the classpath, never from -D properties, so both
# the HDFS client settings and the Kerberos settings have to land in this one file.
CORE_SITE_PROPS=""
if [[ -n "${HDFS_DEFAULT_FS:-}" ]]; then
  CORE_SITE_PROPS+="  <property><name>fs.defaultFS</name><value>${HDFS_DEFAULT_FS}</value></property>
  <property><name>dfs.client.use.datanode.hostname</name><value>true</value></property>
"
fi

CONF=(
  -Djavax.jdo.option.ConnectionURL="jdbc:derby:;databaseName=${DERBY_DIR};create=true"
  -Djavax.jdo.option.ConnectionDriverName=org.apache.derby.jdbc.EmbeddedDriver
  -Djavax.jdo.option.ConnectionUserName=APP
  -Djavax.jdo.option.ConnectionPassword=mine
  -Dmetastore.warehouse.dir="${WAREHOUSE_URI:-file://${WAREHOUSE}}"
  -Dhive.metastore.warehouse.dir="${WAREHOUSE_URI:-file://${WAREHOUSE}}"
  -Dmetastore.schema.verification=false
  -Dhive.metastore.schema.verification=false
  -Ddatanucleus.schema.autoCreateAll=true
  -Ddatanucleus.autoStartMechanismMode=ignored
  -Dmetastore.thrift.port="${PORT}"
  # The real proxy lives in hive-exec, which is on the classpath for the ACID format check anyway.
  # Its stand-in, DefaultPartitionExpressionProxy, throws UnsupportedOperationException the moment a
  # client filters partitions by expression - a plain "WHERE p='...'" over a partitioned table comes
  # back as MetaException(java.lang.UnsupportedOperationException).
  -Dmetastore.expression.proxy=org.apache.hadoop.hive.ql.optimizer.ppr.PartitionExpressionForMetastore
  -Dhive.metastore.expression.proxy=org.apache.hadoop.hive.ql.optimizer.ppr.PartitionExpressionForMetastore
  # Same reason: the default housekeeping list names DumpDirCleanerTask, a replication task that
  # only exists in a full Hive distribution. Keep the tasks the standalone jar actually carries.
  -Dmetastore.task.threads.always=org.apache.hadoop.hive.metastore.events.EventCleanerTask,org.apache.hadoop.hive.metastore.RuntimeStatsCleanerTask
  -Dmetastore.task.threads.remote=
  # The proxy opens backend sessions with set_ugi; without this the metastore drops the call.
  -Dmetastore.execute.setugi=true
  -Dhive.metastore.execute.setugi=true
  # HiveServer2 polls the notification log on startup; the default guards that API to
  # superusers only, which a stand has no reason to model.
  -Dmetastore.event.db.notification.api.auth=false
  -Dhive.metastore.event.db.notification.api.auth=false
  # ACID needs a real transaction manager and a compactor-aware metastore.
  -Dhive.support.concurrency=true
  -Dhive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager
  -Dmetastore.compactor.initiator.on=false
  -Dmetastore.compactor.worker.threads=0
)

if [[ "${KERBEROS_ENABLED:-false}" == "true" ]]; then
  # Wait for the KDC to publish this service's keytab before advertising SASL.
  for _ in $(seq 1 60); do
    [[ -r "${KEYTAB:-/keytabs/hms.keytab}" ]] && break
    sleep 1
  done

  # Without these UGI stays in simple mode and the server tries to use the OS user ("root")
  # as a service principal.
  # Each catalog sits on its own HDFS cluster, so the namenode principal follows the filesystem this
  # metastore was pointed at rather than being hardcoded to the first cluster. The pattern is what
  # lets a client accept a namenode whose principal it did not configure ahead of time; without it
  # the write fails with "Server has invalid Kerberos principal: hdfs/namenode-b@SMOKE.LOCAL".
  HDFS_HOST=$(echo "${HDFS_DEFAULT_FS:-hdfs://namenode:8020}" | sed -E 's#^[a-z]+://([^:/]+).*#\1#')
  CORE_SITE_PROPS+="<property><name>dfs.namenode.kerberos.principal</name><value>hdfs/${HDFS_HOST}@SMOKE.LOCAL</value></property>
  <property><name>dfs.namenode.kerberos.principal.pattern</name><value>*</value></property>
  <property><name>dfs.datanode.kerberos.principal</name><value>hdfs/datanode@SMOKE.LOCAL</value></property>
  <property><name>dfs.data.transfer.protection</name><value>authentication</value></property>
  <property><name>dfs.http.policy</name><value>HTTPS_ONLY</value></property>
  <property><name>hadoop.security.authentication</name><value>kerberos</value></property>
  <property><name>hadoop.security.authorization</name><value>true</value></property>
  <property><name>hadoop.proxyuser.hive.hosts</name><value>*</value></property>
  <property><name>hadoop.proxyuser.hive.groups</name><value>*</value></property>
  <property><name>hadoop.security.auth_to_local</name><value>RULE:[2:\$1@\$0](.*@SMOKE.LOCAL)s/@.*//
RULE:[1:\$1@\$0](.*@SMOKE.LOCAL)s/@.*//
DEFAULT</value></property>
"
  CONF+=(
    -Dmetastore.authentication=KERBEROS
    -Dhive.metastore.sasl.enabled=true
    -Dmetastore.sasl.enabled=true
    -Dhive.metastore.kerberos.principal="${SERVICE_PRINCIPAL}"
    -Dmetastore.kerberos.principal="${SERVICE_PRINCIPAL}"
    -Dhive.metastore.kerberos.keytab.file="${KEYTAB:-/keytabs/hms.keytab}"
    -Dmetastore.kerberos.keytab.file="${KEYTAB:-/keytabs/hms.keytab}"
    -Dhadoop.security.authentication=kerberos
    -Dhadoop.security.authorization=true
    # The proxy connects as its own principal and impersonates the caller.
    -Dhadoop.proxyuser.hive.hosts=*
    -Dhadoop.proxyuser.hive.groups=*
    -Dhadoop.proxyuser.proxy.hosts=*
    -Dhadoop.proxyuser.proxy.groups=*
  )
fi

if [[ -n "${CORE_SITE_PROPS}" ]]; then
  printf '<?xml version="1.0"?>\n<configuration>\n%s</configuration>\n' "${CORE_SITE_PROPS}" \
    > /opt/hms/conf/core-site.xml
  CP="/opt/hms/conf:${CP}"
  echo "[hms] wrote core-site.xml"
fi

echo "[hms] initializing transaction tables in ${DERBY_DIR}"
java -cp "$CP" "${CONF[@]}" InitSchema \
  "javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=${DERBY_DIR};create=true" \
  "javax.jdo.option.ConnectionDriverName=org.apache.derby.jdbc.EmbeddedDriver" \
  "javax.jdo.option.ConnectionUserName=APP" \
  "javax.jdo.option.ConnectionPassword=mine" \
  || echo "[hms] InitSchema reported a problem, continuing to metastore start"

echo "[hms] starting metastore on port ${PORT} (kerberos=${KERBEROS_ENABLED:-false})"
exec java -cp "$CP" "${CONF[@]}" \
  org.apache.hadoop.hive.metastore.HiveMetaStore -p "${PORT}"
