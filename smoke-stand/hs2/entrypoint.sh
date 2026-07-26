#!/usr/bin/env bash
# HiveServer2 pointed at the proxy, not at a metastore: that is the whole point of this layer.
# Every DDL and read the SQL smoke issues then travels the real client path
# (HiveMetaStoreClient -> proxy -> backend), including create_table_with_environment_context.
set -euo pipefail

CONF_DIR=/opt/hs2/conf
WAREHOUSE=${WAREHOUSE_DIR:-/opt/hs2/warehouse}
SCRATCH=${SCRATCH_DIR:-/opt/hs2/scratch}
METASTORE_URI=${METASTORE_URI:-thrift://proxy:9083}

mkdir -p "$CONF_DIR" "$WAREHOUSE" "$SCRATCH" /opt/hs2/logs
# HiveServer2 refuses to start unless the scratch dir is group/other writable.
chmod 1777 "$SCRATCH" "$WAREHOUSE"

KERBEROS_PROPS=""
if [[ "${KERBEROS_ENABLED:-false}" == "true" ]]; then
  for _ in $(seq 1 90); do
    [[ -r "${KEYTAB:-/keytabs/hs2.keytab}" ]] && break
    sleep 1
  done
  cat > "$CONF_DIR/core-site.xml" <<XML
<?xml version="1.0"?>
<configuration>
  <property><name>hadoop.security.authentication</name><value>kerberos</value></property>
  <property><name>hadoop.security.authorization</name><value>true</value></property>
  <!-- HDFS in this stand runs unsecured. A Kerberos client refuses to talk to it unless the
       fallback is explicit; securing HDFS itself (namenode/datanode keytabs, SASL data transfer)
       is out of scope here. -->
  <property><name>ipc.client.fallback-to-simple-auth-allowed</name><value>true</value></property>
</configuration>
XML
  KERBEROS_PROPS=$(cat <<XML
  <property><name>hive.metastore.sasl.enabled</name><value>true</value></property>
  <property><name>hive.metastore.kerberos.principal</name><value>${METASTORE_PRINCIPAL}</value></property>
  <property><name>hive.server2.authentication</name><value>KERBEROS</value></property>
  <property><name>hive.server2.authentication.kerberos.principal</name><value>${SERVICE_PRINCIPAL}</value></property>
  <property><name>hive.server2.authentication.kerberos.keytab</name><value>${KEYTAB}</value></property>
  <!-- Secure Hadoop asks for the ResourceManager principal as the delegation-token renewer even
       when jobs run as local MapReduce and there is no YARN. Point it at this service so token
       collection resolves instead of failing with "Can't get Master Kerberos principal". -->
  <property><name>yarn.resourcemanager.principal</name><value>${SERVICE_PRINCIPAL}</value></property>
  <property><name>mapreduce.jobhistory.principal</name><value>${SERVICE_PRINCIPAL}</value></property>
XML
)
fi

cat > "$CONF_DIR/hive-site.xml" <<XML
<?xml version="1.0"?>
<configuration>
  <!-- The proxy stands in for the metastore. -->
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
  <property><name>hive.querylog.location</name><value>/opt/hs2/logs</value></property>
  <property><name>fs.defaultFS</name><value>${HDFS_DEFAULT_FS:-file:///}</value></property>
  <property><name>dfs.client.use.datanode.hostname</name><value>true</value></property>

  <!-- No YARN in the stand: run queries as local MapReduce. -->
  <property><name>hive.execution.engine</name><value>mr</value></property>
  <property><name>mapreduce.framework.name</name><value>local</value></property>
  <property><name>hive.exec.mode.local.auto</name><value>true</value></property>
  <property><name>hive.exec.submit.local.task.via.child</name><value>false</value></property>
  <property><name>hive.auto.convert.join</name><value>false</value></property>

  <!-- ACID writes go to the default catalog through the proxy. -->
  <property><name>hive.support.concurrency</name><value>true</value></property>
  <property><name>hive.txn.manager</name><value>org.apache.hadoop.hive.ql.lockmgr.DbTxnManager</value></property>
  <property><name>hive.compactor.initiator.on</name><value>false</value></property>
${KERBEROS_PROPS}
</configuration>
XML

CP="${CONF_DIR}:/opt/hs2:/opt/hs2/lib/*"

echo "[hs2] waiting for the proxy at ${METASTORE_URI}"
PROXY_HOST=$(echo "$METASTORE_URI" | sed -E 's#thrift://([^:]+):.*#\1#')
PROXY_PORT=$(echo "$METASTORE_URI" | sed -E 's#.*:([0-9]+)$#\1#')
for _ in $(seq 1 90); do
  nc -z "$PROXY_HOST" "$PROXY_PORT" 2>/dev/null && break
  sleep 2
done

echo "[hs2] starting HiveServer2 against ${METASTORE_URI}"
exec java -Xmx1500m -cp "$CP" \
  -Dlog4j.configurationFile=file:///opt/hs2/log4j2.properties \
  -Dhive.metastore.uris="${METASTORE_URI}" \
  org.apache.hive.service.server.HiveServer2
