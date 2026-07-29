#!/bin/bash
# Configure and start the official apache/hive metastore. Mirrors the shape of ../hms/entrypoint.sh:
# everything is generated from env into conf files, because Hadoop reads core-site.xml from the
# classpath and never from -D properties. The official /entrypoint.sh then runs schematool
# (creating the full Derby schema, transaction tables included) and starts the metastore.
set -euo pipefail

DERBY_DIR=${DERBY_DIR:-/opt/hms/db/metastore_db}
# Written straight into the image's conf dir: the official entrypoint's HIVE_CUSTOM_CONF_DIR
# mechanism silently does nothing here, because the image carries no `find` binary and the
# symlinking loop dies with "command not found" under its own `set -x` only.
CONF_DIR=/opt/hive/conf
mkdir -p "$(dirname "${DERBY_DIR}")"

prop() {
  printf '  <property><name>%s</name><value>%s</value></property>\n' "$1" "$2"
}

HIVE_SITE_PROPS=""
HIVE_SITE_PROPS+=$(prop javax.jdo.option.ConnectionURL "jdbc:derby:;databaseName=${DERBY_DIR};create=true")
HIVE_SITE_PROPS+=$(prop javax.jdo.option.ConnectionDriverName org.apache.derby.jdbc.EmbeddedDriver)
HIVE_SITE_PROPS+=$(prop metastore.warehouse.dir "${WAREHOUSE_URI:-file:///opt/hms/warehouse}")
HIVE_SITE_PROPS+=$(prop hive.metastore.warehouse.dir "${WAREHOUSE_URI:-file:///opt/hms/warehouse}")
HIVE_SITE_PROPS+=$(prop metastore.thrift.port "${METASTORE_PORT:-9083}")
# The proxy opens backend sessions with set_ugi; without this the metastore drops the call.
HIVE_SITE_PROPS+=$(prop metastore.execute.setugi true)
HIVE_SITE_PROPS+=$(prop hive.metastore.execute.setugi true)
# HiveServer2 instances poll the notification log; the default guards that API to superusers.
HIVE_SITE_PROPS+=$(prop metastore.event.db.notification.api.auth false)
HIVE_SITE_PROPS+=$(prop hive.metastore.event.db.notification.api.auth false)
# The stand-in DefaultPartitionExpressionProxy throws on any partition filter-by-expression;
# the real class is available because the official image carries the full Hive distribution.
HIVE_SITE_PROPS+=$(prop metastore.expression.proxy org.apache.hadoop.hive.ql.optimizer.ppr.PartitionExpressionForMetastore)
# No compactions on the stand: single writer, and no YARN to run them on.
HIVE_SITE_PROPS+=$(prop metastore.compactor.initiator.on false)
HIVE_SITE_PROPS+=$(prop metastore.compactor.worker.threads 0)

CORE_SITE_PROPS=""
if [[ -n "${HDFS_DEFAULT_FS:-}" ]]; then
  CORE_SITE_PROPS+=$(prop fs.defaultFS "${HDFS_DEFAULT_FS}")
  CORE_SITE_PROPS+=$(prop dfs.client.use.datanode.hostname true)
fi

if [[ "${KERBEROS_ENABLED:-false}" == "true" ]]; then
  # Wait for the KDC to publish this service's keytab before advertising SASL.
  for _ in $(seq 1 60); do
    [[ -r "${KEYTAB:-/keytabs/hms-hive4.keytab}" ]] && break
    sleep 1
  done

  HIVE_SITE_PROPS+=$(prop metastore.sasl.enabled true)
  HIVE_SITE_PROPS+=$(prop hive.metastore.sasl.enabled true)
  HIVE_SITE_PROPS+=$(prop metastore.kerberos.principal "${SERVICE_PRINCIPAL}")
  HIVE_SITE_PROPS+=$(prop hive.metastore.kerberos.principal "${SERVICE_PRINCIPAL}")
  HIVE_SITE_PROPS+=$(prop metastore.kerberos.keytab.file "${KEYTAB:-/keytabs/hms-hive4.keytab}")
  HIVE_SITE_PROPS+=$(prop hive.metastore.kerberos.keytab.file "${KEYTAB:-/keytabs/hms-hive4.keytab}")

  HDFS_HOST=$(echo "${HDFS_DEFAULT_FS:-hdfs://namenode:8020}" | sed -E 's#^[a-z]+://([^:/]+).*#\1#')
  CORE_SITE_PROPS+=$(prop hadoop.security.authentication kerberos)
  CORE_SITE_PROPS+=$(prop hadoop.security.authorization true)
  CORE_SITE_PROPS+=$(prop dfs.namenode.kerberos.principal "hdfs/${HDFS_HOST}@SMOKE.LOCAL")
  CORE_SITE_PROPS+=$(prop dfs.namenode.kerberos.principal.pattern '*')
  CORE_SITE_PROPS+=$(prop dfs.datanode.kerberos.principal 'hdfs/datanode@SMOKE.LOCAL')
  CORE_SITE_PROPS+=$(prop dfs.data.transfer.protection authentication)
  CORE_SITE_PROPS+=$(prop dfs.http.policy HTTPS_ONLY)
  CORE_SITE_PROPS+=$(prop hadoop.proxyuser.hive.hosts '*')
  CORE_SITE_PROPS+=$(prop hadoop.proxyuser.hive.groups '*')
  CORE_SITE_PROPS+="  <property><name>hadoop.security.auth_to_local</name><value>RULE:[2:\$1@\$0](.*@SMOKE.LOCAL)s/@.*//
RULE:[1:\$1@\$0](.*@SMOKE.LOCAL)s/@.*//
DEFAULT</value></property>
"
fi

printf '<?xml version="1.0"?>\n<configuration>\n%s\n</configuration>\n' "${HIVE_SITE_PROPS}" \
  > "${CONF_DIR}/hive-site.xml"
if [[ -n "${CORE_SITE_PROPS}" ]]; then
  printf '<?xml version="1.0"?>\n<configuration>\n%s\n</configuration>\n' "${CORE_SITE_PROPS}" \
    > "${CONF_DIR}/core-site.xml"
fi

export SERVICE_NAME=metastore
echo "[hms-hive4] starting Hive 4.1.0 metastore (kerberos=${KERBEROS_ENABLED:-false})"
exec /entrypoint.sh
