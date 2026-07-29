#!/bin/bash
# Configure and start the official apache/hive HiveServer2 against the proxy's Hive 4 front
# door. The image's conf-symlink mechanism (HIVE_CUSTOM_CONF_DIR) silently does nothing because
# the image carries no `find`, so this writes /opt/hive/conf directly - reproducing the image's
# own defaults (Tez local mode, local scratch) and layering the stand's settings on top.
set -euo pipefail

CONF_DIR=/opt/hive/conf
METASTORE_URI=${METASTORE_URI:-thrift://proxy:9085}

prop() {
  printf '  <property><name>%s</name><value>%s</value></property>\n' "$1" "$2"
}

HIVE_SITE_PROPS=""
# The image's own defaults, kept verbatim: Tez local mode is what lets queries run without YARN.
HIVE_SITE_PROPS+=$(prop hive.server2.enable.doAs false)
HIVE_SITE_PROPS+=$(prop hive.tez.exec.inplace.progress false)
HIVE_SITE_PROPS+=$(prop hive.tez.exec.print.summary true)
HIVE_SITE_PROPS+=$(prop hive.exec.scratchdir /opt/hive/scratch_dir)
HIVE_SITE_PROPS+=$(prop hive.user.install.directory /opt/hive/install_dir)
HIVE_SITE_PROPS+=$(prop tez.runtime.optimize.local.fetch true)
HIVE_SITE_PROPS+=$(prop hive.exec.submit.local.task.via.child false)
HIVE_SITE_PROPS+=$(prop mapreduce.framework.name local)
HIVE_SITE_PROPS+=$(prop tez.local.mode true)
HIVE_SITE_PROPS+=$(prop hive.metastore.event.db.notification.api.auth false)
# The stand's own settings: the remote metastore is the proxy's Hive 4-dialect front door.
HIVE_SITE_PROPS+=$(prop hive.metastore.uris "${METASTORE_URI}")
HIVE_SITE_PROPS+=$(prop hive.metastore.warehouse.dir "${WAREHOUSE_URI:-hdfs://namenode:8020/warehouse/hive4}")

CORE_SITE_PROPS=""
if [[ -n "${HDFS_DEFAULT_FS:-}" ]]; then
  CORE_SITE_PROPS+=$(prop fs.defaultFS "${HDFS_DEFAULT_FS}")
  CORE_SITE_PROPS+=$(prop dfs.client.use.datanode.hostname true)
fi

if [[ "${KERBEROS_ENABLED:-false}" == "true" ]]; then
  for _ in $(seq 1 60); do
    [[ -r "${KEYTAB:-/keytabs/hs2-hive4.keytab}" ]] && break
    sleep 1
  done

  HIVE_SITE_PROPS+=$(prop hive.server2.authentication KERBEROS)
  HIVE_SITE_PROPS+=$(prop hive.server2.authentication.kerberos.principal "${SERVICE_PRINCIPAL}")
  HIVE_SITE_PROPS+=$(prop hive.server2.authentication.kerberos.keytab "${KEYTAB:-/keytabs/hs2-hive4.keytab}")
  HIVE_SITE_PROPS+=$(prop hive.metastore.sasl.enabled true)
  HIVE_SITE_PROPS+=$(prop hive.metastore.kerberos.principal "${METASTORE_PRINCIPAL:-hive/proxy@SMOKE.LOCAL}")
  HIVE_SITE_PROPS+=$(prop hive.metastore.kerberos.keytab.file "${KEYTAB:-/keytabs/hs2-hive4.keytab}")
  # Tez local mode still starts an in-process AM and talks to it over Hadoop RPC. Once
  # hadoop.security.authentication=kerberos is on, that internal channel demands SASL and has no
  # principal of its own, so every query dies with "Client cannot authenticate via:[TOKEN,
  # KERBEROS]" and then "TezSession has already shutdown". This flag runs local mode with no RPC
  # layer at all, which is what a single-process stand wants anyway.
  HIVE_SITE_PROPS+=$(prop tez.local.mode.without.network true)

  HDFS_HOST=$(echo "${HDFS_DEFAULT_FS:-hdfs://namenode:8020}" | sed -E 's#^[a-z]+://([^:/]+).*#\1#')
  CORE_SITE_PROPS+=$(prop hadoop.security.authentication kerberos)
  CORE_SITE_PROPS+=$(prop hadoop.security.authorization true)
  # A query engine collects HDFS delegation tokens before running a job and names a renewer for
  # them; with none configured it dies with "Can't get Master Kerberos principal for use as
  # renewer". There is no ResourceManager here (Tez runs in local mode), so HiveServer2 itself
  # plays the part. Both keys must live in core-site.xml - hive-site.xml does not reach the
  # job's own Configuration. Tokens are only collected for the filesystems named below, so both
  # clusters have to be listed: the table may live on either.
  CORE_SITE_PROPS+=$(prop yarn.resourcemanager.principal "${SERVICE_PRINCIPAL}")
  CORE_SITE_PROPS+=$(prop mapreduce.job.hdfs-servers "${HDFS_SERVERS:-${HDFS_DEFAULT_FS:-file:///}}")
  CORE_SITE_PROPS+=$(prop dfs.namenode.kerberos.principal "hdfs/${HDFS_HOST}@SMOKE.LOCAL")
  CORE_SITE_PROPS+=$(prop dfs.namenode.kerberos.principal.pattern '*')
  CORE_SITE_PROPS+=$(prop dfs.datanode.kerberos.principal 'hdfs/datanode@SMOKE.LOCAL')
  CORE_SITE_PROPS+=$(prop dfs.data.transfer.protection authentication)
  CORE_SITE_PROPS+=$(prop dfs.http.policy HTTPS_ONLY)
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

# No schema init: the metastore is remote (the proxy), there is no local Derby to prepare.
export IS_RESUME=true
export SERVICE_NAME=hiveserver2
echo "[hs2-hive4] starting Hive 4.1.0 HiveServer2 against ${METASTORE_URI} (kerberos=${KERBEROS_ENABLED:-false})"
exec /entrypoint.sh
