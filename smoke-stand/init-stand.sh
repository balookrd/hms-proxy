#!/usr/bin/env bash
# Automatically prepares all Docker stand prerequisites (HDFS directories, safemode wait,
# and base tables for notification, transactions, and Iceberg REST smoke tests).
# Run this once after 'docker compose up -d' or let smoke runners call it automatically.
set -euo pipefail

STAND_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log() {
  printf '[init-stand] %s\n' "$*"
}

fail() {
  log "error: $*"
  exit 1
}

is_running() {
  local container="$1"
  docker ps --format '{{.Names}}' | grep -q "^${container}$"
}

kerberos_enabled() {
  [[ "$(docker exec stand-proxy printenv KERBEROS_ENABLED 2>/dev/null || true)" == "true" ]]
}

# Runs SQL through the beeline of stand-hs2. On the Kerberos profile the JDBC URL needs the
# HiveServer2 principal and the container a ticket; without them beeline fails in the SASL handshake.
hs2_beeline() {
  local sql="$1"
  local url="jdbc:hive2://localhost:10000/default"
  if kerberos_enabled; then
    url="jdbc:hive2://hs2:10000/default;principal=hive/hs2@SMOKE.LOCAL"
    docker exec stand-hs2 kinit -kt /keytabs/smoke-user.keytab smoke-user@SMOKE.LOCAL
  fi
  docker exec stand-hs2 java -cp '/opt/hs2/conf:/opt/hs2/lib/*' org.apache.hive.beeline.BeeLine \
    -u "${url}" -n hive --silent=true --showHeader=false --outputformat=tsv2 -e "${sql}" 2>&1
}

# Runs the Iceberg REST writer against default.smoke_iceberg_tbl. The Kerberos profile protects
# the REST listener with SPNEGO, so the writer needs its keytab there.
rest_writer() {
  local -a auth_args=()
  if kerberos_enabled; then
    auth_args+=(--keytab /keytabs/smoke-user.keytab --principal smoke-user@SMOKE.LOCAL --rest-auth spnego)
    # The writer mints its SPNEGO token from the ticket cache, and a freshly created proxy
    # container has none.
    docker exec stand-proxy kinit -kt /keytabs/smoke-user.keytab smoke-user@SMOKE.LOCAL
  fi
  docker exec stand-proxy java \
    --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
    --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
    -jar /opt/hms-proxy/iceberg-rest-writer.jar "$@" \
    --uri http://proxy:9183 --namespace default --table smoke_iceberg_tbl \
    ${auth_args[@]+"${auth_args[@]}"}
}

# On the Kerberos profile a namenode container holds no ticket, so every `hdfs dfs` run inside it
# is refused with "Client cannot authenticate via:[TOKEN, KERBEROS]". Log it in by its own keytab.
namenode_login() {
  local container="$1"
  local host="${container#stand-}"
  if kerberos_enabled; then
    docker exec "${container}" kinit -kt "/keytabs/${host}.keytab" "hdfs/${host}@SMOKE.LOCAL" \
      || fail "kinit for hdfs/${host} inside ${container} failed"
  fi
}

# 1. HDFS safemode and directory initialization
if is_running "stand-namenode"; then
  log "Waiting for stand-namenode safemode..."
  docker exec stand-namenode hdfs dfsadmin -safemode wait >/dev/null 2>&1 || true

  namenode_login stand-namenode
  log "Ensuring HDFS warehouse directories on stand-namenode..."
  docker exec stand-namenode bash -c \
    'hdfs dfs -mkdir -p /warehouse/apache /warehouse/hdp /warehouse/hive4 /external && hdfs dfs -chmod -R 1777 /warehouse /external' >/dev/null 2>&1 || true
fi

if is_running "stand-namenode-b"; then
  log "Waiting for stand-namenode-b safemode..."
  docker exec stand-namenode-b hdfs dfsadmin -safemode wait >/dev/null 2>&1 || true

  namenode_login stand-namenode-b
  log "Ensuring HDFS warehouse directories on stand-namenode-b..."
  docker exec stand-namenode-b bash -c \
    'hdfs dfs -mkdir -p /warehouse/apache /warehouse/hdp /warehouse/hive4 /external && hdfs dfs -chmod -R 1777 /warehouse /external' >/dev/null 2>&1 || true
fi

# 2. Base ORC transaction table for notification and lock smokes
if is_running "stand-hs2"; then
  log "Ensuring smoke_txn_tbl exists in default catalog..."
  # The notification smoke needs this table: add_write_notification_log resolves it first, and its
  # IDL declares no exceptions, so a missing table reaches the client only as "Internal error".
  # The table name is checked as a whole line, not as a substring of the echoed statement.
  output=""
  if output="$(hs2_beeline "create table if not exists smoke_txn_tbl (id int) stored as orc;
show tables in default like 'smoke_txn_tbl';")" \
      && grep -qx 'smoke_txn_tbl' <<<"${output}"; then
    log "default.smoke_txn_tbl is ready"
  else
    log "error: could not prepare default.smoke_txn_tbl via stand-hs2:"
    tail -n 20 <<<"${output}" >&2
    exit 1
  fi
fi

# 3. Iceberg table in default catalog (hdp)
if is_running "stand-proxy" && is_running "stand-namenode"; then
  log "Ensuring smoke_iceberg_tbl exists via Iceberg REST writer..."
  # "create" is not idempotent, so an existing table is detected by loading it ("count") first.
  output=""
  if rest_writer count >/dev/null 2>&1; then
    log "default.smoke_iceberg_tbl is ready"
  elif output="$(rest_writer create 2>&1)" && rest_writer count >/dev/null 2>&1; then
    log "default.smoke_iceberg_tbl is ready"
  else
    log "error: could not prepare default.smoke_iceberg_tbl via the Iceberg REST listener:"
    tail -n 20 <<<"${output}" >&2
    exit 1
  fi
fi

# 4. Iceberg table in second catalog (apache on stand-namenode-b)
if is_running "stand-namenode-b" && is_running "stand-hs2"; then
  log "Ensuring smoke_iceberg_tbl_ap metadata exists in stand-namenode-b..."
  docker exec stand-namenode-b bash -c \
    'echo '\''{"format-version":2,"table-uuid":"546edc8c-8dcd-444d-bbb1-8d1788a27ffa","location":"hdfs://namenode-b:8020/warehouse/apache/smoke_iceberg_tbl_ap","last-sequence-number":0,"last-updated-ms":1791929548977,"last-column-id":2,"current-schema-id":0,"schemas":[{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":false,"type":"int"},{"id":2,"name":"src","required":false,"type":"string"}]}],"default-spec-id":0,"partition-specs":[{"spec-id":0,"fields":[]}],"last-partition-id":999,"default-sort-order-id":0,"sort-orders":[{"order-id":0,"fields":[]}],"properties":{"engine.hive.enabled":"true","write.parquet.compression-codec":"zstd"},"current-snapshot-id":-1,"refs":{},"snapshots":[],"statistics":[],"partition-statistics":[],"snapshot-log":[],"metadata-log":[]}'\'' > /tmp/00000-smoke.metadata.json && \
     hdfs dfs -mkdir -p /warehouse/apache/smoke_iceberg_tbl_ap/metadata && \
     hdfs dfs -put -f /tmp/00000-smoke.metadata.json /warehouse/apache/smoke_iceberg_tbl_ap/metadata/' >/dev/null 2>&1 \
    || fail "could not write the smoke_iceberg_tbl_ap metadata file to HDFS on stand-namenode-b"

  log "Ensuring smoke_iceberg_tbl_ap table exists in apache catalog..."
  hs2_beeline "create external table if not exists apache__default.smoke_iceberg_tbl_ap (id int, ds string)
      stored as parquet
      location 'hdfs://namenode-b:8020/warehouse/apache/smoke_iceberg_tbl_ap'
      tblproperties (
        'table_type'='ICEBERG',
        'metadata_location'='hdfs://namenode-b:8020/warehouse/apache/smoke_iceberg_tbl_ap/metadata/00000-smoke.metadata.json');" >/dev/null 2>&1 || true
fi

# 5. Read tables of the SQL smoke (HMS_SMOKE_HDP_READ_TABLE / HMS_SMOKE_APACHE_READ_TABLE in
# env/sql*.env). Created through the proxy, each through the front door paired with its own
# metastore - hs2-hdp for the hdp catalog, hs2 for the apache one - so the step does not depend on
# which catalog is the default and never goes through the cross pairs (TEST-MATRIX rows C6/C7).
# Both HiveServer2 are reached from stand-hs2-hdp, the same place the SQL smoke runs from.
HDP_READ_TABLE="${HMS_SMOKE_HDP_READ_TABLE:-smoke_read_hdp}"
APACHE_READ_TABLE="${HMS_SMOKE_APACHE_READ_TABLE:-smoke_read_ap}"

ensure_read_table() {
  local hs2_host="$1"
  local database="$2"
  local table="$3"
  local url="jdbc:hive2://${hs2_host}:10000/default"
  if kerberos_enabled; then
    url="${url};principal=hive/${hs2_host}@SMOKE.LOCAL"
    docker exec stand-hs2-hdp kinit -kt /keytabs/smoke-user.keytab smoke-user@SMOKE.LOCAL
  fi
  # INSERT OVERWRITE keeps the table at exactly one row however often this runs. The Hortonworks
  # HiveServer2 needs the engine set per session (no MapReduce in its build config, no YARN for Tez).
  local sql="set hive.execution.engine=mr;
set hive.vectorized.execution.enabled=false;
create table if not exists ${database}.${table} (id int, src string) stored as parquet;
insert overwrite table ${database}.${table} values (1, '${table}');
select concat('read_table_ready_', src) from ${database}.${table};"
  local output=""
  # The marker is checked at the start of a line: beeline echoes the statement, so the literal
  # is in the output whatever the query returned.
  if output="$(docker exec stand-hs2-hdp beeline -u "${url}" -n hive \
        --showHeader=false --outputformat=tsv2 -e "${sql}" 2>&1)" \
      && grep -qE "^read_table_ready_${table}([[:space:]]|$)" <<<"${output}"; then
    log "${database}.${table} is ready"
  else
    log "WARN: could not prepare ${database}.${table} via ${hs2_host}; the SQL smoke will fail on it:"
    tail -n 20 <<<"${output}" >&2
  fi
}

if is_running "stand-proxy" && is_running "stand-hs2-hdp" && is_running "stand-hs2"; then
  log "Ensuring SQL smoke read tables ${HDP_READ_TABLE} and ${APACHE_READ_TABLE}..."
  ensure_read_table hs2-hdp hdp__default "${HDP_READ_TABLE}"
  ensure_read_table hs2 apache__default "${APACHE_READ_TABLE}"
fi

log "Stand prerequisites initialized successfully."
