#!/usr/bin/env bash
# Automatically prepares all Docker stand prerequisites (HDFS directories, safemode wait,
# and base tables for notification, transactions, and Iceberg REST smoke tests).
# Run this once after 'docker compose up -d' or let smoke runners call it automatically.
set -euo pipefail

STAND_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log() {
  printf '[init-stand] %s\n' "$*"
}

is_running() {
  local container="$1"
  docker ps --format '{{.Names}}' | grep -q "^${container}$"
}

# 1. HDFS safemode and directory initialization
if is_running "stand-namenode"; then
  log "Waiting for stand-namenode safemode..."
  docker exec stand-namenode hdfs dfsadmin -safemode wait >/dev/null 2>&1 || true

  log "Ensuring HDFS warehouse directories on stand-namenode..."
  docker exec stand-namenode bash -c \
    'hdfs dfs -mkdir -p /warehouse/apache /warehouse/hdp /warehouse/hive4 /external && hdfs dfs -chmod -R 1777 /warehouse /external' >/dev/null 2>&1 || true
fi

if is_running "stand-namenode-b"; then
  log "Waiting for stand-namenode-b safemode..."
  docker exec stand-namenode-b hdfs dfsadmin -safemode wait >/dev/null 2>&1 || true

  log "Ensuring HDFS warehouse directories on stand-namenode-b..."
  docker exec stand-namenode-b bash -c \
    'hdfs dfs -mkdir -p /warehouse/apache /warehouse/hdp /warehouse/hive4 /external && hdfs dfs -chmod -R 1777 /warehouse /external' >/dev/null 2>&1 || true
fi

# 2. Base ORC transaction table for notification and lock smokes
if is_running "stand-hs2"; then
  log "Ensuring smoke_txn_tbl exists in default catalog..."
  docker exec stand-hs2 bash -c \
    "java -cp '/opt/hs2/conf:/opt/hs2/lib/*' org.apache.hive.beeline.BeeLine \
      -u 'jdbc:hive2://localhost:10000/default' -n hive --silent=true \
      -e 'create table if not exists smoke_txn_tbl (id int) stored as orc;'" >/dev/null 2>&1 || true
fi

# 3. Iceberg table in default catalog (hdp)
if is_running "stand-proxy" && is_running "stand-namenode"; then
  log "Ensuring smoke_iceberg_tbl exists via Iceberg REST writer..."
  docker exec stand-proxy java \
    --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
    --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
    -jar /opt/hms-proxy/iceberg-rest-writer.jar create \
    --uri http://proxy:9183 --namespace default --table smoke_iceberg_tbl >/dev/null 2>&1 || true
fi

# 4. Iceberg table in second catalog (apache on stand-namenode-b)
if is_running "stand-namenode-b" && is_running "stand-hs2"; then
  log "Ensuring smoke_iceberg_tbl_ap metadata exists in stand-namenode-b..."
  docker exec stand-namenode-b bash -c \
    'echo '\''{"format-version":2,"table-uuid":"546edc8c-8dcd-444d-bbb1-8d1788a27ffa","location":"hdfs://namenode-b:8020/warehouse/apache/smoke_iceberg_tbl_ap","last-sequence-number":0,"last-updated-ms":1791929548977,"last-column-id":2,"current-schema-id":0,"schemas":[{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":false,"type":"int"},{"id":2,"name":"src","required":false,"type":"string"}]}],"default-spec-id":0,"partition-specs":[{"spec-id":0,"fields":[]}],"last-partition-id":999,"default-sort-order-id":0,"sort-orders":[{"order-id":0,"fields":[]}],"properties":{"engine.hive.enabled":"true","write.parquet.compression-codec":"zstd"},"current-snapshot-id":-1,"refs":{},"snapshots":[],"statistics":[],"partition-statistics":[],"snapshot-log":[],"metadata-log":[]}'\'' > /tmp/00000-smoke.metadata.json && \
     hdfs dfs -mkdir -p /warehouse/apache/smoke_iceberg_tbl_ap/metadata && \
     hdfs dfs -put -f /tmp/00000-smoke.metadata.json /warehouse/apache/smoke_iceberg_tbl_ap/metadata/' >/dev/null 2>&1 || true

  log "Ensuring smoke_iceberg_tbl_ap table exists in apache catalog..."
  docker exec stand-hs2 bash -c \
    "java -cp '/opt/hs2/conf:/opt/hs2/lib/*' org.apache.hive.beeline.BeeLine \
      -u 'jdbc:hive2://localhost:10000/default' -n hive --silent=true \
      -e \"create external table if not exists apache__default.smoke_iceberg_tbl_ap (id int, ds string) \
          stored as parquet \
          location 'hdfs://namenode-b:8020/warehouse/apache/smoke_iceberg_tbl_ap' \
          tblproperties ( \
            'table_type'='ICEBERG', \
            'metadata_location'='hdfs://namenode-b:8020/warehouse/apache/smoke_iceberg_tbl_ap/metadata/00000-smoke.metadata.json');\"" >/dev/null 2>&1 || true
fi

log "Stand prerequisites initialized successfully."
