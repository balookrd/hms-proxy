#!/usr/bin/env bash
# Spark SQL path-based tables and file datasource formats (csv."hdfs://...", parquet, global_temp) smoke test runner.
# Verifies that direct file relation queries and format pseudo-databases return NoSuchObjectException / empty lists
# early in hms-proxy without triggering Ranger access denials or querying backend metastores.
set -euo pipefail

STAND_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${STAND_DIR}/.." && pwd)"

log() {
  printf '[spark-path-smoke] %s\n' "$*"
}

fail() {
  printf '[spark-path-smoke] ERROR: %s\n' "$*" >&2
  exit 1
}

PROXY_HOST=${PROXY_HOST:-127.0.0.1}
PROXY_PORT=${PROXY_PORT:-19085}
PROXY_CONTAINER=stand-proxy

CLI_JAR="${REPO_DIR}/smoke-stand/proxy/hms-proxy-fat.jar"
if [[ ! -f "${CLI_JAR}" ]]; then
  CLI_JAR=$(ls -t "${REPO_DIR}"/target/hms-proxy-*-fat.jar 2>/dev/null | head -1 || true)
fi

run_cli() {
  local user="$1"
  shift
  if docker ps --format '{{.Names}}' | grep -q "^${PROXY_CONTAINER}$"; then
    docker exec -e HADOOP_USER_NAME="${user}" "${PROXY_CONTAINER}" java \
      -cp /opt/hms-proxy/hms-proxy.jar \
      io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli metadata \
      --uri "thrift://localhost:9083" \
      --user "${user}" \
      "$@"
  else
    [[ -n "${CLI_JAR}" && -f "${CLI_JAR}" ]] || fail "CLI jar not found. Run 'mvn package -DskipTests' first."
    HADOOP_USER_NAME="${user}" java \
      -cp "${CLI_JAR}" \
      io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli metadata \
      --uri "thrift://${PROXY_HOST}:${PROXY_PORT}" \
      --user "${user}" \
      "$@"
  fi
}

HDFS_PATH="hdfs://titanium/warehouse/tablespace/external/hive/amdocs_arena_lz.db/csvlogs/tasks/"

log "=== 1. Verify get_database for Spark format pseudo-databases (csv, parquet, orc, delta) ==="
for fmt in csv parquet orc json delta iceberg; do
  OUT=$(run_cli admin --op get_database --db "${fmt}" 2>&1 || true)
  if echo "${OUT}" | grep -q "Access denied"; then
    fail "get_database for '${fmt}' was rejected with Ranger 'Access denied' instead of NoSuchObjectException: ${OUT}"
  fi
  if ! echo "${OUT}" | grep -q "NoSuchObjectException"; then
    fail "get_database for '${fmt}' did not throw NoSuchObjectException: ${OUT}"
  fi
  log "get_database '${fmt}' correctly returned NoSuchObjectException without Ranger denial"
done

log "=== 2. Verify get_database for composite path-based database name csv.\"hdfs://...\" ==="
OUT=$(run_cli admin --op get_database --db "csv.\"${HDFS_PATH}\"" 2>&1 || true)
if echo "${OUT}" | grep -q "Access denied"; then
  fail "get_database for 'csv.\"${HDFS_PATH}\"' was rejected with Ranger 'Access denied': ${OUT}"
fi
if ! echo "${OUT}" | grep -q "NoSuchObjectException"; then
  fail "get_database for 'csv.\"${HDFS_PATH}\"' did not throw NoSuchObjectException: ${OUT}"
fi
log "get_database 'csv.\"${HDFS_PATH}\"' correctly returned NoSuchObjectException"

log "=== 3. Verify get_table for Spark path-based table (format + path) ==="
OUT=$(run_cli admin --op get_table --db "csv" --table "${HDFS_PATH}" 2>&1 || true)
if echo "${OUT}" | grep -q "Access denied"; then
  fail "get_table for csv + path was rejected with Ranger 'Access denied': ${OUT}"
fi
if ! echo "${OUT}" | grep -q "NoSuchObjectException"; then
  fail "get_table for csv + path did not throw NoSuchObjectException: ${OUT}"
fi
log "get_table csv + path correctly returned NoSuchObjectException"

log "=== 4. Verify get_table for quoted path table ==="
OUT=$(run_cli admin --op get_table --db "csv" --table "\"${HDFS_PATH}\"" 2>&1 || true)
if echo "${OUT}" | grep -q "Access denied"; then
  fail "get_table for csv + quoted path was rejected with Ranger 'Access denied': ${OUT}"
fi
if ! echo "${OUT}" | grep -q "NoSuchObjectException"; then
  fail "get_table for csv + quoted path did not throw NoSuchObjectException: ${OUT}"
fi
log "get_table csv + quoted path correctly returned NoSuchObjectException"

log "=== 5. Verify get_table with default db and format-prefixed table name csv.\"hdfs://...\" ==="
OUT=$(run_cli admin --op get_table --db "default" --table "csv.\"${HDFS_PATH}\"" 2>&1 || true)
if echo "${OUT}" | grep -q "Access denied"; then
  fail "get_table default + csv.\"hdfs://...\" was rejected with Ranger 'Access denied': ${OUT}"
fi
if ! echo "${OUT}" | grep -q "NoSuchObjectException"; then
  fail "get_table default + csv.\"hdfs://...\" did not throw NoSuchObjectException: ${OUT}"
fi
log "get_table default + csv.\"${HDFS_PATH}\" correctly returned NoSuchObjectException"

log "=== 6. Verify get_tables for Spark format pseudo-database returns empty list ==="
OUT=$(run_cli admin --op get_tables --db "csv" 2>&1)
echo "${OUT}" | grep -q "tables=\[\]" || fail "get_tables for 'csv' did not return empty tables list: ${OUT}"
log "get_tables 'csv' correctly returned empty list: ${OUT}"

log "=== 7. Verify get_table_meta for Spark format pattern returns empty list ==="
OUT=$(run_cli admin --op get_table_meta --pattern "csv" 2>&1)
echo "${OUT}" | grep -q "table_meta=\[\]" || fail "get_table_meta for 'csv' did not return empty list: ${OUT}"
log "get_table_meta 'csv' correctly returned empty list: ${OUT}"

log "=== 8. Verify Spark in-memory database global_temp ==="
OUT=$(run_cli admin --op get_database --db "global_temp" 2>&1 || true)
if ! echo "${OUT}" | grep -q "NoSuchObjectException"; then
  fail "get_database for 'global_temp' did not throw NoSuchObjectException: ${OUT}"
fi
OUT=$(run_cli admin --op get_tables --db "global_temp" 2>&1)
echo "${OUT}" | grep -q "tables=\[\]" || fail "get_tables for 'global_temp' did not return empty tables list: ${OUT}"
log "global_temp database and table listings correctly intercepted"

log "=== ALL SPARK PATH-BASED SMOKE CHECKS PASSED ==="
