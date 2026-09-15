#!/usr/bin/env bash
# HiveServer2 / JDBC converted schema pattern routing smoke test on the Docker stand.
# Verifies that get_databases and get_table_meta with patterns translated by
# HiveServer2 convertSchemaPattern (e.g. "catalog..db", "catalog..*") route correctly.
set -euo pipefail

STAND_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${STAND_DIR}/.." && pwd)"

log() {
  printf '[schema-pattern-smoke] %s\n' "$*"
}

fail() {
  printf '[schema-pattern-smoke] ERROR: %s\n' "$*" >&2
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

log "=== 1. Setup temporary test database in default catalog ==="
TEST_DB="smoke_pattern_db"
TEST_TABLE="smoke_test_tbl"

run_cli admin --op drop_table --db "${TEST_DB}" --table "${TEST_TABLE}" 2>/dev/null || true
run_cli admin --op drop_database --db "${TEST_DB}" --cascade true 2>/dev/null || true

cleanup() {
  log "Cleaning up test metadata..."
  run_cli admin --op drop_table --db "${TEST_DB}" --table "${TEST_TABLE}" 2>/dev/null || true
  run_cli admin --op drop_database --db "${TEST_DB}" --cascade true 2>/dev/null || true
}
trap cleanup EXIT

run_cli admin --op create_database --db "${TEST_DB}"
run_cli admin --op create_table --db "${TEST_DB}" --table "${TEST_TABLE}"

log "=== 2. Verify get_databases with converted pattern (double-dot) ==="
# Test default catalog with double-dot (e.g. hdp..smoke_pattern_db)
HDP_DBS=$(run_cli admin --op get_databases --pattern "hdp..${TEST_DB}")
log "get_databases for 'hdp..${TEST_DB}': ${HDP_DBS}"
echo "${HDP_DBS}" | grep -q "${TEST_DB}" || fail "get_databases 'hdp..${TEST_DB}' did not return ${TEST_DB}"

# Test remote catalog with double-dot (apache..default)
APACHE_DBS=$(run_cli admin --op get_databases --pattern "apache..default")
log "get_databases for 'apache..default': ${APACHE_DBS}"
echo "${APACHE_DBS}" | grep -q "apache__default" || fail "get_databases 'apache..default' did not return apache__default"

log "=== 3. Verify get_table_meta with converted pattern (DBeaver/Hue query shape) ==="
# DBeaver queries table meta for the specific schema using the double-dot pattern
HDP_META=$(run_cli admin --op get_table_meta --pattern "hdp..${TEST_DB}")
log "get_table_meta for 'hdp..${TEST_DB}': ${HDP_META}"
echo "${HDP_META}" | grep -q "${TEST_TABLE}" || fail "get_table_meta 'hdp..${TEST_DB}' did not contain ${TEST_TABLE}"

# Remote catalog table meta
APACHE_META=$(run_cli admin --op get_table_meta --pattern "apache..default")
log "get_table_meta for 'apache..default': ${APACHE_META}"
echo "${APACHE_META}" | grep -q "table_meta=\[" || fail "get_table_meta 'apache..default' failed"

log "=== 4. Verify get_table_meta with wildcard converted pattern ==="
APACHE_WILDCARD=$(run_cli admin --op get_table_meta --pattern "apache..*")
log "get_table_meta for 'apache..*': ${APACHE_WILDCARD}"
echo "${APACHE_WILDCARD}" | grep -q "table_meta=\[" || fail "get_table_meta 'apache..*' failed"

log "=== 5. Negative test: unknown catalog pattern returns empty without failing ==="
UNKNOWN_META=$(run_cli admin --op get_table_meta --pattern "unknown_catalog..default")
log "get_table_meta for 'unknown_catalog..default': ${UNKNOWN_META}"
echo "${UNKNOWN_META}" | grep -q "table_meta=\[\]" || fail "unknown catalog pattern should return empty table_meta"

log "=== ALL SCHEMA PATTERN SMOKE CHECKS PASSED ==="
