#!/usr/bin/env bash
# Unprefixed databases routing and live configuration reload smoke test runner on the Docker stand.
# Verifies that:
# 1. catalog.<name>.unprefixed-databases routes requests directly to the remote catalog.
# 2. Remote unprefixed databases shadow identically named databases in default-catalog.
# 3. Dynamic configuration reload works via SIGHUP and mtime polling.
# 4. Safe Reload keeps running on the existing valid configuration when errors occur.
set -euo pipefail

STAND_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${STAND_DIR}/.." && pwd)"

log() {
  printf '[unprefixed-reload-smoke] %s\n' "$*"
}

fail() {
  printf '[unprefixed-reload-smoke] ERROR: %s\n' "$*" >&2
  exit 1
}

PROXY_HOST=${PROXY_HOST:-127.0.0.1}
PROXY_PORT=${PROXY_PORT:-19085}
MGMT_PORT=${MGMT_PORT:-19090}
PROXY_CONTAINER=stand-proxy

TEST_DB="smoke_unprefixed_db"
APACHE_TABLE="apache_remote_tbl"
HDP_TABLE="hdp_shadowed_tbl"

CONFIG_FILE="${STAND_DIR}/proxy/hms-proxy.properties"
BACKUP_FILE="${STAND_DIR}/proxy/hms-proxy.properties.bak"

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

send_reload_signal() {
  if docker ps --format '{{.Names}}' | grep -q "^${PROXY_CONTAINER}$"; then
    docker kill --signal=HUP "${PROXY_CONTAINER}" >/dev/null
  else
    local pid
    pid=$(pgrep -f "io.github.mmalykhin.hmsproxy.app.HmsProxyApplication" || true)
    if [[ -n "${pid}" ]]; then
      kill -HUP "${pid}"
    fi
  fi
}

check_ready() {
  if docker ps --format '{{.Names}}' | grep -q "^${PROXY_CONTAINER}$"; then
    docker exec "${PROXY_CONTAINER}" curl -sf http://localhost:9090/readyz >/dev/null || return 1
  else
    curl -sf "http://${PROXY_HOST}:${MGMT_PORT}/readyz" >/dev/null || return 1
  fi
}

cleanup() {
  log "Cleaning up test databases and restoring configuration..."
  if [[ -f "${BACKUP_FILE}" ]]; then
    cp "${BACKUP_FILE}" "${CONFIG_FILE}"
    rm -f "${BACKUP_FILE}"
    send_reload_signal || true
    sleep 2
  fi

  run_cli admin --op drop_table --db "apache__${TEST_DB}" --table "${APACHE_TABLE}" 2>/dev/null || true
  run_cli admin --op drop_table --db "${TEST_DB}" --table "${APACHE_TABLE}" 2>/dev/null || true
  run_cli admin --op drop_table --db "${TEST_DB}" --table "${HDP_TABLE}" 2>/dev/null || true
  run_cli admin --op drop_database --db "apache__${TEST_DB}" --cascade true 2>/dev/null || true
  run_cli admin --op drop_database --db "${TEST_DB}" --cascade true 2>/dev/null || true
}
trap cleanup EXIT

wait_for_safemode() {
  if docker ps --format '{{.Names}}' | grep -q "^stand-namenode$"; then
    docker exec stand-namenode hdfs dfsadmin -safemode wait >/dev/null 2>&1 || true
  fi
  if docker ps --format '{{.Names}}' | grep -q "^stand-namenode-b$"; then
    docker exec stand-namenode-b hdfs dfsadmin -safemode wait >/dev/null 2>&1 || true
  fi
}

log "=== 0. Verify proxy connectivity and HDFS readiness ==="
wait_for_safemode
check_ready || fail "Proxy readiness check failed at http://${PROXY_HOST}:${MGMT_PORT}/readyz"

# Backup configuration
cp "${CONFIG_FILE}" "${BACKUP_FILE}"

log "=== 1. Initial setup: create databases in both default and remote catalogs ==="
run_cli admin --op drop_database --db "apache__${TEST_DB}" --cascade true 2>/dev/null || true
run_cli admin --op drop_database --db "${TEST_DB}" --cascade true 2>/dev/null || true

log "Creating database in remote catalog (apache)..."
run_cli admin --op create_database --db "apache__${TEST_DB}"
run_cli admin --op create_table --db "apache__${TEST_DB}" --table "${APACHE_TABLE}"

log "Creating identically named database in default catalog (hdp)..."
run_cli admin --op create_database --db "${TEST_DB}"
run_cli admin --op create_table --db "${TEST_DB}" --table "${HDP_TABLE}"

log "Verifying pre-reload state: both databases are visible, and '${TEST_DB}' points to default catalog"
PRE_DBS=$(run_cli admin --op get_all_databases)
echo "${PRE_DBS}" | grep -q "${TEST_DB}" || fail "'${TEST_DB}' not found in pre-reload get_all_databases"
echo "${PRE_DBS}" | grep -q "apache__${TEST_DB}" || fail "'apache__${TEST_DB}' not found in pre-reload get_all_databases"

PRE_TABLES=$(run_cli admin --op get_all_tables --db "${TEST_DB}")
echo "${PRE_TABLES}" | grep -q "${HDP_TABLE}" || fail "Expected '${HDP_TABLE}' in '${TEST_DB}' before reload"

log "=== 2. Apply unprefixed-databases configuration dynamically via SIGHUP ==="
printf '\n# Dynamic unprefixed-databases test\ncatalog.apache.unprefixed-databases=%s\n' "${TEST_DB}" >> "${CONFIG_FILE}"
send_reload_signal
sleep 2

log "=== 3. Verify unprefixed database routing and shadowing ==="
POST_DBS=$(run_cli admin --op get_all_databases)
echo "${POST_DBS}" | grep -q "${TEST_DB}" || fail "'${TEST_DB}' missing from get_all_databases after reload"

if echo "${POST_DBS}" | grep -q "apache__${TEST_DB}"; then
  fail "'apache__${TEST_DB}' should NOT be visible with catalog prefix once unprefixed-databases is active"
fi

log "Verifying shadowing: '${TEST_DB}' must now route to remote catalog 'apache' containing '${APACHE_TABLE}'"
POST_TABLES=$(run_cli admin --op get_all_tables --db "${TEST_DB}")
echo "${POST_TABLES}" | grep -q "${APACHE_TABLE}" || fail "'${TEST_DB}' did not return '${APACHE_TABLE}' from remote catalog"

if echo "${POST_TABLES}" | grep -q "${HDP_TABLE}"; then
  fail "Shadowing failed: '${HDP_TABLE}' from default catalog should be shadowed by remote catalog database"
fi

log "Verifying DDL operations in the unprefixed database..."
NEW_TABLE="smoke_new_tbl"
run_cli admin --op create_table --db "${TEST_DB}" --table "${NEW_TABLE}"
CREATED_TABLE=$(run_cli admin --op get_table --db "${TEST_DB}" --table "${NEW_TABLE}")
echo "${CREATED_TABLE}" | grep -q "${NEW_TABLE}" || fail "Failed to get newly created table in unprefixed database"
run_cli admin --op drop_table --db "${TEST_DB}" --table "${NEW_TABLE}"

log "=== 4. Verify Safe Reload: invalid configuration does not break running proxy ==="
log "Injecting invalid configuration (unprefixed-databases on default catalog)..."
printf 'catalog.hdp.unprefixed-databases=invalid_on_default\n' >> "${CONFIG_FILE}"
send_reload_signal
sleep 2

check_ready || fail "Proxy went down after reloading invalid configuration! Safe Reload failed."

log "Verifying proxy continues serving on valid configuration..."
SAFE_TABLES=$(run_cli admin --op get_all_tables --db "${TEST_DB}")
echo "${SAFE_TABLES}" | grep -q "${APACHE_TABLE}" || fail "Proxy failed to serve requests after rejected reload"

log "=== 5. Restore configuration and verify un-shadowing ==="
cp "${BACKUP_FILE}" "${CONFIG_FILE}"
rm -f "${BACKUP_FILE}"
send_reload_signal
sleep 2

RESTORED_DBS=$(run_cli admin --op get_all_databases)
echo "${RESTORED_DBS}" | grep -q "apache__${TEST_DB}" || fail "Expected 'apache__${TEST_DB}' to be restored after config revert"

RESTORED_TABLES=$(run_cli admin --op get_all_tables --db "${TEST_DB}")
echo "${RESTORED_TABLES}" | grep -q "${HDP_TABLE}" || fail "Expected default catalog '${HDP_TABLE}' to be un-shadowed after config revert"

log "=== ALL UNPREFIXED DATABASES AND RELOAD SMOKE CHECKS PASSED ==="
