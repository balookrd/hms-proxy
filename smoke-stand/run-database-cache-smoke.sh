#!/usr/bin/env bash
# Database Cache Background Auto-Refresh smoke test runner on the Docker stand.
set -euo pipefail

STAND_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${STAND_DIR}/.." && pwd)"

log() {
  printf '[db-cache-smoke] %s\n' "$*"
}

fail() {
  printf '[db-cache-smoke] ERROR: %s\n' "$*" >&2
  exit 1
}

PROXY_HOST=${PROXY_HOST:-127.0.0.1}
PROXY_PORT=${PROXY_PORT:-19085}
MGMT_PORT=${MGMT_PORT:-19090}
PROXY_CONTAINER=stand-proxy
TEST_DB="db_cache_smoke_test"

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

get_metrics() {
  if docker ps --format '{{.Names}}' | grep -q "^${PROXY_CONTAINER}$"; then
    docker exec "${PROXY_CONTAINER}" curl -sf http://localhost:9090/metrics || true
  else
    curl -sf "http://${PROXY_HOST}:${MGMT_PORT}/metrics" || true
  fi
}

get_metric_sum() {
  local pattern="$1"
  local metrics
  metrics=$(get_metrics)
  echo "${metrics}" | grep -E "^${pattern}" | awk '{s+=$2} END {print s+0}'
}

cleanup() {
  log "Cleaning up test database and restoring proxy configuration..."
  run_cli admin --op drop_database --db "${TEST_DB}" --cascade true 2>/dev/null || true
  if docker ps --format '{{.Names}}' | grep -q "^${PROXY_CONTAINER}$"; then
    (cd "${STAND_DIR}" && PROXY_CONFIG=/opt/hms-proxy/hms-proxy.properties docker compose up -d proxy >/dev/null 2>&1) || true
  fi
}
trap cleanup EXIT

if docker ps --format '{{.Names}}' | grep -q "^${PROXY_CONTAINER}$"; then
  log "Restarting proxy with hms-proxy-db-cache.properties..."
  (cd "${STAND_DIR}" && PROXY_CONFIG=/opt/hms-proxy/hms-proxy-db-cache.properties docker compose up -d proxy >/dev/null 2>&1)
  sleep 4
fi

log "=== 1. Setup test database ==="
run_cli admin --op drop_database --db "${TEST_DB}" --cascade true 2>/dev/null || true
run_cli admin --op create_database --db "${TEST_DB}"

log "=== 2. Prime database list and metadata caches ==="
DBS=$(run_cli admin --op get_all_databases)
echo "${DBS}" | grep -q "${TEST_DB}" || fail "Created database ${TEST_DB} not found in get_all_databases"

DB_INFO=$(run_cli admin --op get_database --db "${TEST_DB}")
echo "${DB_INFO}" | grep -q "database.name=${TEST_DB}" || fail "get_database for ${TEST_DB} failed"

log "=== 3. Measure initial background refresh metrics ==="
INIT_LIST_REFRESH=$(get_metric_sum 'hms_proxy_cache_refreshes_total\{.*cache="database_list".*result="success".*\}')
INIT_META_REFRESH=$(get_metric_sum 'hms_proxy_cache_refreshes_total\{.*cache="database_metadata".*result="success".*\}')
log "Initial refreshes: list=${INIT_LIST_REFRESH}, metadata=${INIT_META_REFRESH}"

log "=== 4. Wait for background refreshes (interval is 2s, waiting 6s) ==="
sleep 6

REFRESHED_LIST=$(get_metric_sum 'hms_proxy_cache_refreshes_total\{.*cache="database_list".*result="success".*\}')
REFRESHED_META=$(get_metric_sum 'hms_proxy_cache_refreshes_total\{.*cache="database_metadata".*result="success".*\}')
log "Refreshes after 6s: list=${REFRESHED_LIST}, metadata=${REFRESHED_META}"

if (( REFRESHED_LIST <= INIT_LIST_REFRESH )); then
  fail "Expected background refresh count for database_list to increase (> ${INIT_LIST_REFRESH}, got ${REFRESHED_LIST})"
fi

if (( REFRESHED_META <= INIT_META_REFRESH )); then
  fail "Expected background refresh count for database_metadata to increase (> ${INIT_META_REFRESH}, got ${REFRESHED_META})"
fi
log "Background refresh verified while active: list +$(( REFRESHED_LIST - INIT_LIST_REFRESH )), metadata +$(( REFRESHED_META - INIT_META_REFRESH ))"

log "=== 5. Verify idle activity window (activity window is 10s, waiting 12s without requests) ==="
sleep 12

IDLE_LIST=$(get_metric_sum 'hms_proxy_cache_refreshes_total\{.*cache="database_list".*result="success".*\}')
IDLE_META=$(get_metric_sum 'hms_proxy_cache_refreshes_total\{.*cache="database_metadata".*result="success".*\}')
log "Refreshes after 12s idle: list=${IDLE_LIST}, metadata=${IDLE_META}"

log "Waiting another 5s to verify refresher is idle..."
sleep 5

AFTER_IDLE_LIST=$(get_metric_sum 'hms_proxy_cache_refreshes_total\{.*cache="database_list".*result="success".*\}')
AFTER_IDLE_META=$(get_metric_sum 'hms_proxy_cache_refreshes_total\{.*cache="database_metadata".*result="success".*\}')
log "Refreshes after additional 5s idle: list=${AFTER_IDLE_LIST}, metadata=${AFTER_IDLE_META}"

if (( AFTER_IDLE_LIST != IDLE_LIST )); then
  fail "Expected database_list refresh to stop after activity window expired (was ${IDLE_LIST}, now ${AFTER_IDLE_LIST})"
fi

if (( AFTER_IDLE_META != IDLE_META )); then
  fail "Expected database_metadata refresh to stop after activity window expired (was ${IDLE_META}, now ${AFTER_IDLE_META})"
fi
log "Idle timeout verified: no background refreshes occurred after activity window expired"

log "=== 6. Re-awaken cache with new requests and verify refresh resumes ==="
run_cli admin --op get_all_databases >/dev/null
run_cli admin --op get_database --db "${TEST_DB}" >/dev/null

log "Waiting 4s for refresher to resume..."
sleep 4

RESUMED_LIST=$(get_metric_sum 'hms_proxy_cache_refreshes_total\{.*cache="database_list".*result="success".*\}')
RESUMED_META=$(get_metric_sum 'hms_proxy_cache_refreshes_total\{.*cache="database_metadata".*result="success".*\}')
log "Refreshes after awakening: list=${RESUMED_LIST}, metadata=${RESUMED_META}"

if (( RESUMED_LIST <= AFTER_IDLE_LIST )); then
  fail "Expected database_list refresh to resume after activity (> ${AFTER_IDLE_LIST}, got ${RESUMED_LIST})"
fi

if (( RESUMED_META <= AFTER_IDLE_META )); then
  fail "Expected database_metadata refresh to resume after activity (> ${AFTER_IDLE_META}, got ${RESUMED_META})"
fi
log "Cache awakening verified: refreshes resumed successfully"

log "=== ALL DATABASE CACHE BACKGROUND REFRESH SMOKE CHECKS PASSED ==="
