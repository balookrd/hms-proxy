#!/usr/bin/env bash
# Cross-DC Resilience & High Availability smoke test runner on the Docker stand.
# Exercises:
# 1. Table & Partition metadata caches (hit, miss, LRU, single-flight coalescing).
# 2. DDL cache invalidation on table mutations.
# 3. Serve-stale-on-error during remote catalog / WAN network outages.
# 4. Granular /readyz readiness probing (remote catalog does not evict local proxy).
# 5. Catalog shadow / replica fallback for read-only RPCs.
# 6. Strict split-brain protection (denial of mutating RPCs to shadow replica).
# 7. Lenient startup mode (proxy boots cleanly when secondary remote metastore is offline).
set -euo pipefail

STAND_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${STAND_DIR}/.." && pwd)"

log() {
  printf '[cross-dc-smoke] %s\n' "$*"
}

fail() {
  printf '[cross-dc-smoke] ERROR: %s\n' "$*" >&2
  exit 1
}

PROXY_HOST=${PROXY_HOST:-127.0.0.1}
PROXY_PORT=${PROXY_PORT:-19085}
MGMT_PORT=${MGMT_PORT:-19090}
PROXY_CONTAINER=stand-proxy
APACHE_CONTAINER=stand-hms-apache
TEST_DB="cross_dc_smoke_db"
TEST_TBL="tbl_cached_test"
UNIQUE_TBL="tbl_apache_unique"
SHADOW_TBL="tbl_shadow_test"

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
  local matched
  matched=$(echo "${metrics}" | grep -E "^${pattern}" || true)
  if [[ -z "${matched}" ]]; then
    echo "0"
  else
    echo "${matched}" | awk '{s+=$2} END {print s+0}'
  fi
}

get_readyz_code_and_body() {
  if docker ps --format '{{.Names}}' | grep -q "^${PROXY_CONTAINER}$"; then
    docker exec "${PROXY_CONTAINER}" curl -s -w "\n%{http_code}" http://localhost:9090/readyz || true
  else
    curl -s -w "\n%{http_code}" "http://${PROXY_HOST}:${MGMT_PORT}/readyz" || true
  fi
}

wait_for_proxy_ready() {
  local max_seconds=${1:-30}
  log "Waiting up to ${max_seconds}s for proxy readiness..."
  for _ in $(seq 1 "${max_seconds}"); do
    local out
    out=$(get_readyz_code_and_body)
    local code
    code=$(echo "${out}" | tail -n 1)
    if [[ "${code}" == "200" ]]; then
      log "Proxy is ready (HTTP 200)"
      return 0
    fi
    sleep 1
  done
  fail "Proxy did not become ready within ${max_seconds}s"
}

cleanup() {
  log "Cleaning up test databases and restoring standard stand configuration..."
  # Ensure secondary metastore is started if it was stopped
  if docker ps -a --format '{{.Names}}' | grep -q "^${APACHE_CONTAINER}$"; then
    docker start "${APACHE_CONTAINER}" >/dev/null 2>&1 || true
    sleep 3
  fi

  run_cli admin --op drop_table --db "${TEST_DB}" --table "${TEST_TBL}" 2>/dev/null || true
  run_cli admin --op drop_table --db "${TEST_DB}" --table "${SHADOW_TBL}" 2>/dev/null || true
  run_cli admin --op drop_database --db "${TEST_DB}" --cascade true 2>/dev/null || true

  run_cli admin --op drop_table --db "apache__${TEST_DB}" --table "${TEST_TBL}" 2>/dev/null || true
  run_cli admin --op drop_table --db "apache__${TEST_DB}" --table "${UNIQUE_TBL}" 2>/dev/null || true
  run_cli admin --op drop_database --db "apache__${TEST_DB}" --cascade true 2>/dev/null || true

  if docker ps --format '{{.Names}}' | grep -q "^${PROXY_CONTAINER}$"; then
    (cd "${STAND_DIR}" && PROXY_CONFIG=/opt/hms-proxy/hms-proxy.properties docker compose up -d --force-recreate proxy >/dev/null 2>&1) || true
  fi
}
trap cleanup EXIT

log "=== 1. Starting proxy with cross-DC resilience configuration ==="
if docker ps --format '{{.Names}}' | grep -q "^${PROXY_CONTAINER}$"; then
  (cd "${STAND_DIR}" && PROXY_CONFIG=/opt/hms-proxy/hms-proxy-cross-dc.properties docker compose up -d --force-recreate proxy >/dev/null 2>&1)
  wait_for_proxy_ready 40
fi

log "=== 2. Setting up test metadata in default (hdp) and remote (apache) catalogs ==="
run_cli admin --op drop_table --db "${TEST_DB}" --table "${TEST_TBL}" 2>/dev/null || true
run_cli admin --op drop_table --db "${TEST_DB}" --table "${SHADOW_TBL}" 2>/dev/null || true
run_cli admin --op drop_database --db "${TEST_DB}" --cascade true 2>/dev/null || true
run_cli admin --op create_database --db "${TEST_DB}"

run_cli admin --op drop_table --db "apache__${TEST_DB}" --table "${TEST_TBL}" 2>/dev/null || true
run_cli admin --op drop_table --db "apache__${TEST_DB}" --table "${UNIQUE_TBL}" 2>/dev/null || true
run_cli admin --op drop_database --db "apache__${TEST_DB}" --cascade true 2>/dev/null || true
run_cli admin --op create_database --db "apache__${TEST_DB}"

# Create partitioned table in default catalog
run_cli admin --op create_table --db "${TEST_DB}" --table "${TEST_TBL}" --partition-keys "dt:string"
run_cli admin --op add_partition --db "${TEST_DB}" --table "${TEST_TBL}" --part-vals 2026-09-22

# Create partitioned table in remote catalog
run_cli admin --op create_table --db "apache__${TEST_DB}" --table "${TEST_TBL}" --partition-keys "dt:string"
run_cli admin --op add_partition --db "apache__${TEST_DB}" --table "${TEST_TBL}" --part-vals 2026-09-22

# Create unique table in remote catalog only (not present in hdp, so outage triggers serve-stale)
run_cli admin --op create_table --db "apache__${TEST_DB}" --table "${UNIQUE_TBL}"

log "=== 3. Testing Table and Partition Metadata Caches ==="
TABLE_MISS_1=$(get_metric_sum 'hms_proxy_cache_requests_total\{.*cache="table_metadata".*result="miss".*\}')
TABLE_HIT_1=$(get_metric_sum 'hms_proxy_cache_requests_total\{.*cache="table_metadata".*result="hit".*\}')

# Initial get_table: should be cache miss
run_cli admin --op get_table --db "${TEST_DB}" --table "${TEST_TBL}" >/dev/null
TABLE_MISS_2=$(get_metric_sum 'hms_proxy_cache_requests_total\{.*cache="table_metadata".*result="miss".*\}')
if (( TABLE_MISS_2 <= TABLE_MISS_1 )); then
  fail "Expected table_metadata miss count to increase (was ${TABLE_MISS_1}, now ${TABLE_MISS_2})"
fi
log "Table cache miss verified on first lookup: +$(( TABLE_MISS_2 - TABLE_MISS_1 ))"

# Second get_table: should be cache hit
run_cli admin --op get_table --db "${TEST_DB}" --table "${TEST_TBL}" >/dev/null
TABLE_HIT_2=$(get_metric_sum 'hms_proxy_cache_requests_total\{.*cache="table_metadata".*result="hit".*\}')
if (( TABLE_HIT_2 <= TABLE_HIT_1 )); then
  fail "Expected table_metadata hit count to increase (was ${TABLE_HIT_1}, now ${TABLE_HIT_2})"
fi
log "Table cache hit verified on subsequent lookup: +$(( TABLE_HIT_2 - TABLE_HIT_1 ))"

# Partition cache checks
PART_MISS_1=$(get_metric_sum 'hms_proxy_cache_requests_total\{.*cache="partition_metadata".*result="miss".*\}')
PART_HIT_1=$(get_metric_sum 'hms_proxy_cache_requests_total\{.*cache="partition_metadata".*result="hit".*\}')

run_cli admin --op get_partition --db "${TEST_DB}" --table "${TEST_TBL}" --part-vals 2026-09-22 >/dev/null
PART_MISS_2=$(get_metric_sum 'hms_proxy_cache_requests_total\{.*cache="partition_metadata".*result="miss".*\}')
if (( PART_MISS_2 <= PART_MISS_1 )); then
  fail "Expected partition_metadata miss count to increase (was ${PART_MISS_1}, now ${PART_MISS_2})"
fi
log "Partition cache miss verified on first lookup: +$(( PART_MISS_2 - PART_MISS_1 ))"

run_cli admin --op get_partition --db "${TEST_DB}" --table "${TEST_TBL}" --part-vals 2026-09-22 >/dev/null
PART_HIT_2=$(get_metric_sum 'hms_proxy_cache_requests_total\{.*cache="partition_metadata".*result="hit".*\}')
if (( PART_HIT_2 <= PART_HIT_1 )); then
  fail "Expected partition_metadata hit count to increase (was ${PART_HIT_1}, now ${PART_HIT_2})"
fi
log "Partition cache hit verified on subsequent lookup: +$(( PART_HIT_2 - PART_HIT_1 ))"

log "=== 4. Testing Automatic DDL Cache Invalidation ==="
# Altering table should invalidate table and partition caches
run_cli admin --op alter_table --db "${TEST_DB}" --table "${TEST_TBL}" >/dev/null
TABLE_MISS_BEFORE_ALTER=$(get_metric_sum 'hms_proxy_cache_requests_total\{.*cache="table_metadata".*result="miss".*\}')

run_cli admin --op get_table --db "${TEST_DB}" --table "${TEST_TBL}" >/dev/null
TABLE_MISS_AFTER_ALTER=$(get_metric_sum 'hms_proxy_cache_requests_total\{.*cache="table_metadata".*result="miss".*\}')

if (( TABLE_MISS_AFTER_ALTER <= TABLE_MISS_BEFORE_ALTER )); then
  fail "Expected cache miss after alter_table DDL invalidation (was ${TABLE_MISS_BEFORE_ALTER}, now ${TABLE_MISS_AFTER_ALTER})"
fi
log "Cache invalidation on alter_table verified successfully"

log "=== 5. Testing Serve-Stale-on-Error on Remote Outage ==="
# Prime cache for remote unique table
run_cli admin --op get_table --db "apache__${TEST_DB}" --table "${UNIQUE_TBL}" >/dev/null

# Wait for 3.5s so cache entry expires (TTL is 3s in hms-proxy-cross-dc.properties)
log "Waiting 4s for cache TTL to expire..."
sleep 4

log "Simulating remote datacenter outage: stopping ${APACHE_CONTAINER}..."
docker stop "${APACHE_CONTAINER}" >/dev/null

STALE_HIT_1=$(get_metric_sum 'hms_proxy_cache_requests_total\{.*cache="table_metadata".*result="stale_hit".*\}')

# Query remote table while secondary HMS is down: serve-stale must return cached copy
log "Querying remote table while backend is down..."
REMOTE_TBL_DATA=$(run_cli admin --op get_table --db "apache__${TEST_DB}" --table "${UNIQUE_TBL}")
echo "${REMOTE_TBL_DATA}" | grep -q "table=apache__${TEST_DB}.${UNIQUE_TBL}" || fail "Failed to retrieve remote table via serve-stale"

STALE_HIT_2=$(get_metric_sum 'hms_proxy_cache_requests_total\{.*cache="table_metadata".*result="stale_hit".*\}')
if (( STALE_HIT_2 <= STALE_HIT_1 )); then
  fail "Expected stale_hit metric count to increase (was ${STALE_HIT_1}, now ${STALE_HIT_2})"
fi
log "Serve-stale-on-error verified: remote table returned from stale cache, stale_hit count +$(( STALE_HIT_2 - STALE_HIT_1 ))"

log "=== 6. Testing Granular /readyz Health Probing ==="
# While secondary HMS is down, verify /readyz remains 200 because required-for-readiness=false for apache
READYZ_OUT=$(get_readyz_code_and_body)
READYZ_CODE=$(echo "${READYZ_OUT}" | tail -n 1)
READYZ_BODY=$(echo "${READYZ_OUT}" | sed '$d')

if [[ "${READYZ_CODE}" != "200" ]]; then
  fail "Expected /readyz to return 200 OK when secondary catalog is down (required-for-readiness=false), got: ${READYZ_CODE}"
fi

echo "${READYZ_BODY}" | grep -q '"backend":"apache"' || fail "apache catalog not found in /readyz body"
echo "${READYZ_BODY}" | grep -q '"requiredForReadiness":false' || fail "apache catalog requiredForReadiness flag not false"
echo "${READYZ_BODY}" | grep -q '"connected":false' || fail "apache catalog connected flag not false during outage"
log "Granular readiness verified: proxy is ready (200), local LB will not evict proxy during remote DC outage"

log "=== 7. Testing Catalog Shadow Fallback and Split-Brain Protection ==="
# Create shadow table on local replica (hdp)
run_cli admin --op create_table --db "${TEST_DB}" --table "${SHADOW_TBL}" >/dev/null

FALLBACK_1=$(get_metric_sum 'hms_proxy_catalog_fallback_total\{.*primary_catalog="apache".*fallback_catalog="hdp".*\}')

# Read-only RPC (get_table) targeting apache should transparently fall back to hdp
log "Querying shadow table via failed apache catalog (should fall back to hdp)..."
SHADOW_DATA=$(run_cli admin --op get_table --db "apache__${TEST_DB}" --table "${SHADOW_TBL}")
echo "${SHADOW_DATA}" | grep -q "table=apache__${TEST_DB}.${SHADOW_TBL}" || fail "Failed to retrieve shadow table via fallback"

FALLBACK_2=$(get_metric_sum 'hms_proxy_catalog_fallback_total\{.*primary_catalog="apache".*fallback_catalog="hdp".*\}')
if (( FALLBACK_2 <= FALLBACK_1 )); then
  fail "Expected catalog fallback metric count to increase (was ${FALLBACK_1}, now ${FALLBACK_2})"
fi
log "Shadow fallback verified: read-only call succeeded via replica, metric incremented by +$(( FALLBACK_2 - FALLBACK_1 ))"

# Verify strict split-brain protection: mutating RPC (create_table) MUST be rejected
log "Verifying split-brain protection (mutating RPC must be denied on outage)..."
if run_cli admin --op create_table --db "apache__${TEST_DB}" --table "tbl_forbidden_split_brain" >/dev/null 2>&1; then
  fail "Mutating RPC (create_table) succeeded on failed catalog! Split-brain protection failed."
fi
log "Split-brain protection verified: mutating RPC was rejected with error as expected"

log "=== 8. Testing Lenient Startup Mode ==="
# Restart proxy while stand-hms-apache is STILL STOPPED
log "Restarting proxy while apache metastore is stopped (lenient startup test)..."
(cd "${STAND_DIR}" && WAIT_FOR_APACHE=false PROXY_CONFIG=/opt/hms-proxy/hms-proxy-cross-dc.properties docker compose up -d --force-recreate proxy >/dev/null 2>&1)
wait_for_proxy_ready 40

# Verify local catalog works normally
HDP_DATA=$(run_cli admin --op get_table --db "${TEST_DB}" --table "${TEST_TBL}")
echo "${HDP_DATA}" | grep -q "table=${TEST_DB}.${TEST_TBL}" || fail "Local default catalog unavailable after lenient startup"
log "Lenient startup verified: proxy booted successfully with offline secondary catalog and serves local traffic"

log "=== 9. Restarting secondary metastore and verifying recovery ==="
docker start "${APACHE_CONTAINER}" >/dev/null
sleep 6

# Verify normal remote catalog operation restored
RECOVERED_DATA=$(run_cli admin --op get_table --db "apache__${TEST_DB}" --table "${TEST_TBL}")
echo "${RECOVERED_DATA}" | grep -q "table=apache__${TEST_DB}.${TEST_TBL}" || fail "Remote catalog failed to recover after restart"
log "Remote catalog recovery verified successfully"

log "=== ALL CROSS-DC RESILIENCE SMOKE CHECKS PASSED ==="
