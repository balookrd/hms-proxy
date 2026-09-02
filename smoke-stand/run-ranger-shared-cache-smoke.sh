#!/usr/bin/env bash
# Apache Ranger & Shared Metadata Cache smoke test runner on the Docker stand.
set -euo pipefail

STAND_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${STAND_DIR}/.." && pwd)"

log() {
  printf '[ranger-smoke] %s\n' "$*"
}

fail() {
  printf '[ranger-smoke] ERROR: %s\n' "$*" >&2
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

if docker ps --format '{{.Names}}' | grep -q "^${PROXY_CONTAINER}$"; then
  log "Ensuring proxy is running with hms-proxy-ranger.properties..."
  (cd "${STAND_DIR}" && PROXY_CONFIG=/opt/hms-proxy/hms-proxy-ranger.properties docker compose up -d proxy >/dev/null 2>&1)
  sleep 3
fi

log "=== 1. Setup test metadata (databases & tables) via admin user ==="

# Clean any leftover dbs first
run_cli admin --op drop_table --db sales --table orders 2>/dev/null || true
run_cli admin --op drop_table --db sales --table customers 2>/dev/null || true
run_cli admin --op drop_database --db sales --cascade true 2>/dev/null || true

run_cli admin --op drop_table --db finance --table reports 2>/dev/null || true
run_cli admin --op drop_table --db finance --table expenses 2>/dev/null || true
run_cli admin --op drop_database --db finance --cascade true 2>/dev/null || true

# Create sales db & tables
run_cli admin --op create_database --db sales
run_cli admin --op create_table --db sales --table orders
run_cli admin --op create_table --db sales --table customers

# Create finance db & tables
run_cli admin --op create_database --db finance
run_cli admin --op create_table --db finance --table reports
run_cli admin --op create_table --db finance --table expenses

log "=== 2. Alice queries get_all_databases (should see sales, NOT finance) ==="
ALICE_DBS=$(run_cli alice --op get_all_databases)
log "Alice databases: ${ALICE_DBS}"
echo "${ALICE_DBS}" | grep -q "sales" || fail "Alice expected to see sales"
if echo "${ALICE_DBS}" | grep -q "finance"; then
  fail "Alice MUST NOT see finance"
fi

log "=== 3. Bob queries get_all_databases (shared cache hit; should see finance, NOT sales) ==="
BOB_DBS=$(run_cli bob --op get_all_databases)
log "Bob databases: ${BOB_DBS}"
echo "${BOB_DBS}" | grep -q "finance" || fail "Bob expected to see finance"
if echo "${BOB_DBS}" | grep -q "sales"; then
  fail "Bob MUST NOT see sales"
fi

log "=== 4. Alice accesses get_database and get_all_tables ==="
run_cli alice --op get_database --db sales | grep -q "database.name=sales" || fail "Alice get_database(sales) failed"
ALICE_TABLES=$(run_cli alice --op get_all_tables --db sales)
log "Alice tables in sales: ${ALICE_TABLES}"
echo "${ALICE_TABLES}" | grep -q "orders" || fail "Alice expected to see orders"
echo "${ALICE_TABLES}" | grep -q "customers" || fail "Alice expected to see customers"

log "=== 5. Bob tries to access sales database (should be rejected by Ranger) ==="
if run_cli bob --op get_database --db sales 2>/dev/null; then
  fail "Bob should NOT have access to get_database(sales)"
else
  log "Bob was correctly denied access to get_database(sales)"
fi

log "=== 6. Bob accesses get_database and get_all_tables on finance ==="
run_cli bob --op get_database --db finance | grep -q "database.name=finance" || fail "Bob get_database(finance) failed"
BOB_TABLES=$(run_cli bob --op get_all_tables --db finance)
log "Bob tables in finance: ${BOB_TABLES}"
echo "${BOB_TABLES}" | grep -q "reports" || fail "Bob expected to see reports"
echo "${BOB_TABLES}" | grep -q "expenses" || fail "Bob expected to see expenses"

log "=== 7. Alice tries to access finance database (should be rejected by Ranger) ==="
if run_cli alice --op get_database --db finance 2>/dev/null; then
  fail "Alice should NOT have access to get_database(finance)"
else
  log "Alice was correctly denied access to get_database(finance)"
fi

log "=== 8. Eve (unauthorized user) queries get_all_databases and get_database ==="
EVE_DBS=$(run_cli eve --op get_all_databases)
log "Eve databases: ${EVE_DBS}"
if echo "${EVE_DBS}" | grep -q "sales" || echo "${EVE_DBS}" | grep -q "finance"; then
  fail "Eve MUST NOT see sales or finance"
fi
if run_cli eve --op get_database --db sales 2>/dev/null; then
  fail "Eve should NOT have access to get_database(sales)"
fi

log "=== 9. Admin queries get_all_databases (should see everything) ==="
ADMIN_DBS=$(run_cli admin --op get_all_databases)
log "Admin databases: ${ADMIN_DBS}"
echo "${ADMIN_DBS}" | grep -q "sales" || fail "Admin expected to see sales"
echo "${ADMIN_DBS}" | grep -q "finance" || fail "Admin expected to see finance"

log "=== 10. Check Prometheus metrics for cache and authorization ==="
METRICS=$(docker exec "${PROXY_CONTAINER}" curl -sf http://localhost:9090/metrics || true)
if [ -n "${METRICS}" ]; then
  echo "${METRICS}" | grep -q "hms_proxy_cache_requests_total" || fail "Missing hms_proxy_cache_requests_total in /metrics"
  echo "${METRICS}" | grep -q "hms_proxy_cache_entries" || fail "Missing hms_proxy_cache_entries in /metrics"
  echo "${METRICS}" | grep -q "hms_proxy_ranger_evaluations_total" || fail "Missing hms_proxy_ranger_evaluations_total in /metrics"
  echo "${METRICS}" | grep -q "hms_proxy_ranger_plugin_info" || fail "Missing hms_proxy_ranger_plugin_info in /metrics"
  log "Observed cache metrics: $(echo "${METRICS}" | grep "^hms_proxy_cache_" | head -n 4)"
  log "Observed ranger metrics: $(echo "${METRICS}" | grep "^hms_proxy_ranger_" | head -n 4)"
fi

log "=== 11. Cleanup test metadata ==="
run_cli admin --op drop_table --db sales --table orders || true
run_cli admin --op drop_table --db sales --table customers || true
run_cli admin --op drop_database --db sales --cascade true || true

run_cli admin --op drop_table --db finance --table reports || true
run_cli admin --op drop_table --db finance --table expenses || true
run_cli admin --op drop_database --db finance --cascade true || true

log "=== ALL RANGER & SHARED CACHE SMOKE CHECKS PASSED ==="
