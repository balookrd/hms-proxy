#!/usr/bin/env bash
# Apache Ranger & Shared Metadata Cache smoke test runner on the Docker stand.
# Exercises kerberized Ranger authorization policies, granular table permissions,
# group-based access rules, negative denials, and shared metadata caching.
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
KERBEROS=${KERBEROS:-true}
REALM=${REALM:-SMOKE.LOCAL}

CLI_JAR="${REPO_DIR}/smoke-stand/proxy/hms-proxy-fat.jar"
if [[ ! -f "${CLI_JAR}" ]]; then
  CLI_JAR=$(ls -t "${REPO_DIR}"/target/hms-proxy-*-fat.jar 2>/dev/null | head -1 || true)
fi

run_cli() {
  local user="$1"
  shift
  local groups=""
  local -a extra_cli_args=()
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --groups)
        groups="$2"
        shift 2
        ;;
      *)
        extra_cli_args+=("$1")
        shift
        ;;
    esac
  done

  if docker ps --format '{{.Names}}' | grep -q "^${PROXY_CONTAINER}$"; then
    local -a auth_args=()
    if [[ "${KERBEROS}" == "true" ]]; then
      local user_keytab="/keytabs/${user}.keytab"
      local client_principal="${user}@${REALM}"
      if ! docker exec "${PROXY_CONTAINER}" test -f "${user_keytab}"; then
        user_keytab="/keytabs/smoke-user.keytab"
        client_principal="smoke-user@${REALM}"
      fi
      auth_args+=("--auth" "kerberos")
      auth_args+=("--server-principal" "hive/proxy@${REALM}")
      auth_args+=("--client-principal" "${client_principal}")
      auth_args+=("--keytab" "${user_keytab}")
      auth_args+=("--krb5-conf" "/etc/krb5.conf")
      auth_args+=("--set-ugi" "true")
      auth_args+=("--set-ugi-user" "${user}")
    else
      auth_args+=("--auth" "simple")
      auth_args+=("--user" "${user}")
      auth_args+=("--set-ugi" "true")
      auth_args+=("--set-ugi-user" "${user}")
    fi

    if [[ -n "${groups}" ]]; then
      auth_args+=("--groups" "${groups}")
    fi

    docker exec -e HADOOP_USER_NAME="${user}" "${PROXY_CONTAINER}" java \
      --add-opens=java.base/java.lang=ALL-UNNAMED \
      --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
      --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
      -cp /opt/hms-proxy/hms-proxy.jar \
      io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli metadata \
      --uri "thrift://localhost:9083" \
      "${auth_args[@]}" \
      "${extra_cli_args[@]}"
  else
    [[ -n "${CLI_JAR}" && -f "${CLI_JAR}" ]] || fail "CLI jar not found. Run 'mvn package -DskipTests' first."
    local -a host_auth_args=()
    if [[ "${KERBEROS}" == "true" && -f "${STAND_DIR}/kdc/krb5.conf" ]]; then
      host_auth_args+=("--auth" "kerberos")
      host_auth_args+=("--server-principal" "hive/localhost@${REALM}")
      host_auth_args+=("--client-principal" "${user}@${REALM}")
      host_auth_args+=("--keytab" "${STAND_DIR}/keytabs/${user}.keytab")
      host_auth_args+=("--krb5-conf" "${STAND_DIR}/kdc/krb5.conf")
      host_auth_args+=("--set-ugi" "true")
      host_auth_args+=("--set-ugi-user" "${user}")
    else
      host_auth_args+=("--auth" "simple")
      host_auth_args+=("--user" "${user}")
      host_auth_args+=("--set-ugi" "true")
      host_auth_args+=("--set-ugi-user" "${user}")
    fi
    if [[ -n "${groups}" ]]; then
      host_auth_args+=("--groups" "${groups}")
    fi

    local java_bin="${JAVA_HOME:+$JAVA_HOME/bin/}java"
    if ! command -v "${java_bin}" >/dev/null 2>&1; then
      java_bin="java"
    fi

    HADOOP_USER_NAME="${user}" "${java_bin}" \
      --add-opens=java.base/java.lang=ALL-UNNAMED \
      --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
      --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
      -cp "${CLI_JAR}" \
      io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli metadata \
      --uri "thrift://${PROXY_HOST}:${PROXY_PORT}" \
      "${host_auth_args[@]}" \
      "${extra_cli_args[@]}"
  fi
}

run_cli_expect_fail() {
  local user="$1"
  shift
  local output=""
  set +e
  output="$(run_cli "${user}" "$@" 2>&1)"
  local status=$?
  set -e
  if [[ ${status} -eq 0 ]]; then
    fail "Operation was expected to fail for user '${user}', but succeeded. Output: ${output}"
  fi
  log "Operation correctly denied for user '${user}'"
}

cleanup() {
  log "Cleaning up test Ranger metadata and restoring proxy..."
  docker logs "${PROXY_CONTAINER}" > "${STAND_DIR}/last-smoke-proxy.log" 2>&1 || true
  run_cli admin --op drop_table --db sales --table secret_orders 2>/dev/null || true
  run_cli admin --op drop_table --db sales --table orders 2>/dev/null || true
  run_cli admin --op drop_table --db sales --table customers 2>/dev/null || true
  run_cli admin --op drop_database --db sales --cascade true 2>/dev/null || true

  run_cli admin --op drop_table --db finance --table reports 2>/dev/null || true
  run_cli admin --op drop_table --db finance --table expenses 2>/dev/null || true
  run_cli admin --op drop_database --db finance --cascade true 2>/dev/null || true

  if docker ps --format '{{.Names}}' | grep -q "^${PROXY_CONTAINER}$"; then
    if [[ "${KERBEROS}" == "true" ]]; then
      (cd "${STAND_DIR}" && docker compose --env-file .env.kerberos --profile kerberos up -d proxy >/dev/null 2>&1) || true
    else
      (cd "${STAND_DIR}" && PROXY_CONFIG=/opt/hms-proxy/hms-proxy.properties docker compose up -d proxy >/dev/null 2>&1) || true
    fi
  fi
}
trap cleanup EXIT

if [[ "${KERBEROS}" == "true" ]]; then
  log "Ensuring proxy is running with hms-proxy-ranger-kerberos.properties (kerberized Ranger)..."
  (cd "${STAND_DIR}" && PROXY_CONFIG=/opt/hms-proxy/hms-proxy-ranger-kerberos.properties docker compose --env-file .env.kerberos --profile kerberos up -d --force-recreate proxy >/dev/null 2>&1)
else
  log "Ensuring proxy is running with hms-proxy-ranger.properties..."
  (cd "${STAND_DIR}" && PROXY_CONFIG=/opt/hms-proxy/hms-proxy-ranger.properties KERBEROS_ENABLED=false docker compose up -d --force-recreate proxy >/dev/null 2>&1)
fi

log "Waiting for proxy to be ready..."
for i in {1..45}; do
  if docker exec "${PROXY_CONTAINER}" nc -z localhost 9083 >/dev/null 2>&1 && \
     docker exec "${PROXY_CONTAINER}" curl -sf http://localhost:9090/readyz >/dev/null 2>&1; then
    break
  fi
  sleep 1
done


log "=== 1. Setup test metadata (databases & tables) via admin user ==="

# Clean any leftover dbs first
run_cli admin --op drop_table --db sales --table secret_orders 2>/dev/null || true
run_cli admin --op drop_table --db sales --table orders 2>/dev/null || true
run_cli admin --op drop_table --db sales --table customers 2>/dev/null || true
run_cli admin --op drop_database --db sales --cascade true 2>/dev/null || true

run_cli admin --op drop_table --db finance --table reports 2>/dev/null || true
run_cli admin --op drop_table --db finance --table expenses 2>/dev/null || true
run_cli admin --op drop_database --db finance --cascade true 2>/dev/null || true

# Create sales db & tables (including secret_orders which is excluded from sales_policy)
run_cli admin --op create_database --db sales
run_cli admin --op create_table --db sales --table orders
run_cli admin --op create_table --db sales --table customers
run_cli admin --op create_table --db sales --table secret_orders

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

log "=== 4. Alice accesses get_database, get_all_tables, get_tables, get_table_meta, get_table ==="
run_cli alice --op get_database --db sales | grep -q "database=sales" || fail "Alice get_database(sales) failed"

ALICE_ALL_TABLES=$(run_cli alice --op get_all_tables --db sales)
log "Alice all tables in sales: ${ALICE_ALL_TABLES}"
echo "${ALICE_ALL_TABLES}" | grep -q "orders" || fail "Alice expected to see orders"
echo "${ALICE_ALL_TABLES}" | grep -q "customers" || fail "Alice expected to see customers"
if echo "${ALICE_ALL_TABLES}" | grep -q "secret_orders"; then
  fail "Alice MUST NOT see secret_orders in get_all_tables"
fi

ALICE_PATTERN_TABLES=$(run_cli alice --op get_tables --db sales --pattern ".*")
log "Alice pattern tables in sales: ${ALICE_PATTERN_TABLES}"
echo "${ALICE_PATTERN_TABLES}" | grep -q "orders" || fail "Alice expected to see orders in get_tables"
echo "${ALICE_PATTERN_TABLES}" | grep -q "customers" || fail "Alice expected to see customers in get_tables"
if echo "${ALICE_PATTERN_TABLES}" | grep -q "secret_orders"; then
  fail "Alice MUST NOT see secret_orders in get_tables"
fi

ALICE_TABLE_META=$(run_cli alice --op get_table_meta --pattern "sales" --table ".*")
log "Alice table_meta: ${ALICE_TABLE_META}"
echo "${ALICE_TABLE_META}" | grep -q "sales.orders" || fail "Alice expected to see sales.orders in get_table_meta"
echo "${ALICE_TABLE_META}" | grep -q "sales.customers" || fail "Alice expected to see sales.customers in get_table_meta"
if echo "${ALICE_TABLE_META}" | grep -q "secret_orders"; then
  fail "Alice MUST NOT see secret_orders in get_table_meta"
fi

run_cli alice --op get_table --db sales --table orders | grep -q "table=sales.orders" || fail "Alice get_table(sales, orders) failed"
run_cli alice --op get_table --db sales --table customers | grep -q "table=sales.customers" || fail "Alice get_table(sales, customers) failed"

log "=== 5. Alice tries to access forbidden table secret_orders in sales (should be rejected by Ranger) ==="
run_cli_expect_fail alice --op get_table --db sales --table secret_orders

log "=== 6. Bob tries to access sales database and tables (should be rejected by Ranger) ==="
run_cli_expect_fail bob --op get_database --db sales
run_cli_expect_fail bob --op get_table --db sales --table orders

log "=== 7. Bob accesses get_database, get_all_tables, and get_table on finance ==="
run_cli bob --op get_database --db finance | grep -q "database=finance" || fail "Bob get_database(finance) failed"
BOB_TABLES=$(run_cli bob --op get_all_tables --db finance)
log "Bob tables in finance: ${BOB_TABLES}"
echo "${BOB_TABLES}" | grep -q "reports" || fail "Bob expected to see reports"
echo "${BOB_TABLES}" | grep -q "expenses" || fail "Bob expected to see expenses"
run_cli bob --op get_table --db finance --table reports | grep -q "table=finance.reports" || fail "Bob get_table(finance, reports) failed"

log "=== 8. Alice tries to access finance database and tables (should be rejected by Ranger) ==="
run_cli_expect_fail alice --op get_database --db finance
run_cli_expect_fail alice --op get_table --db finance --table reports

log "=== 9. Eve (unauthorized user) queries get_all_databases, get_database, get_table ==="
EVE_DBS=$(run_cli eve --op get_all_databases)
log "Eve databases: ${EVE_DBS}"
if echo "${EVE_DBS}" | grep -q "sales" || echo "${EVE_DBS}" | grep -q "finance"; then
  fail "Eve MUST NOT see sales or finance"
fi
run_cli_expect_fail eve --op get_database --db sales
run_cli_expect_fail eve --op get_table --db sales --table orders
run_cli_expect_fail eve --op get_database --db finance

log "=== 10. Group-based policy checks (Charlie in group sales, David in group finance) ==="
CHARLIE_DBS=$(run_cli charlie --groups sales --op get_all_databases)
log "Charlie (group sales) databases: ${CHARLIE_DBS}"
echo "${CHARLIE_DBS}" | grep -q "sales" || fail "Charlie (sales group) expected to see sales"
if echo "${CHARLIE_DBS}" | grep -q "finance"; then
  fail "Charlie (sales group) MUST NOT see finance"
fi
run_cli charlie --groups sales --op get_table --db sales --table orders | grep -q "table=sales.orders" || fail "Charlie get_table(sales, orders) failed"
run_cli_expect_fail charlie --groups sales --op get_database --db finance

DAVID_DBS=$(run_cli david --groups finance --op get_all_databases)
log "David (group finance) databases: ${DAVID_DBS}"
echo "${DAVID_DBS}" | grep -q "finance" || fail "David (finance group) expected to see finance"
if echo "${DAVID_DBS}" | grep -q "sales"; then
  fail "David (finance group) MUST NOT see sales"
fi
run_cli david --groups finance --op get_table --db finance --table reports | grep -q "table=finance.reports" || fail "David get_table(finance, reports) failed"
run_cli_expect_fail david --groups finance --op get_database --db sales

log "=== 11. Admin queries get_all_databases (should see everything) ==="
ADMIN_DBS=$(run_cli admin --op get_all_databases)
log "Admin databases: ${ADMIN_DBS}"
echo "${ADMIN_DBS}" | grep -q "sales" || fail "Admin expected to see sales"
echo "${ADMIN_DBS}" | grep -q "finance" || fail "Admin expected to see finance"

log "=== 12. Check Prometheus metrics for cache and authorization ==="
METRICS=$(docker exec "${PROXY_CONTAINER}" curl -sf http://localhost:9090/metrics || true)
if [ -n "${METRICS}" ]; then
  echo "${METRICS}" | grep -q "hms_proxy_cache_requests_total" || fail "Missing hms_proxy_cache_requests_total in /metrics"
  echo "${METRICS}" | grep -q "hms_proxy_cache_entries" || fail "Missing hms_proxy_cache_entries in /metrics"
  echo "${METRICS}" | grep -q 'hms_proxy_ranger_evaluations_total.*result="allowed"' || fail "Missing allowed evaluations in /metrics"
  echo "${METRICS}" | grep -q 'hms_proxy_ranger_evaluations_total.*result="denied"' || fail "Missing denied evaluations in /metrics"
  echo "${METRICS}" | grep -q "hms_proxy_ranger_filtered_objects_total" || fail "Missing hms_proxy_ranger_filtered_objects_total in /metrics"
  echo "${METRICS}" | grep -q "hms_proxy_ranger_plugin_info" || fail "Missing hms_proxy_ranger_plugin_info in /metrics"
  log "Observed cache metrics: $(echo "${METRICS}" | grep "^hms_proxy_cache_" | head -n 4)"
  log "Observed ranger metrics: $(echo "${METRICS}" | grep "^hms_proxy_ranger_" | head -n 4)"
fi

log "=== 13. Cleanup test metadata ==="
run_cli admin --op drop_table --db sales --table secret_orders || true
run_cli admin --op drop_table --db sales --table orders || true
run_cli admin --op drop_table --db sales --table customers || true
run_cli admin --op drop_database --db sales --cascade true || true

run_cli admin --op drop_table --db finance --table reports || true
run_cli admin --op drop_table --db finance --table expenses || true
run_cli admin --op drop_database --db finance --cascade true || true

log "=== ALL KERBERIZED RANGER & SHARED CACHE SMOKE CHECKS PASSED ==="
