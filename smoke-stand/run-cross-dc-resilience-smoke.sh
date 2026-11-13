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

# The scenario follows the auth mode of the running proxy: on the Kerberos profile
# (KERBEROS_ENABLED=true, .env.kerberos) its front door speaks only SASL, so a simple-auth client
# cannot talk to it. Decided once, before anything restarts the proxy.
REALM=${REALM:-SMOKE.LOCAL}
kerberos_enabled() {
  docker ps --format '{{.Names}}' | grep -q "^${PROXY_CONTAINER}$" \
    && [[ "$(docker exec "${PROXY_CONTAINER}" printenv KERBEROS_ENABLED 2>/dev/null || true)" == "true" ]]
}
KERBEROS=false
if kerberos_enabled; then
  KERBEROS=true
fi

run_cli() {
  local user="$1"
  shift
  if [[ "${KERBEROS}" == "true" ]]; then
    # Each scenario user has its own principal and keytab (kdc/entrypoint.sh). The CLI logs in
    # from the keytab, so the proxy sees <user>@REALM and impersonates it towards the backends.
    docker exec "${PROXY_CONTAINER}" java \
      --add-opens=java.base/java.lang=ALL-UNNAMED \
      --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
      --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
      -cp /opt/hms-proxy/hms-proxy.jar \
      io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli metadata \
      --uri "thrift://localhost:9083" \
      --auth kerberos \
      --client-principal "${user}@${REALM}" \
      --keytab "/keytabs/${user}.keytab" \
      --server-principal "hive/proxy@${REALM}" \
      "$@"
  elif docker ps --format '{{.Names}}' | grep -q "^${PROXY_CONTAINER}$"; then
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

# Fails fast with a readable message when the KDC is down or the keytab is missing; otherwise the
# first CLI call dies deep in a GSS stack trace. A private ticket cache keeps the container's
# default one (used by the Iceberg REST writer) untouched.
if [[ "${KERBEROS}" == "true" ]]; then
  log "Kerberos profile detected, checking the admin@${REALM} keytab"
  docker exec "${PROXY_CONTAINER}" kinit -c FILE:/tmp/krb5cc_cross_dc -kt /keytabs/admin.keytab "admin@${REALM}" \
    || fail "kinit for admin@${REALM} inside ${PROXY_CONTAINER} failed"
fi

# The scenario restarts the proxy on its own config and then puts back whatever the stand ran.
# PROXY_CONFIG and KERBEROS_ENABLED are passed from the shell, which wins over any --env-file, so
# the recreated container keeps the auth mode of the stand. --no-deps matters: without it compose
# starts a stopped hms-apache again through depends_on, and the lenient-startup step would boot
# against a live metastore.
ORIG_PROXY_CONFIG=""
if docker ps --format '{{.Names}}' | grep -q "^${PROXY_CONTAINER}$"; then
  ORIG_PROXY_CONFIG="$(docker exec "${PROXY_CONTAINER}" printenv PROXY_CONFIG 2>/dev/null || true)"
fi
ORIG_PROXY_CONFIG="${ORIG_PROXY_CONFIG:-/opt/hms-proxy/hms-proxy.properties}"
if [[ "${KERBEROS}" == "true" ]]; then
  CROSS_DC_PROXY_CONFIG=/opt/hms-proxy/hms-proxy-cross-dc-kerberos.properties
else
  CROSS_DC_PROXY_CONFIG=/opt/hms-proxy/hms-proxy-cross-dc.properties
fi

recreate_proxy() {
  local config="$1"
  local wait_for_apache="${2:-true}"
  (cd "${STAND_DIR}" && PROXY_CONFIG="${config}" KERBEROS_ENABLED="${KERBEROS}" WAIT_FOR_APACHE="${wait_for_apache}" \
    docker compose up -d --no-deps --force-recreate proxy >/dev/null 2>&1)
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
  if [[ -n "${READYZ_WATCH_PID:-}" ]]; then
    kill "${READYZ_WATCH_PID}" 2>/dev/null || true
  fi
  log "Cleaning up test databases and restoring standard stand configuration..."
  # A run that failed inside step 4b leaves the metastore paused; docker start does not resume it.
  docker unpause "${APACHE_CONTAINER}" >/dev/null 2>&1 || true
  # Ensure secondary metastore is started if it was stopped
  if docker ps -a --format '{{.Names}}' | grep -q "^${APACHE_CONTAINER}$"; then
    docker start "${APACHE_CONTAINER}" >/dev/null 2>&1 || true
    sleep 3
  fi

  run_cli admin --op drop_table --db "${TEST_DB}" --table "${TEST_TBL}" 2>/dev/null || true
  run_cli admin --op drop_table --db "${TEST_DB}" --table "${SHADOW_TBL}" 2>/dev/null || true
  run_cli admin --op drop_table --db "${TEST_DB}" --table "tbl_forbidden_split_brain" 2>/dev/null || true
  run_cli admin --op drop_database --db "${TEST_DB}" --cascade true 2>/dev/null || true

  run_cli admin --op drop_table --db "apache__${TEST_DB}" --table "${TEST_TBL}" 2>/dev/null || true
  run_cli admin --op drop_table --db "apache__${TEST_DB}" --table "${UNIQUE_TBL}" 2>/dev/null || true
  run_cli admin --op drop_database --db "apache__${TEST_DB}" --cascade true 2>/dev/null || true

  if docker ps --format '{{.Names}}' | grep -q "^${PROXY_CONTAINER}$"; then
    log "Restoring proxy with ${ORIG_PROXY_CONFIG}..."
    recreate_proxy "${ORIG_PROXY_CONFIG}" || true
  fi
}
trap cleanup EXIT

log "=== 1. Starting proxy with cross-DC resilience configuration ==="
if docker ps --format '{{.Names}}' | grep -q "^${PROXY_CONTAINER}$"; then
  recreate_proxy "${CROSS_DC_PROXY_CONFIG}" || fail "could not recreate ${PROXY_CONTAINER} with ${CROSS_DC_PROXY_CONFIG}"
  wait_for_proxy_ready 40
  [[ "$(docker exec "${PROXY_CONTAINER}" printenv PROXY_CONFIG)" == "${CROSS_DC_PROXY_CONFIG}" ]] \
    || fail "${PROXY_CONTAINER} did not pick up ${CROSS_DC_PROXY_CONFIG}"
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

log "=== 4b. Testing /readyz with a Hung (Not Down) Remote Catalog ==="
# docker stop makes the apache probe fail fast once the JVM's 30 s DNS cache for hms-apache has
# expired (UnknownHostException), so it never exercises a probe that hangs. docker pause does: the
# name resolves and the kernel accepts the connection, but the metastore never answers, like a
# remote DC that is unreachable rather than gone. A hung apache probe must not fail the hdp one,
# and with apache not required for readiness /readyz must stay 200 the whole time.
#
# Measured on the Kerberos stand with the pre-fix /readyz prober: the hdp probe was failed only when
# the proxy had seen no backend traffic for ~35 s before the pause (503 in every sample); right after
# traffic the same pause left /readyz at 200. The likely cause is the JVM's 30 s DNS cache pushing the
# apache probe past its deadline, which equals its own socket timeout - inferred, not measured. The
# idle wait below reproduces the measured precondition so that a regression turns this step red.
HUNG_IDLE_SECONDS=${HUNG_IDLE_SECONDS:-35}
HUNG_WATCH_SECONDS=${HUNG_WATCH_SECONDS:-20}
log "Leaving the proxy without backend traffic for ${HUNG_IDLE_SECONDS}s before the pause..."
sleep "${HUNG_IDLE_SECONDS}"
docker pause "${APACHE_CONTAINER}" >/dev/null
HUNG_SAMPLES=0
HUNG_SEEN_DOWN=false
HUNG_BAD=""
HUNG_DEADLINE=$(( SECONDS + HUNG_WATCH_SECONDS ))
while (( SECONDS < HUNG_DEADLINE )); do
  sample=$(get_readyz_code_and_body)
  code=$(echo "${sample}" | tail -n 1)
  body=$(echo "${sample}" | sed '$d')
  HUNG_SAMPLES=$(( HUNG_SAMPLES + 1 ))
  if [[ "${code}" != "200" && -z "${HUNG_BAD}" ]]; then
    HUNG_BAD="${code} $(echo "${body}" | tr -d '\n' | cut -c1-400)"
  fi
  if echo "${body}" | grep -q '"backend":"apache","requiredForReadiness":false,"connected":false'; then
    HUNG_SEEN_DOWN=true
  fi
  sleep 1
done
docker unpause "${APACHE_CONTAINER}" >/dev/null
if [[ -n "${HUNG_BAD}" ]]; then
  fail "/readyz left 200 while the non-required apache catalog was hung; first bad sample: ${HUNG_BAD}"
fi
# Without this the step would also pass if the probes never noticed the hang at all.
[[ "${HUNG_SEEN_DOWN}" == "true" ]] \
  || fail "apache never showed connected=false in /readyz during the ${HUNG_WATCH_SECONDS}s pause (${HUNG_SAMPLES} samples)"
(( HUNG_SAMPLES >= 3 )) || fail "/readyz watch collected only ${HUNG_SAMPLES} samples during the pause"
log "/readyz stayed 200 in all ${HUNG_SAMPLES} samples while apache was hung, and reported apache down"
log "Waiting for apache to answer probes again..."
for _ in $(seq 1 60); do
  if get_readyz_code_and_body | sed '$d' | grep -q '"backend":"apache","requiredForReadiness":false,"connected":true'; then
    break
  fi
  sleep 1
done
get_readyz_code_and_body | sed '$d' | grep -q '"backend":"apache","requiredForReadiness":false,"connected":true' \
  || fail "apache did not come back in /readyz after docker unpause"

log "=== 5. Testing Serve-Stale-on-Error on Remote Outage ==="
# Prime cache for remote unique table
run_cli admin --op get_table --db "apache__${TEST_DB}" --table "${UNIQUE_TBL}" >/dev/null

# Wait for 3.5s so cache entry expires (TTL is 3s in hms-proxy-cross-dc.properties)
log "Waiting 4s for cache TTL to expire..."
sleep 4

log "Simulating remote datacenter outage: stopping ${APACHE_CONTAINER}..."
docker stop "${APACHE_CONTAINER}" >/dev/null

# Readiness is sampled every second from the moment of the outage, not once in step 6. This covers a
# backend that is gone (connection refused, then an unresolvable name); a hung backend is step 4b.
READYZ_WATCH_SECONDS=${READYZ_WATCH_SECONDS:-20}
READYZ_WATCH_FILE="$(mktemp)"
(
  for _ in $(seq 1 "${READYZ_WATCH_SECONDS}"); do
    sample=$(get_readyz_code_and_body)
    code=$(echo "${sample}" | tail -n 1)
    if [[ "${code}" == "200" ]]; then
      echo "200" >> "${READYZ_WATCH_FILE}"
    else
      echo "${code} $(echo "${sample}" | sed '$d' | tr -d '\n' | cut -c1-400)" >> "${READYZ_WATCH_FILE}"
    fi
    sleep 1
  done
) &
READYZ_WATCH_PID=$!

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
log "Waiting for the ${READYZ_WATCH_SECONDS}s /readyz watch that started with the outage..."
wait "${READYZ_WATCH_PID}" || true
READYZ_WATCH_PID=""
READYZ_SAMPLES=$(grep -c . "${READYZ_WATCH_FILE}" || true)
READYZ_BAD=$(grep -v '^200$' "${READYZ_WATCH_FILE}" || true)
rm -f "${READYZ_WATCH_FILE}"
if (( READYZ_SAMPLES < READYZ_WATCH_SECONDS / 2 )); then
  fail "/readyz watch collected only ${READYZ_SAMPLES} samples in ${READYZ_WATCH_SECONDS}s"
fi
if [[ -n "${READYZ_BAD}" ]]; then
  fail "/readyz left 200 while only the non-required apache catalog was down ($(echo "${READYZ_BAD}" | grep -c .) of ${READYZ_SAMPLES} samples); first: $(echo "${READYZ_BAD}" | head -1)"
fi
log "/readyz stayed 200 in all ${READYZ_SAMPLES} samples since the outage began"
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

# Split-brain protection: a mutating RPC on the failed catalog must not fall back to the replica.
# A bare "the call failed" proves nothing here: with the protection gone the write still fails on
# the stopped metastore. So the failure must come from the apache route, the fallback counter for
# create_table must not move, and the table must not exist on the replica.
log "Verifying split-brain protection (mutating RPC must be denied on outage)..."
SPLIT_BRAIN_TBL="tbl_forbidden_split_brain"
CREATE_FALLBACK_1=$(get_metric_sum 'hms_proxy_catalog_fallback_total\{.*method="create_table.*\}')
# The wording depends on where the call died: "Backend catalog 'apache' failed in method ..." from
# the proxy's error normalizer (a failed connect included, with the client's "Could not connect to
# meta store ... hms-apache" kept inside), or "Unable to open backend metastore client for catalog
# apache ..." when an impersonation session (Kerberos) cannot be opened. Both name the apache route;
# a MetaException raised after the CLI connected rules out a refused connection or a login failure.
SPLIT_BRAIN_OUT=$(run_cli admin --op create_table --db "apache__${TEST_DB}" --table "${SPLIT_BRAIN_TBL}" \
  --expect-error MetaException) \
  || fail "create_table on the failed apache catalog did not fail with a MetaException"
echo "${SPLIT_BRAIN_OUT}" | grep -Eq "failed as expected with: MetaException: .*catalog '?apache'?" \
  || fail "create_table failed, but not on the apache route: ${SPLIT_BRAIN_OUT}"
CREATE_FALLBACK_2=$(get_metric_sum 'hms_proxy_catalog_fallback_total\{.*method="create_table.*\}')
if (( CREATE_FALLBACK_2 != CREATE_FALLBACK_1 )); then
  fail "create_table was routed to the shadow catalog (fallback counter ${CREATE_FALLBACK_1} -> ${CREATE_FALLBACK_2})"
fi
run_cli admin --op get_table --db "${TEST_DB}" --table "${SPLIT_BRAIN_TBL}" \
  --expect-error NoSuchObjectException \
  || fail "${SPLIT_BRAIN_TBL} exists on the hdp replica: the write leaked through the fallback"
log "Split-brain protection verified: create_table failed on the apache route and left nothing on the replica"

log "=== 8. Testing Lenient Startup Mode ==="
# Restart proxy while stand-hms-apache is STILL STOPPED
log "Restarting proxy while apache metastore is stopped (lenient startup test)..."
recreate_proxy "${CROSS_DC_PROXY_CONFIG}" false || fail "could not recreate ${PROXY_CONTAINER} with ${CROSS_DC_PROXY_CONFIG}"
wait_for_proxy_ready 40
# The point of the step: the proxy came up while apache was down, not after compose revived it.
if docker ps --format '{{.Names}}' | grep -q "^${APACHE_CONTAINER}$"; then
  fail "${APACHE_CONTAINER} is running again after the proxy restart; lenient startup was not exercised"
fi

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
