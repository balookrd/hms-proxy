#!/usr/bin/env bash
# Soft remote catalog merging (merge-with-default) and configuration reload smoke test runner.
# Verifies that:
# 1. catalog.<name>.merge-with-default=true dynamically merges unique remote databases without prefix.
# 2. Canonical access with explicit catalog prefix (<catalog>__<db>) continues to work.
# 3. No shadowing occurs: colliding database in default catalog is served unprefixed, while
#    the remote database remains accessible via its explicit catalog prefix.
# 4. Creating a database without explicit prefix always routes to the default catalog.
# 5. Dynamic configuration reload works via SIGHUP.
# 6. Safe Reload protects running proxy if invalid configuration is supplied.
set -euo pipefail

STAND_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${STAND_DIR}/.." && pwd)"

log() {
  printf '[merged-databases-smoke] %s\n' "$*"
}

fail() {
  printf '[merged-databases-smoke] ERROR: %s\n' "$*" >&2
  exit 1
}

PROXY_HOST=${PROXY_HOST:-127.0.0.1}
PROXY_PORT=${PROXY_PORT:-19085}
MGMT_PORT=${MGMT_PORT:-19090}
PROXY_CONTAINER=stand-proxy

UNIQUE_DB="smoke_merged_unique_db"
APACHE_UNIQUE_TABLE="apache_unique_tbl"

COLLISION_DB="smoke_collision_db"
APACHE_COLLISION_TABLE="apache_col_tbl"
HDP_COLLISION_TABLE="hdp_col_tbl"

NEW_DB="smoke_new_unprefixed_db"
NEW_TABLE="smoke_new_tbl"

# The reload edits the file the running proxy actually reads: on the Kerberos profile that is
# hms-proxy-kerberos.properties, and editing hms-proxy.properties there would reload nothing.
RUNNING_PROXY_CONFIG=""
if docker ps --format '{{.Names}}' | grep -q "^${PROXY_CONTAINER}$"; then
  RUNNING_PROXY_CONFIG="$(docker exec "${PROXY_CONTAINER}" printenv PROXY_CONFIG 2>/dev/null || true)"
fi
CONFIG_FILE="${STAND_DIR}/proxy/$(basename "${RUNNING_PROXY_CONFIG:-/opt/hms-proxy/hms-proxy.properties}")"
BACKUP_FILE="${CONFIG_FILE}.bak"
[[ -f "${CONFIG_FILE}" ]] || { printf '[merged-databases-smoke] ERROR: proxy config %s not found\n' "${CONFIG_FILE}" >&2; exit 1; }

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
  docker exec "${PROXY_CONTAINER}" kinit -c FILE:/tmp/krb5cc_merged_databases -kt /keytabs/admin.keytab "admin@${REALM}" \
    || fail "kinit for admin@${REALM} inside ${PROXY_CONTAINER} failed"
fi

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

  # Cleanup unique db
  run_cli admin --op drop_table --db "apache__${UNIQUE_DB}" --table "${APACHE_UNIQUE_TABLE}" 2>/dev/null || true
  run_cli admin --op drop_table --db "${UNIQUE_DB}" --table "${APACHE_UNIQUE_TABLE}" 2>/dev/null || true
  run_cli admin --op drop_database --db "apache__${UNIQUE_DB}" --cascade true 2>/dev/null || true
  run_cli admin --op drop_database --db "${UNIQUE_DB}" --cascade true 2>/dev/null || true

  # Cleanup collision db
  run_cli admin --op drop_table --db "apache__${COLLISION_DB}" --table "${APACHE_COLLISION_TABLE}" 2>/dev/null || true
  run_cli admin --op drop_table --db "${COLLISION_DB}" --table "${HDP_COLLISION_TABLE}" 2>/dev/null || true
  run_cli admin --op drop_database --db "apache__${COLLISION_DB}" --cascade true 2>/dev/null || true
  run_cli admin --op drop_database --db "${COLLISION_DB}" --cascade true 2>/dev/null || true

  # Cleanup created db
  run_cli admin --op drop_table --db "${NEW_DB}" --table "${NEW_TABLE}" 2>/dev/null || true
  run_cli admin --op drop_database --db "${NEW_DB}" --cascade true 2>/dev/null || true
  run_cli admin --op drop_database --db "apache__${NEW_DB}" --cascade true 2>/dev/null || true
}
trap cleanup EXIT

# On the Kerberos profile a namenode container holds no ticket, and dfsadmin is refused with
# "Client cannot authenticate via:[TOKEN, KERBEROS]"; that refusal used to be swallowed, so the
# wait silently did nothing. Log in by the namenode's own keytab and let a refusal fail the run.
wait_for_safemode() {
  local container host
  for container in stand-namenode stand-namenode-b; do
    docker ps --format '{{.Names}}' | grep -q "^${container}$" || continue
    host="${container#stand-}"
    if [[ "${KERBEROS}" == "true" ]]; then
      docker exec "${container}" kinit -kt "/keytabs/${host}.keytab" "hdfs/${host}@${REALM}" \
        || fail "kinit for hdfs/${host} inside ${container} failed"
    fi
    docker exec "${container}" hdfs dfsadmin -safemode wait >/dev/null \
      || fail "hdfs dfsadmin -safemode wait failed inside ${container}"
  done
}

log "=== 0. Verify proxy connectivity and HDFS readiness ==="
wait_for_safemode
check_ready || fail "Proxy readiness check failed at http://${PROXY_HOST}:${MGMT_PORT}/readyz"

# Backup configuration
cp "${CONFIG_FILE}" "${BACKUP_FILE}"

log "=== 1. Initial setup: create test databases in remote and default catalogs ==="
run_cli admin --op drop_database --db "apache__${UNIQUE_DB}" --cascade true 2>/dev/null || true
run_cli admin --op drop_database --db "${UNIQUE_DB}" --cascade true 2>/dev/null || true
run_cli admin --op drop_database --db "apache__${COLLISION_DB}" --cascade true 2>/dev/null || true
run_cli admin --op drop_database --db "${COLLISION_DB}" --cascade true 2>/dev/null || true

log "Creating unique database in remote catalog (apache)..."
run_cli admin --op create_database --db "apache__${UNIQUE_DB}"
run_cli admin --op create_table --db "apache__${UNIQUE_DB}" --table "${APACHE_UNIQUE_TABLE}"

log "Creating colliding database in remote catalog (apache)..."
run_cli admin --op create_database --db "apache__${COLLISION_DB}"
run_cli admin --op create_table --db "apache__${COLLISION_DB}" --table "${APACHE_COLLISION_TABLE}"

log "Creating colliding database in default catalog (hdp)..."
run_cli admin --op create_database --db "${COLLISION_DB}"
run_cli admin --op create_table --db "${COLLISION_DB}" --table "${HDP_COLLISION_TABLE}"

db_in_list() {
  local list="$1"
  local target="$2"
  echo "${list}" | tr '[], ' '\n\n\n\n' | grep -Fxq "${target}"
}

log "Verifying pre-reload state: remote unique db is ONLY visible with prefix"
PRE_DBS=$(run_cli admin --op get_all_databases)
db_in_list "${PRE_DBS}" "apache__${UNIQUE_DB}" || fail "Expected 'apache__${UNIQUE_DB}' in pre-reload get_all_databases"
if db_in_list "${PRE_DBS}" "${UNIQUE_DB}"; then
  fail "'${UNIQUE_DB}' should NOT be visible without prefix before merge-with-default is active"
fi

db_in_list "${PRE_DBS}" "${COLLISION_DB}" || fail "Expected '${COLLISION_DB}' in pre-reload get_all_databases"
db_in_list "${PRE_DBS}" "apache__${COLLISION_DB}" || fail "Expected 'apache__${COLLISION_DB}' in pre-reload get_all_databases"

log "=== 2. Apply merge-with-default configuration dynamically via SIGHUP ==="
printf '\n# Dynamic merge-with-default test\ncatalog.apache.merge-with-default=true\n' >> "${CONFIG_FILE}"
send_reload_signal
sleep 2

log "=== 3. Verify merged databases exposure and routing ==="
POST_DBS=$(run_cli admin --op get_all_databases)

# Unique DB: exposed unprefixed in listing, and canonical prefixed access via direct RPC remains
db_in_list "${POST_DBS}" "${UNIQUE_DB}" || fail "'${UNIQUE_DB}' missing from get_all_databases after merge-with-default"
run_cli admin --op get_database --db "apache__${UNIQUE_DB}" >/dev/null || fail "Canonical access to 'apache__${UNIQUE_DB}' via get_database failed"

# Colliding DB: default catalog is unprefixed, remote database is PRESERVED with prefix (no shadowing)
db_in_list "${POST_DBS}" "${COLLISION_DB}" || fail "'${COLLISION_DB}' missing from get_all_databases"
db_in_list "${POST_DBS}" "apache__${COLLISION_DB}" || fail "Remote collision database 'apache__${COLLISION_DB}' must NOT be shadowed and must remain visible"

log "Verifying table routing for unique database..."
POST_UNIQUE_TABLES=$(run_cli admin --op get_all_tables --db "${UNIQUE_DB}")
echo "${POST_UNIQUE_TABLES}" | grep -q "${APACHE_UNIQUE_TABLE}" || fail "'${UNIQUE_DB}' did not route to remote catalog table '${APACHE_UNIQUE_TABLE}'"

POST_CANONICAL_TABLES=$(run_cli admin --op get_all_tables --db "apache__${UNIQUE_DB}")
echo "${POST_CANONICAL_TABLES}" | grep -q "${APACHE_UNIQUE_TABLE}" || fail "'apache__${UNIQUE_DB}' did not return '${APACHE_UNIQUE_TABLE}'"

log "Verifying table routing for collision database..."
POST_COLLISION_HDP_TABLES=$(run_cli admin --op get_all_tables --db "${COLLISION_DB}")
echo "${POST_COLLISION_HDP_TABLES}" | grep -q "${HDP_COLLISION_TABLE}" || fail "'${COLLISION_DB}' did not return default catalog table '${HDP_COLLISION_TABLE}'"
if echo "${POST_COLLISION_HDP_TABLES}" | grep -q "${APACHE_COLLISION_TABLE}"; then
  fail "'${COLLISION_DB}' incorrectly returned remote catalog table '${APACHE_COLLISION_TABLE}'"
fi

POST_COLLISION_APACHE_TABLES=$(run_cli admin --op get_all_tables --db "apache__${COLLISION_DB}")
echo "${POST_COLLISION_APACHE_TABLES}" | grep -q "${APACHE_COLLISION_TABLE}" || fail "'apache__${COLLISION_DB}' did not return remote catalog table '${APACHE_COLLISION_TABLE}'"
if echo "${POST_COLLISION_APACHE_TABLES}" | grep -q "${HDP_COLLISION_TABLE}"; then
  fail "'apache__${COLLISION_DB}' incorrectly returned default catalog table '${HDP_COLLISION_TABLE}'"
fi

log "=== 4. Verify create_database routing: new unprefixed database routes to default catalog ==="
run_cli admin --op drop_database --db "${NEW_DB}" --cascade true 2>/dev/null || true
run_cli admin --op create_database --db "${NEW_DB}"
run_cli admin --op create_table --db "${NEW_DB}" --table "${NEW_TABLE}"

# Table must be in default catalog (hdp), and not in remote catalog (apache)
CREATED_TABLES=$(run_cli admin --op get_all_tables --db "${NEW_DB}")
echo "${CREATED_TABLES}" | grep -q "${NEW_TABLE}" || fail "Created table '${NEW_TABLE}' not found in '${NEW_DB}'"

# Direct query to default catalog prefix hdp__ must show it
HDP_DIRECT=$(run_cli admin --op get_all_tables --db "hdp__${NEW_DB}")
echo "${HDP_DIRECT}" | grep -q "${NEW_TABLE}" || fail "Expected table in default catalog 'hdp__${NEW_DB}'"

# Remote catalog apache__ must NOT contain it
run_cli admin --op get_database --db "apache__${NEW_DB}" \
  --expect-error NoSuchObjectException \
  || fail "Database '${NEW_DB}' was erroneously created in remote catalog 'apache' instead of default catalog"

log "Cleaning up newly created test database..."
run_cli admin --op drop_table --db "${NEW_DB}" --table "${NEW_TABLE}"
run_cli admin --op drop_database --db "${NEW_DB}" --cascade true

log "=== 5. Verify Safe Reload: invalid configuration does not break running proxy ==="
log "Injecting invalid configuration (merge-with-default on default catalog)..."
printf 'catalog.hdp.merge-with-default=true\n' >> "${CONFIG_FILE}"
send_reload_signal
sleep 2

check_ready || fail "Proxy went down after reloading invalid configuration! Safe Reload failed."

log "Verifying proxy continues serving on valid configuration..."
SAFE_TABLES=$(run_cli admin --op get_all_tables --db "${UNIQUE_DB}")
echo "${SAFE_TABLES}" | grep -q "${APACHE_UNIQUE_TABLE}" || fail "Proxy failed to serve requests after rejected reload"

log "=== 6. Restore configuration and verify revert ==="
cp "${BACKUP_FILE}" "${CONFIG_FILE}"
rm -f "${BACKUP_FILE}"
send_reload_signal
sleep 2

RESTORED_DBS=$(run_cli admin --op get_all_databases)
db_in_list "${RESTORED_DBS}" "apache__${UNIQUE_DB}" || fail "Expected 'apache__${UNIQUE_DB}' after config revert"
if db_in_list "${RESTORED_DBS}" "${UNIQUE_DB}"; then
  fail "'${UNIQUE_DB}' should NOT be visible without prefix after merge-with-default is turned off"
fi

log "=== ALL MERGED DATABASES SMOKE CHECKS PASSED ==="
