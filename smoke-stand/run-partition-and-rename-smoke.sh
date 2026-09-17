#!/usr/bin/env bash
# Table rename, partition exchange, partition rename, and partition location rewriting
# smoke test runner on the Docker stand.
# Verifies recent federation routing and mutation fixes across HDP and Apache catalogs:
# 1. Table rename within catalog and negative cross-catalog rename denial.
# 2. Exchange partition within catalog and negative cross-catalog exchange denial.
# 3. Partition rename in federated (non-default) catalog.
# 4. Partition location normalization/rewriting for external tables.
set -euo pipefail

STAND_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${STAND_DIR}/.." && pwd)"

log() {
  printf '[partition-rename-smoke] %s\n' "$*"
}

fail() {
  printf '[partition-rename-smoke] ERROR: %s\n' "$*" >&2
  exit 1
}

PROXY_HOST=${PROXY_HOST:-127.0.0.1}
PROXY_PORT=${PROXY_PORT:-19085}
PROXY_CONTAINER=stand-proxy
HDP_FS_PREFIX=${HDP_FS_PREFIX:-stand-namenode}
APACHE_FS_PREFIX=${APACHE_FS_PREFIX:-stand-namenode-b}

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

RUN_ID="$(date +%Y%m%d%H%M%S)"
TBL_REN_SRC="smoke_ren_src_${RUN_ID}"
TBL_REN_DST="smoke_ren_dst_${RUN_ID}"
TBL_REN_AP_SRC="smoke_ren_ap_src_${RUN_ID}"
TBL_REN_AP_DST="smoke_ren_ap_dst_${RUN_ID}"

TBL_EXCH_SRC="smoke_exch_src_${RUN_ID}"
TBL_EXCH_DST="smoke_exch_dst_${RUN_ID}"
TBL_EXCH_AP="smoke_exch_ap_${RUN_ID}"

TBL_PART_REN="smoke_part_ren_${RUN_ID}"
TBL_EXT_PART_HDP="smoke_ext_part_hdp_${RUN_ID}"
TBL_EXT_PART_AP="smoke_ext_part_ap_${RUN_ID}"

cleanup() {
  log "Cleaning up temporary test tables..."
  run_cli admin --op drop_table --db "hdp__default" --table "${TBL_REN_SRC}" 2>/dev/null || true
  run_cli admin --op drop_table --db "hdp__default" --table "${TBL_REN_DST}" 2>/dev/null || true
  run_cli admin --op drop_table --db "apache__default" --table "${TBL_REN_AP_SRC}" 2>/dev/null || true
  run_cli admin --op drop_table --db "apache__default" --table "${TBL_REN_AP_DST}" 2>/dev/null || true
  run_cli admin --op drop_table --db "hdp__default" --table "${TBL_EXCH_SRC}" 2>/dev/null || true
  run_cli admin --op drop_table --db "hdp__default" --table "${TBL_EXCH_DST}" 2>/dev/null || true
  run_cli admin --op drop_table --db "apache__default" --table "${TBL_EXCH_AP}" 2>/dev/null || true
  run_cli admin --op drop_table --db "apache__default" --table "${TBL_PART_REN}" 2>/dev/null || true
  run_cli admin --op drop_table --db "hdp__default" --table "${TBL_EXT_PART_HDP}" 2>/dev/null || true
  run_cli admin --op drop_table --db "apache__default" --table "${TBL_EXT_PART_AP}" 2>/dev/null || true
}
trap cleanup EXIT

log "=== 1. Table Rename within Catalog and Negative Cross-Catalog Denial ==="
log "Creating table in default catalog: hdp__default.${TBL_REN_SRC}"
run_cli admin --op create_table --db "hdp__default" --table "${TBL_REN_SRC}"

log "Renaming within default catalog: ${TBL_REN_SRC} -> ${TBL_REN_DST}"
run_cli admin --op rename_table --db "hdp__default" --table "${TBL_REN_SRC}" --new-table "${TBL_REN_DST}"

log "Verifying new table exists and old table is gone"
run_cli admin --op get_table --db "hdp__default" --table "${TBL_REN_DST}" >/dev/null
if run_cli admin --op get_table --db "hdp__default" --table "${TBL_REN_SRC}" 2>/dev/null; then
  fail "Old table ${TBL_REN_SRC} still exists after rename"
fi

log "Negative test: cross-catalog table rename from hdp to apache must be rejected"
run_cli admin --op rename_table \
  --db "hdp__default" --table "${TBL_REN_DST}" \
  --new-db "apache__default" --new-table "${TBL_REN_DST}" \
  --expect-error "Cross-catalog table rename is not supported"

log "Renaming within federated catalog: apache__default.${TBL_REN_AP_SRC} -> ${TBL_REN_AP_DST}"
run_cli admin --op create_table --db "apache__default" --table "${TBL_REN_AP_SRC}"
run_cli admin --op rename_table --db "apache__default" --table "${TBL_REN_AP_SRC}" --new-table "${TBL_REN_AP_DST}"
run_cli admin --op get_table --db "apache__default" --table "${TBL_REN_AP_DST}" >/dev/null
if run_cli admin --op get_table --db "apache__default" --table "${TBL_REN_AP_SRC}" 2>/dev/null; then
  fail "Old table ${TBL_REN_AP_SRC} still exists in apache catalog after rename"
fi

log "=== 2. Exchange Partition within Catalog and Negative Cross-Catalog Denial ==="
log "Creating source and destination partitioned tables in hdp__default"
run_cli admin --op create_table --db "hdp__default" --table "${TBL_EXCH_SRC}" --partition-keys "p:string"
run_cli admin --op create_table --db "hdp__default" --table "${TBL_EXCH_DST}" --partition-keys "p:string"

log "Adding partition p=100 to source table"
run_cli admin --op add_partition --db "hdp__default" --table "${TBL_EXCH_SRC}" --part-vals "100"

log "Exchanging partition p=100 from ${TBL_EXCH_SRC} to ${TBL_EXCH_DST}"
run_cli admin --op exchange_partition \
  --source-db "hdp__default" --source-table "${TBL_EXCH_SRC}" \
  --dest-db "hdp__default" --dest-table "${TBL_EXCH_DST}" \
  --partition-specs "p=100"

log "Verifying partition moved to destination table"
run_cli admin --op get_partition --db "hdp__default" --table "${TBL_EXCH_DST}" --part-vals "100" >/dev/null
if run_cli admin --op get_partition --db "hdp__default" --table "${TBL_EXCH_SRC}" --part-vals "100" 2>/dev/null; then
  fail "Partition p=100 still exists on source table ${TBL_EXCH_SRC} after exchange"
fi

log "Creating partitioned table in remote catalog: apache__default.${TBL_EXCH_AP}"
run_cli admin --op create_table --db "apache__default" --table "${TBL_EXCH_AP}" --partition-keys "p:string"

log "Negative test: cross-catalog exchange partition (hdp <-> apache) must be rejected"
run_cli admin --op exchange_partition \
  --source-db "hdp__default" --source-table "${TBL_EXCH_DST}" \
  --dest-db "apache__default" --dest-table "${TBL_EXCH_AP}" \
  --partition-specs "p=100" \
  --expect-error "Cross-catalog exchange partition is not supported"

log "=== 3. Partition Rename in Federated (Non-Default) Catalog ==="
log "Creating partitioned table in apache__default: ${TBL_PART_REN}"
run_cli admin --op create_table --db "apache__default" --table "${TBL_PART_REN}" --partition-keys "p:string"

log "Adding partition p=2026-01-01"
run_cli admin --op add_partition --db "apache__default" --table "${TBL_PART_REN}" --part-vals "2026-01-01"

log "Renaming partition: 2026-01-01 -> 2026-01-02"
run_cli admin --op rename_partition \
  --db "apache__default" --table "${TBL_PART_REN}" \
  --part-vals "2026-01-01" --new-part-vals "2026-01-02"

log "Verifying renamed partition in federated catalog"
RENAMED_PART_OUT=$(run_cli admin --op get_partition --db "apache__default" --table "${TBL_PART_REN}" --part-vals "2026-01-02")
log "get_partition output: ${RENAMED_PART_OUT}"
echo "${RENAMED_PART_OUT}" | grep -q "db=apache__default" || fail "Expected db=apache__default in partition output"
echo "${RENAMED_PART_OUT}" | grep -q "2026-01-02" || fail "Expected value 2026-01-02 in partition output"

if run_cli admin --op get_partition --db "apache__default" --table "${TBL_PART_REN}" --part-vals "2026-01-01" 2>/dev/null; then
  fail "Old partition 2026-01-01 still exists after rename"
fi

log "=== 4. Partition Location Normalization for External Tables ==="
log "Creating external partitioned table in hdp__default: ${TBL_EXT_PART_HDP}"
run_cli admin --op create_table \
  --db "hdp__default" --table "${TBL_EXT_PART_HDP}" \
  --table-type "EXTERNAL_TABLE" \
  --partition-keys "p:string" \
  --location "/external/hdp/tables/${TBL_EXT_PART_HDP}"

log "Adding partition with unqualified path to hdp external table"
HDP_PART_OUT=$(run_cli admin --op add_partition \
  --db "hdp__default" --table "${TBL_EXT_PART_HDP}" \
  --part-vals "p1" \
  --location "/external/hdp/partitions/${TBL_EXT_PART_HDP}/p1")
log "Added partition output: ${HDP_PART_OUT}"
echo "${HDP_PART_OUT}" | grep -q "${HDP_FS_PREFIX}" || fail "HDP partition location was not rewritten with HDP filesystem host (${HDP_FS_PREFIX})"

log "Altering partition location with unqualified path"
HDP_ALTER_OUT=$(run_cli admin --op alter_partition \
  --db "hdp__default" --table "${TBL_EXT_PART_HDP}" \
  --part-vals "p1" \
  --location "/external/hdp/partitions/${TBL_EXT_PART_HDP}/p1_moved")
log "Altered partition output: ${HDP_ALTER_OUT}"
echo "${HDP_ALTER_OUT}" | grep -q "${HDP_FS_PREFIX}" || fail "Altered HDP partition location was not rewritten with HDP filesystem host (${HDP_FS_PREFIX})"
echo "${HDP_ALTER_OUT}" | grep -q "p1_moved" || fail "Altered HDP partition location does not contain p1_moved"

log "Creating external partitioned table in apache__default: ${TBL_EXT_PART_AP}"
run_cli admin --op create_table \
  --db "apache__default" --table "${TBL_EXT_PART_AP}" \
  --table-type "EXTERNAL_TABLE" \
  --partition-keys "p:string" \
  --location "/external/apache/tables/${TBL_EXT_PART_AP}"

log "Adding partition with unqualified path to apache external table"
APACHE_PART_OUT=$(run_cli admin --op add_partition \
  --db "apache__default" --table "${TBL_EXT_PART_AP}" \
  --part-vals "p1" \
  --location "/external/apache/partitions/${TBL_EXT_PART_AP}/p1")
log "Added partition output: ${APACHE_PART_OUT}"
echo "${APACHE_PART_OUT}" | grep -q "${APACHE_FS_PREFIX}" || fail "Apache partition location was not rewritten with Apache filesystem host (${APACHE_FS_PREFIX})"

log "=== ALL PARTITION AND RENAME SMOKE CHECKS PASSED ==="
