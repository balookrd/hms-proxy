#!/usr/bin/env bash
# Iceberg v2 row-level smoke: DELETE and UPDATE, and who can still read the result.
#
# The interop scenario next door only ever appends, so nothing there produces a delete file.
# This one does: Hive 4 - the only engine on the stand with native row-level DML over Iceberg -
# deletes and updates rows in a table the REST front door created, and every other front door
# then has to read what it left behind. The REST client verifies the effect (fewer rows after
# the delete, a changed value after the update) and reports the table's file shape, which is
# what tells merge-on-read from copy-on-write as a fact rather than as a setting.
#
# What is being tested is a Hive/Iceberg capability boundary, not a proxy decision: the proxy
# relays the same alter_table either way. The point is to have the boundary written down with
# the exact error, the way section H already records the Hive 4 `inputFormat` asymmetry.
#
# Both `write.delete.mode`/`write.update.mode` values are exercised by default; --mode picks one.
#
# Stand-local on purpose: every step is a docker exec into the container that owns the engine.
# Needs the same compose profiles as the interop scenario (the backend's own, plus hdp and
# hive4fe for the SQL clients), e.g.:
#   docker compose --env-file .env.hive4 --profile hive4 --profile hive4fe --profile hdp up -d --build
#
#   smoke-stand/run-iceberg-rowlevel-smoke.sh --prefix hive4
#   smoke-stand/run-iceberg-rowlevel-smoke.sh --prefix hive4 --kerberos
#   smoke-stand/run-iceberg-rowlevel-smoke.sh --prefix hive4 --mode merge-on-read
set -euo pipefail

AUTH=plain
PREFIX=${ROWLEVEL_PREFIX:-hive4}
NAMENODE=${ROWLEVEL_NAMENODE:-}
MODE=${ROWLEVEL_MODE:-both}
while [[ $# -gt 0 ]]; do
  case "$1" in
    --kerberos) AUTH=kerberos; shift ;;
    --prefix) [[ $# -ge 2 ]] || { echo "missing value for --prefix" >&2; exit 1; }; PREFIX="$2"; shift 2 ;;
    --namenode) [[ $# -ge 2 ]] || { echo "missing value for --namenode" >&2; exit 1; }; NAMENODE="$2"; shift 2 ;;
    --mode) [[ $# -ge 2 ]] || { echo "missing value for --mode" >&2; exit 1; }; MODE="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 1 ;;
  esac
done

case "${MODE}" in
  merge-on-read) MODES=(merge-on-read) ;;
  copy-on-write) MODES=(copy-on-write) ;;
  both) MODES=(merge-on-read copy-on-write) ;;
  *) echo "unknown --mode '${MODE}'; expected merge-on-read, copy-on-write or both" >&2; exit 1 ;;
esac

# The apache catalog is the only one on the second HDFS cluster; everything else lives on the
# first. Only used to verify and clean up the table directory after the purge-drop.
if [[ -z "${NAMENODE}" ]]; then
  NAMENODE=$([[ "${PREFIX}" == "apache" ]] && echo stand-namenode-b || echo stand-namenode)
fi

NS=${ROWLEVEL_NAMESPACE:-default}
TABLE=${ROWLEVEL_TABLE:-smoke_iceberg_rowlevel}
REST_NET_URL=http://proxy:9183
WRITER_JAR=/opt/hms-proxy/iceberg-rest-writer.jar

fail() {
  printf '[iceberg-rowlevel] error: %s\n' "$*" >&2
  exit 1
}

log() {
  printf '[iceberg-rowlevel] %s\n' "$*"
}

# --- engine wrappers -------------------------------------------------------------------------

writer() {
  local -a auth_args=()
  if [[ "${AUTH}" == "kerberos" ]]; then
    auth_args+=(--keytab /keytabs/smoke-user.keytab --principal smoke-user@SMOKE.LOCAL)
    auth_args+=(--rest-auth spnego)
  fi
  # The add-opens/add-exports pair is what Hadoop's Kerberos code needs on Java 17 (see
  # AGENTS.md); harmless on the plain profile.
  docker exec stand-proxy java \
    --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
    --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
    -jar "${WRITER_JAR}" "$@" \
    --uri "${REST_NET_URL}" --namespace "${NS}" --table "${TABLE}" \
    ${auth_args[@]+"${auth_args[@]}"}
}

# Runs a beeline batch in the named container and echoes its output (stderr included, so the
# failure cases below can assert on the message the engine prints).
#   beeline_run <container> <jdbc-url> <sql...>
beeline_run() {
  local container="$1"
  local url="$2"
  shift 2
  local sql="$*"
  if [[ "${container}" == "stand-hs2" ]]; then
    # The Apache 3.1.3 image has no beeline wrapper script, only the Maven-resolved classpath.
    docker exec "${container}" bash -c \
      "java -cp '/opt/hs2/conf:/opt/hs2/lib/*' org.apache.hive.beeline.BeeLine -u '${url}' -n hive \
        --silent=true --showHeader=false --outputformat=tsv2 -e \"${sql}\"" 2>&1
  else
    docker exec "${container}" beeline -u "${url}" -n hive \
      --silent=true --showHeader=false --outputformat=tsv2 -e "${sql}" 2>&1
  fi
}

# The session settings every 3.1 SQL step needs: MR execution (the vendor build cannot name an
# engine in hive-site, and the stand has no YARN/Tez) and no vectorization (the Hive 3 Iceberg
# reader does not support it).
SQL_INIT="set hive.execution.engine=mr; set hive.vectorized.execution.enabled=false;"

sql_container() {
  case "$1" in
    hdp) printf 'stand-hs2-hdp' ;;
    apache) printf 'stand-hs2' ;;
    hive4) printf 'stand-hs2-hive4' ;;
    *) fail "not a SQL engine: $1" ;;
  esac
}

sql_url() {
  local host
  case "$1" in
    hdp) host=hs2-hdp ;;
    apache) host=hs2 ;;
    hive4) host=hs2-hive4 ;;
    *) fail "not a SQL engine: $1" ;;
  esac
  if [[ "${AUTH}" == "kerberos" ]]; then
    printf 'jdbc:hive2://%s:10000/default;principal=hive/%s@SMOKE.LOCAL' "${host}" "${host}"
  else
    printf 'jdbc:hive2://%s:10000/default' "${host}"
  fi
}

# Hive 4 has no MapReduce engine at all (the `set` would be refused) and its native Iceberg
# reader needs no vectorization switch, so it runs on the image's Tez local-mode defaults.
sql_init() {
  case "$1" in
    hive4) printf '' ;;
    *) printf '%s' "${SQL_INIT}" ;;
  esac
}

engine_label() {
  case "$1" in
    hdp) printf 'HDP HiveServer2 (Hortonworks front door)' ;;
    apache) printf 'Apache HiveServer2 (Apache front door)' ;;
    hive4) printf 'Hive 4 HiveServer2 (Hive 4 front door)' ;;
    *) fail "unknown engine: $1" ;;
  esac
}

rowlevel_kinit() {
  [[ "${AUTH}" == "kerberos" ]] || return 0
  local c
  for c in stand-proxy stand-hs2 stand-hs2-hdp stand-hs2-hive4; do
    docker exec "${c}" kinit -kt /keytabs/smoke-user.keytab smoke-user@SMOKE.LOCAL \
      || fail "kinit failed in ${c}"
  done
  # The namenode container needs one too: without a ticket every hdfs CLI call from it fails under
  # Kerberos, and the purge assertion below used to discard stderr, so an unreadable HDFS counted
  # as an empty one. It logs in as the node's own principal, the HDFS superuser.
  local host=${NAMENODE#stand-}
  docker exec "${NAMENODE}" kinit -kt "/keytabs/${host}.keytab" "hdfs/${host}@SMOKE.LOCAL" \
    || fail "kinit failed in ${NAMENODE}"
}

# Echoes a recursive listing of the table directory, empty when the directory does not exist.
# Any other failure is fatal: an HDFS that cannot be read must never look like an empty one.
namenode_table_listing() {
  local path="/warehouse/${PREFIX}/${TABLE}"
  local out status=0
  out="$(docker exec "${NAMENODE}" hdfs dfs -ls -R "${path}" 2>&1)" || status=$?
  if (( status != 0 )); then
    grep -q 'No such file or directory' <<< "${out}" \
      || fail "could not list ${path} on ${NAMENODE}: ${out}"
    return 0
  fi
  printf '%s\n' "${out}"
}

# --- assertions ------------------------------------------------------------------------------

# Echoes the table's whole content as a sorted "id:src,id:src" string. A full row scan on
# purpose: `select count(*)` can be answered from the Iceberg summary Hive keeps as table stats,
# which would make a reader that cannot apply delete files look like it can.
sql_content() {
  local who="$1"
  beeline_run "$(sql_container "${who}")" "$(sql_url "${who}")" \
    "$(sql_init "${who}") select id, src from ${TABLE};" \
    | sed -n 's/^\([0-9][0-9]*\)[[:space:]]\{1,\}\([A-Za-z0-9_]*\)[[:space:]]*$/\1:\2/p' \
    | sort -t: -k1,1n \
    | paste -sd, -
}

expect_sql_content() {
  local who="$1"
  local expected="$2"
  local label="$3"
  local actual
  actual="$(sql_content "${who}")"
  [[ "${actual}" == "${expected}" ]] \
    || fail "$(engine_label "${who}") ${label}: expected rows '${expected}', got '${actual}'"
  log "$(engine_label "${who}") ${label}: ${actual}"
}

expect_rest_rows() {
  local expected="$1"
  local label="$2"
  shift 2
  local out
  out="$(writer count "$@" | sed -n 's/^rows=\([0-9]*\)$/\1/p')"
  [[ "${out}" == "${expected}" ]] \
    || fail "REST ${label}: expected ${expected} row(s), got '${out}'"
  log "REST ${label}: ${expected} row(s) confirmed"
}

# Echoes "<data-files> <delete-files>" for the current snapshot. Two -e expressions rather than
# one alternation: this runs on the host, and BSD sed has no \| in a basic regular expression.
rest_files() {
  writer files \
    | sed -n -e 's/^data-files=\([0-9]*\)$/\1/p' -e 's/^delete-files=\([0-9]*\)$/\1/p' \
    | paste -sd' ' -
}

# A SQL statement that has to succeed. beeline can exit 0 while printing the failure, so the
# output is checked too - and kept, because a swallowed error would otherwise surface several
# assertions later as an unexplained row count.
sql_exec() {
  local who="$1"
  local sql="$2"
  local out rc=0
  out="$(beeline_run "$(sql_container "${who}")" "$(sql_url "${who}")" \
    "$(sql_init "${who}") ${sql}")" || rc=$?
  if [[ ${rc} -ne 0 ]] || grep -qE '^Error:' <<< "${out}"; then
    fail "$(engine_label "${who}") failed on '${sql}' (exit ${rc}): ${out}"
  fi
}

# A SQL statement that has to be refused, with the message it has to be refused with.
expect_sql_error() {
  local who="$1"
  local sql="$2"
  local expected="$3"
  local out
  out="$(beeline_run "$(sql_container "${who}")" "$(sql_url "${who}")" \
    "$(sql_init "${who}") ${sql}" || true)"
  grep -qF "${expected}" <<< "${out}" \
    || fail "$(engine_label "${who}") was expected to refuse '${sql}' with '${expected}', got: ${out}"
  log "$(engine_label "${who}") refuses '${sql}': ${expected}"
}

# --- one pass over a delete mode ---------------------------------------------------------------

run_mode() {
  local mode="$1"

  log "=== ${mode}: REST creates '${NS}.${TABLE}' (format-version=2, write.delete.mode=write.update.mode=${mode})"
  # A rerun on a dirty stand must not half-fail on a leftover table; the drop is not asserted.
  writer drop --purge >/dev/null 2>&1 || true
  writer create --properties \
    "format-version=2,write.delete.mode=${mode},write.update.mode=${mode}" >/dev/null
  writer append --rows 5 --marker rest >/dev/null
  expect_rest_rows 5 "sees its own 5 rows"

  local files
  files="$(rest_files)"
  [[ "${files}" == "1 0" ]] \
    || fail "a freshly appended table should be 1 data file and no delete files, got '${files}'"
  log "REST file shape after the append: data-files=1 delete-files=0"

  # The control: every engine reads the table before any row-level DML touches it, so a failure
  # further down is attributable to the delete files and not to the table's shape.
  local baseline="1:rest,2:rest,3:rest,4:rest,5:rest"
  local who
  for who in hive4 hdp apache; do
    expect_sql_content "${who}" "${baseline}" "reads the appended table"
  done

  log "--- ${mode}: Hive 4 deletes two rows"
  sql_exec hive4 "delete from ${TABLE} where id = 2 or id = 4;"
  expect_rest_rows 3 "sees the delete (5 - 2)"
  expect_rest_rows 0 "no longer finds the deleted id=2" --where id=2

  files="$(rest_files)"
  local delete_files="${files#* }"
  if [[ "${mode}" == "merge-on-read" ]]; then
    [[ "${delete_files}" -gt 0 ]] \
      || fail "merge-on-read produced no delete file (files: ${files}); the mode did not take effect"
  else
    [[ "${delete_files}" -eq 0 ]] \
      || fail "copy-on-write produced ${delete_files} delete file(s) (files: ${files}); the mode did not take effect"
  fi
  log "REST file shape after the delete: data-files=${files%% *} delete-files=${delete_files}"

  local after_delete="1:rest,3:rest,5:rest"
  for who in hive4 hdp apache; do
    expect_sql_content "${who}" "${after_delete}" "reads the table after the delete"
  done

  log "--- ${mode}: Hive 4 updates one row"
  sql_exec hive4 "update ${TABLE} set src = 'updated' where id = 5;"
  expect_rest_rows 3 "still sees 3 rows after the update"
  expect_rest_rows 1 "sees the new value" --where src=updated
  expect_rest_rows 2 "sees the two untouched rows" --where src=rest

  local after_update="1:rest,3:rest,5:updated"
  for who in hive4 hdp apache; do
    expect_sql_content "${who}" "${after_update}" "reads the table after the update"
  done

  # Reading a row-level-modified table is one thing; committing onto it is another - the 3.1
  # storage handler has to append to a snapshot whose state it cannot itself produce, and the
  # INSERT opens with the DROPPROPS alter_table that IcebergTablePointerGuard has to survive.
  log "--- ${mode}: the HDP 3.1 engine appends to the row-level-modified table"
  sql_exec hdp "insert into ${TABLE} values (7, 'hdp');"
  local after_insert="1:rest,3:rest,5:updated,7:hdp"
  expect_rest_rows 4 "sees the 3.1 append on top of the row-level changes"
  for who in hive4 hdp apache; do
    expect_sql_content "${who}" "${after_insert}" "reads the table after the 3.1 append"
  done

  # The 3.1 line has no row-level DML over Iceberg at all: iceberg-hive-runtime 1.6.1 registers
  # no ACID-capable storage handler for Hive 3, so the semantic analyzer stops the statement
  # before any plan is made. It is a clean compile-time refusal, not a partial write.
  log "--- ${mode}: the 3.1 engines try row-level DML of their own"
  local not_transactional="Attempt to do update or delete on table ${NS}.${TABLE} that is not transactional"
  for who in hdp apache; do
    expect_sql_error "${who}" "delete from ${TABLE} where id = 1;" "${not_transactional}"
    expect_sql_error "${who}" "update ${TABLE} set src = 'from31' where id = 1;" "${not_transactional}"
  done
  expect_rest_rows 4 "still sees 4 rows after the refused 3.1 statements"
  expect_rest_rows 0 "sees nothing written by the refused 3.1 statements" --where src=from31

  log "--- ${mode}: REST drops the table with purge"
  writer drop --purge >/dev/null
  local leftovers
  leftovers="$(namenode_table_listing | grep -cE 'parquet|avro|metadata.json' || true)"
  [[ "${leftovers}" == "0" ]] \
    || fail "purge left ${leftovers} file(s) under /warehouse/${PREFIX}/${TABLE}"
  # The emptied directory itself is not removed by a purge; drop it so a rerun starts clean. -f
  # already tolerates a missing path, so a non-zero status here is a real failure.
  docker exec "${NAMENODE}" hdfs dfs -rm -r -f "/warehouse/${PREFIX}/${TABLE}" >/dev/null 2>&1 \
    || fail "could not remove /warehouse/${PREFIX}/${TABLE} on ${NAMENODE}"

  log "=== ${mode}: passed"
}

# --- the scenario ------------------------------------------------------------------------------

rowlevel_kinit

for mode in "${MODES[@]}"; do
  run_mode "${mode}"
done

log "iceberg row-level smoke passed (auth=${AUTH}, modes '${MODES[*]}', table '${NS}.${TABLE}', backend catalog '${PREFIX}')"
