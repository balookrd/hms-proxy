#!/usr/bin/env bash
# Iceberg interop smoke: one table crosses every engine and every front-door dialect over
# whichever metastore the stand currently has as its default catalog.
#
# Four participants take part - the REST front door and the three SQL dialects (Hortonworks
# 9084, Apache 9083, Hive 4 9085). --origin picks which one creates the table and writes the
# first rows; the other three then change it in turn, each reading the running total before its
# own append, so every hand-off is proven to be visible across the front-door boundary. REST
# drops the table at the end. With --origin rest the table is born in the Iceberg catalog and
# SQL takes it over; with a SQL origin it is born as a Hive table (STORED BY iceberg) and REST
# has to resolve and commit onto something it did not create.
#
# The backend under test is whichever catalog the running proxy config makes default - writes
# are gated to it - so --prefix has to name that catalog and match the config the stand was
# brought up with:
#
#   default profile          -> --prefix hdp     (Hortonworks 3.1.0 metastore)
#   .env.apache              -> --prefix apache  (Apache 3.1.3 metastore, second HDFS cluster)
#   .env.hive4               -> --prefix hive4   (Apache Hive 4.1.0 metastore)
#
# Stand-local on purpose: every step is a docker exec into the container that owns the engine,
# which has no meaning on a real installation. Run it from anywhere:
#
#   smoke-stand/run-iceberg-interop-smoke.sh --prefix hdp
#   smoke-stand/run-iceberg-interop-smoke.sh --prefix hive4 --kerberos
#   smoke-stand/run-iceberg-interop-smoke.sh --prefix hdp --origin apache
#
# Every profile needs the SQL clients too, so bring the stand up with the hdp and hive4fe
# compose profiles on top of the backend's own, e.g.:
#   docker compose --env-file .env.hive4 --profile hive4 --profile hive4fe --profile hdp up -d --build
set -euo pipefail

STAND_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

AUTH=plain
PREFIX=${INTEROP_PREFIX:-hive4}
NAMENODE=${INTEROP_NAMENODE:-}
ORIGIN=${INTEROP_ORIGIN:-rest}
while [[ $# -gt 0 ]]; do
  case "$1" in
    --kerberos) AUTH=kerberos; shift ;;
    --prefix) [[ $# -ge 2 ]] || { echo "missing value for --prefix" >&2; exit 1; }; PREFIX="$2"; shift 2 ;;
    --namenode) [[ $# -ge 2 ]] || { echo "missing value for --namenode" >&2; exit 1; }; NAMENODE="$2"; shift 2 ;;
    --origin) [[ $# -ge 2 ]] || { echo "missing value for --origin" >&2; exit 1; }; ORIGIN="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 1 ;;
  esac
done

# Participation order: the origin creates the table and writes first, the rest take it in turn.
PARTICIPANTS=(rest hdp apache hive4)
OTHERS=()
for candidate in "${PARTICIPANTS[@]}"; do
  [[ "${candidate}" == "${ORIGIN}" ]] || OTHERS+=("${candidate}")
done
[[ ${#OTHERS[@]} -eq $((${#PARTICIPANTS[@]} - 1)) ]] \
  || { echo "unknown --origin '${ORIGIN}'; expected one of: ${PARTICIPANTS[*]}" >&2; exit 1; }

# Every origin runs with all four participants. This used to carve hdp and apache out of a
# `--origin hive4` run, on the belief that a Hive 4-created Iceberg table is unreadable by the 3.1
# line - `STORED BY ICEBERG` was said to leave the descriptor's inputFormat as the abstract
# org.apache.hadoop.mapred.FileInputFormat, which Hive 3.1 instantiates and dies on. Measured
# again on 2026-07-31, that is not what happens: the descriptor lands carrying the concrete
# HiveIcebergInputFormat, and both 3.1 engines read the table. The carve-out meant the scenario
# asserted the limitation instead of testing it, so it could never notice - which is exactly why
# a skip must never stand in for a check here.

# The apache catalog is the only one on the second HDFS cluster; everything else lives on the
# first. Only used to verify and clean up the table directory after the purge-drop.
if [[ -z "${NAMENODE}" ]]; then
  NAMENODE=$([[ "${PREFIX}" == "apache" ]] && echo stand-namenode-b || echo stand-namenode)
fi

NS=${INTEROP_NAMESPACE:-default}
TABLE=${INTEROP_TABLE:-smoke_iceberg_interop}
REST_HOST_URL=${INTEROP_REST_URL:-http://localhost:19183}
# In-network REST URL, used by the writer and by kerberos curl (SPNEGO only resolves in-network).
REST_NET_URL=http://proxy:9183
WRITER_JAR=/opt/hms-proxy/iceberg-rest-writer.jar

fail() {
  printf '[iceberg-interop] error: %s\n' "$*" >&2
  exit 1
}

log() {
  printf '[iceberg-interop] %s\n' "$*"
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

rest_curl() {
  # Metadata-only checks. Plain talks to the host port; kerberos has to run curl --negotiate
  # inside the network, after a kinit that interop_kinit already did.
  if [[ "${AUTH}" == "kerberos" ]]; then
    docker exec stand-proxy curl -sS --negotiate -u : "$@"
  else
    curl -sS "$@"
  fi
}

rest_url() {
  if [[ "${AUTH}" == "kerberos" ]]; then
    printf '%s' "${REST_NET_URL}"
  else
    printf '%s' "${REST_HOST_URL}"
  fi
}

# Runs a beeline batch in the named container and echoes its output.
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
        --silent=true --showHeader=false --outputformat=tsv2 -e \"${sql}\""
  else
    docker exec "${container}" beeline -u "${url}" -n hive \
      --silent=true --showHeader=false --outputformat=tsv2 -e "${sql}"
  fi
}

# The session settings every SQL step needs: MR execution (the vendor build cannot name an
# engine in hive-site, and the stand has no YARN/Tez) and no vectorization (the Hive 3 Iceberg
# reader does not support it).
SQL_INIT="set hive.execution.engine=mr; set hive.vectorized.execution.enabled=false;"

hdp_jdbc_url() {
  if [[ "${AUTH}" == "kerberos" ]]; then
    printf 'jdbc:hive2://hs2-hdp:10000/default;principal=hive/hs2-hdp@SMOKE.LOCAL'
  else
    printf 'jdbc:hive2://hs2-hdp:10000/default'
  fi
}

apache_jdbc_url() {
  if [[ "${AUTH}" == "kerberos" ]]; then
    printf 'jdbc:hive2://hs2:10000/default;principal=hive/hs2@SMOKE.LOCAL'
  else
    printf 'jdbc:hive2://hs2:10000/default'
  fi
}

hive4_jdbc_url() {
  if [[ "${AUTH}" == "kerberos" ]]; then
    printf 'jdbc:hive2://hs2-hive4:10000/default;principal=hive/hs2-hive4@SMOKE.LOCAL'
  else
    printf 'jdbc:hive2://hs2-hive4:10000/default'
  fi
}

interop_kinit() {
  [[ "${AUTH}" == "kerberos" ]] || return 0
  local c
  for c in stand-proxy stand-hs2 stand-hs2-hdp stand-hs2-hive4; do
    docker exec "${c}" kinit -kt /keytabs/smoke-user.keytab smoke-user@SMOKE.LOCAL \
      || fail "kinit failed in ${c}"
  done
}

expect_rows() {
  local label="$1"
  local expected="$2"
  local output="$3"
  grep -qE "(^|[[:space:]])${expected}([[:space:]]|$)" <<< "${output}" \
    || fail "${label}: expected count ${expected}, got: ${output}"
  log "${label}: count=${expected} confirmed"
}

# A fixed 3.1 front door for the descriptor check below - "hdp" always fronts whatever catalog
# is default (routing.default-catalog), so this works unchanged regardless of --prefix.
beeline_query() {
  beeline_run "$(sql_container hdp)" "$(hdp_jdbc_url)" "$@"
}

# A row count alone cannot catch a relapse: Hive 4 answers count(*) from the Iceberg summary it
# keeps as table stats, so it reports the right number even off a descriptor the 3.1 line cannot
# open. describe formatted instead reads the raw metastore record through a 3.1 front door, the
# same shape H12 lost - see IcebergTablePointerGuard and rest-catalog.hive-engine-descriptor.
assert_hive_readable_descriptor() {
  local stage="$1" descriptor
  descriptor="$(beeline_query "describe formatted ${TABLE};" | grep -iE 'inputformat' | head -n 1)"
  case "${descriptor}" in
    *HiveIcebergInputFormat*) log "descriptor still Hive-readable after ${stage}" ;;
    *) fail "after ${stage} the descriptor lost its Hive input format: ${descriptor}" ;;
  esac
}

# --- participants ----------------------------------------------------------------------------
#
# Four front doors can each play any role. Every participant knows how to create the table,
# append rows to it and count them; the scenario below picks one as the origin and hands the
# table to the other three.

participant_label() {
  case "$1" in
    rest) printf 'REST front door' ;;
    hdp) printf 'HDP HiveServer2 (Hortonworks front door)' ;;
    apache) printf 'Apache HiveServer2 (Apache front door)' ;;
    hive4) printf 'Hive 4 HiveServer2 (Hive 4 front door)' ;;
    *) fail "unknown participant: $1" ;;
  esac
}

sql_container() {
  case "$1" in
    hdp) printf 'stand-hs2-hdp' ;;
    apache) printf 'stand-hs2' ;;
    hive4) printf 'stand-hs2-hive4' ;;
    *) fail "not a SQL participant: $1" ;;
  esac
}

sql_url() {
  case "$1" in
    hdp) hdp_jdbc_url ;;
    apache) apache_jdbc_url ;;
    hive4) hive4_jdbc_url ;;
    *) fail "not a SQL participant: $1" ;;
  esac
}

# Hive 4 has no MapReduce engine at all (the `set` would be refused) and its native Iceberg
# reader needs no vectorization switch, so it runs on the image's Tez local-mode defaults.
sql_init() {
  case "$1" in
    hive4) printf '' ;;
    *) printf '%s' "${SQL_INIT}" ;;
  esac
}

# The DDL that makes an Iceberg table. Hive 4 has first-class syntax; the 3.1 line names the
# storage handler that iceberg-hive-runtime brings.
sql_create_ddl() {
  case "$1" in
    hive4) printf 'create table %s (id int, src string) stored by iceberg;' "${TABLE}" ;;
    *) printf "create table %s (id int, src string) stored by 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler';" "${TABLE}" ;;
  esac
}

# --- participant operations ------------------------------------------------------------------

participant_create() {
  local who="$1"
  if [[ "${who}" == "rest" ]]; then
    writer create
    return
  fi
  beeline_run "$(sql_container "${who}")" "$(sql_url "${who}")" \
    "$(sql_init "${who}") $(sql_create_ddl "${who}")" >/dev/null
}

participant_append() {
  local who="$1"
  local rows="$2"
  local id="$3"
  if [[ "${who}" == "rest" ]]; then
    writer append --rows "${rows}" --marker rest
    return
  fi
  local values=""
  local i
  for ((i = 0; i < rows; i++)); do
    [[ -n "${values}" ]] && values+=", "
    values+="($((id + i)), '${who}')"
  done
  beeline_run "$(sql_container "${who}")" "$(sql_url "${who}")" \
    "$(sql_init "${who}") insert into ${TABLE} values ${values};" >/dev/null
}

# Echoes the row count this participant sees, as a bare number.
participant_count() {
  local who="$1"
  if [[ "${who}" == "rest" ]]; then
    writer count | sed -n 's/^rows=\([0-9]*\)$/\1/p'
    return
  fi
  beeline_run "$(sql_container "${who}")" "$(sql_url "${who}")" \
    "$(sql_init "${who}") select count(*) from ${TABLE};"
}

# --- the scenario ----------------------------------------------------------------------------

interop_kinit

# A rerun on a dirty stand must not half-fail on a leftover table; the drop is not asserted.
log "defensive drop of a possible leftover '${NS}.${TABLE}'"
writer drop --purge >/dev/null 2>&1 || true

log "origin: ${ORIGIN} ($(participant_label "${ORIGIN}")) creates '${NS}.${TABLE}' and writes 2 rows"
participant_create "${ORIGIN}"
participant_append "${ORIGIN}" 2 1
count="$(participant_count "${ORIGIN}")"
expect_rows "$(participant_label "${ORIGIN}") read of its own rows" 2 "${count}"
[[ "${ORIGIN}" == "rest" ]] && assert_hive_readable_descriptor "the origin's REST commit"

# Whoever created the table, its metadata has to be loadable through REST: a SQL-created Iceberg
# table is only interoperable if the REST catalog can resolve it too.
code="$(rest_curl -o /dev/null -w '%{http_code}' "$(rest_url)/v1/${PREFIX}/namespaces/${NS}/tables/${TABLE}")"
[[ "${code}" == "200" ]] || fail "REST load of '${TABLE}' returned HTTP ${code}"
log "REST metadata load answers 200"

# Everyone else changes the table in turn, each reading the running total first - that read is
# what proves the previous participant's commit is visible across the front-door boundary.
expected=2
next_id=100
for who in "${OTHERS[@]}"; do
  log "${who} ($(participant_label "${who}")) reads what the others wrote, then appends"
  count="$(participant_count "${who}")"
  expect_rows "$(participant_label "${who}") read before its own append" "${expected}" "${count}"

  participant_append "${who}" 1 "${next_id}"
  expected=$((expected + 1))
  next_id=$((next_id + 100))
  count="$(participant_count "${who}")"
  expect_rows "$(participant_label "${who}") read after its own append" "${expected}" "${count}"
  [[ "${who}" == "rest" ]] && assert_hive_readable_descriptor "the REST participant's append"
done

log "final: every front door sees all ${expected} rows"
for who in "${ORIGIN}" "${OTHERS[@]}"; do
  count="$(participant_count "${who}")"
  expect_rows "$(participant_label "${who}") final read" "${expected}" "${count}"
done

log "REST drops the table with purge"
code="$(rest_curl -o /dev/null -w '%{http_code}' -X DELETE \
  "$(rest_url)/v1/${PREFIX}/namespaces/${NS}/tables/${TABLE}?purgeRequested=true")"
[[ "${code}" =~ ^2 ]] || fail "REST purge-drop of '${TABLE}' returned HTTP ${code}"

code="$(rest_curl -o /dev/null -w '%{http_code}' "$(rest_url)/v1/${PREFIX}/namespaces/${NS}/tables/${TABLE}")"
[[ "${code}" == "404" ]] || fail "REST load of dropped '${TABLE}' expected HTTP 404, got ${code}"

# A purge must leave no data, manifest or metadata file behind - that walk over the manifests is
# the one REST path that reads Avro, so a broken dependency shows up here and nowhere else.
leftovers="$(docker exec "${NAMENODE}" hdfs dfs -ls -R "/warehouse/${PREFIX}/${TABLE}" 2>/dev/null \
  | grep -cE 'parquet|avro|metadata.json' || true)"
[[ "${leftovers}" == "0" ]] \
  || fail "purge left ${leftovers} file(s) under /warehouse/${PREFIX}/${TABLE}"
log "purge left no data, manifest or metadata files behind"

# The emptied directory itself is not removed by a purge; drop it so a rerun starts clean.
docker exec "${NAMENODE}" hdfs dfs -rm -r -f "/warehouse/${PREFIX}/${TABLE}" >/dev/null 2>&1 || true

out="$(beeline_run stand-hs2 "$(apache_jdbc_url)" "show tables like '${TABLE}';")"
if grep -q "${TABLE}" <<< "${out}"; then
  fail "SQL still lists '${TABLE}' after the REST drop: ${out}"
fi

log "iceberg interop smoke passed (auth=${AUTH}, origin ${ORIGIN}, table '${NS}.${TABLE}', backend catalog '${PREFIX}')"
