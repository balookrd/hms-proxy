#!/usr/bin/env bash
# Iceberg interop smoke: one table crosses every engine and every front-door dialect over
# whichever metastore the stand currently has as its default catalog.
#
#   1. REST creates the table and writes real rows (iceberg-rest-writer inside stand-proxy).
#   2. The vendor HDP HiveServer2 (Hortonworks front door, 9084) reads them and appends its own.
#   3. The Apache HiveServer2 (Apache front door, 9083) appends and reads everything.
#   4. The Hive 4 HiveServer2 (Hive 4 front door, 9085 - the APACHE_4_1_0 dialect) reads all of
#      the above and appends its own row through its native Iceberg support.
#   5. REST sees every SQL commit (client-side scan), then drops the table.
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
#
# Every profile needs the SQL clients too, so bring the stand up with the hdp and hive4fe
# compose profiles on top of the backend's own, e.g.:
#   docker compose --env-file .env.hive4 --profile hive4 --profile hive4fe --profile hdp up -d --build
set -euo pipefail

STAND_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

AUTH=plain
PREFIX=${INTEROP_PREFIX:-hive4}
NAMENODE=${INTEROP_NAMENODE:-}
while [[ $# -gt 0 ]]; do
  case "$1" in
    --kerberos) AUTH=kerberos; shift ;;
    --prefix) [[ $# -ge 2 ]] || { echo "missing value for --prefix" >&2; exit 1; }; PREFIX="$2"; shift 2 ;;
    --namenode) [[ $# -ge 2 ]] || { echo "missing value for --namenode" >&2; exit 1; }; NAMENODE="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 1 ;;
  esac
done

# The apache catalog is the only one on the second HDFS cluster; everything else lives on the
# first. Only used to delete the table's files after a (non-purge) drop.
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

# --- the scenario ----------------------------------------------------------------------------

interop_kinit

# A rerun on a dirty stand must not half-fail on a leftover table; neither drop is asserted.
# No purge anywhere in this scenario: DELETE with purgeRequested=true currently dies server-side
# on a missing Avro class (tracked separately); the data files are removed explicitly below.
log "defensive drop of a possible leftover '${NS}.${TABLE}'"
writer drop >/dev/null 2>&1 || true

log "step 1: REST creates '${NS}.${TABLE}' and writes 2 rows (src=rest)"
writer create
writer append --rows 2 --marker rest
out="$(writer count)"
grep -q '^rows=2$' <<< "${out}" || fail "REST-side count after the REST append expected rows=2, got: ${out}"

code="$(rest_curl -o /dev/null -w '%{http_code}' "$(rest_url)/v1/${PREFIX}/namespaces/${NS}/tables/${TABLE}")"
[[ "${code}" == "200" ]] || fail "REST load of '${TABLE}' returned HTTP ${code}"
log "REST metadata load answers 200"

log "step 2: HDP HiveServer2 (Hortonworks front door) reads and appends"
out="$(beeline_run stand-hs2-hdp "$(hdp_jdbc_url)" \
  "${SQL_INIT} select count(*) from ${TABLE};")"
expect_rows "HDP read of the REST-written rows" 2 "${out}"

beeline_run stand-hs2-hdp "$(hdp_jdbc_url)" \
  "${SQL_INIT} insert into ${TABLE} values (100, 'hdp');" >/dev/null
out="$(beeline_run stand-hs2-hdp "$(hdp_jdbc_url)" \
  "${SQL_INIT} select count(*) from ${TABLE};")"
expect_rows "HDP read after its own append" 3 "${out}"

log "step 3: Apache HiveServer2 (Apache front door) appends and reads"
beeline_run stand-hs2 "$(apache_jdbc_url)" \
  "${SQL_INIT} insert into ${TABLE} values (200, 'apache');" >/dev/null
out="$(beeline_run stand-hs2 "$(apache_jdbc_url)" \
  "${SQL_INIT} select count(*) from ${TABLE};")"
expect_rows "Apache read after both SQL appends" 4 "${out}"

log "step 4: Hive 4 HiveServer2 (Hive 4 front door) reads and appends"
# No SQL_INIT here: Hive 4 has no MapReduce engine at all (`set hive.execution.engine=mr` would
# be refused), and its native Iceberg reader needs no vectorization switch - the image's Tez
# local mode defaults are exactly what this server should run with.
out="$(beeline_run stand-hs2-hive4 "$(hive4_jdbc_url)" \
  "select count(*) from ${TABLE};")"
expect_rows "Hive 4 read of the 3.1-era appends" 4 "${out}"

beeline_run stand-hs2-hive4 "$(hive4_jdbc_url)" \
  "insert into ${TABLE} values (300, 'hive4');" >/dev/null
out="$(beeline_run stand-hs2-hive4 "$(hive4_jdbc_url)" \
  "select count(*) from ${TABLE};")"
expect_rows "Hive 4 read after its own append" 5 "${out}"

log "step 5: REST sees every SQL commit and drops the table"
out="$(writer count)"
grep -q '^rows=5$' <<< "${out}" \
  || fail "REST-side scan after the SQL appends expected rows=5, got: ${out}"
log "REST-side scan confirms all 5 rows (2 rest + 1 hdp + 1 apache + 1 hive4)"

code="$(rest_curl -o /dev/null -w '%{http_code}' -X DELETE \
  "$(rest_url)/v1/${PREFIX}/namespaces/${NS}/tables/${TABLE}")"
[[ "${code}" =~ ^2 ]] || fail "REST drop of '${TABLE}' returned HTTP ${code}"

code="$(rest_curl -o /dev/null -w '%{http_code}' "$(rest_url)/v1/${PREFIX}/namespaces/${NS}/tables/${TABLE}")"
[[ "${code}" == "404" ]] || fail "REST load of dropped '${TABLE}' expected HTTP 404, got ${code}"

# The non-purge drop leaves the data and metadata files behind; remove them explicitly so a
# rerun starts from a genuinely clean location.
docker exec "${NAMENODE}" hdfs dfs -rm -r -f "/warehouse/${PREFIX}/${TABLE}" >/dev/null 2>&1 || true

out="$(beeline_run stand-hs2 "$(apache_jdbc_url)" "show tables like '${TABLE}';")"
if grep -q "${TABLE}" <<< "${out}"; then
  fail "SQL still lists '${TABLE}' after the REST drop: ${out}"
fi

log "iceberg interop smoke passed (auth=${AUTH}, table '${NS}.${TABLE}', backend catalog '${PREFIX}')"
