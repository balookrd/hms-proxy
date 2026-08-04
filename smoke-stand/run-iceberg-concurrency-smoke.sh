#!/usr/bin/env bash
# Writer-isolation smoke: do concurrent commits to one Iceberg table through the REST front door
# lose updates?
#
# This is the check the whole write gate rests on. Writes are allowed only in the default
# catalog because only there does a commit take a real Hive lock through
# HiveTableOperations.commit; every other catalog is served by the synthetic shim, which grants
# locks without checking conflicts (pinned by
# RoutingMetaStoreProxySyntheticReadLocksTest#syntheticShimGrantsConflictingExclusiveLocksOnTheSameObject).
# That argument had never been tested against a real metastore: every other scenario is
# single-client.
#
# The assertion is deliberately not "all writers succeed". Iceberg commits optimistically and
# retries, and a writer that runs out of retries and fails loudly is CORRECT behaviour - the
# defect would be a writer that reports success while its rows are gone. So the scenario counts
# the writers that exited 0 and requires the table to hold exactly that many rows, plus the
# baseline one.
#
#   smoke-stand/run-iceberg-concurrency-smoke.sh --prefix hive4
#   smoke-stand/run-iceberg-concurrency-smoke.sh --prefix hdp --writers 6 --kerberos
set -euo pipefail

AUTH=plain
PREFIX=${CONCURRENCY_PREFIX:-hive4}
WRITERS=${CONCURRENCY_WRITERS:-5}
SQL_WRITERS=${CONCURRENCY_SQL_WRITERS:-0}
SQL_ENGINE=${CONCURRENCY_SQL_ENGINE:-hdp}
NAMENODE=${CONCURRENCY_NAMENODE:-}
while [[ $# -gt 0 ]]; do
  case "$1" in
    --kerberos) AUTH=kerberos; shift ;;
    --prefix) [[ $# -ge 2 ]] || { echo "missing value for --prefix" >&2; exit 1; }; PREFIX="$2"; shift 2 ;;
    --writers) [[ $# -ge 2 ]] || { echo "missing value for --writers" >&2; exit 1; }; WRITERS="$2"; shift 2 ;;
    --sql-writers) [[ $# -ge 2 ]] || { echo "missing value for --sql-writers" >&2; exit 1; }; SQL_WRITERS="$2"; shift 2 ;;
    --sql-engine) [[ $# -ge 2 ]] || { echo "missing value for --sql-engine" >&2; exit 1; }; SQL_ENGINE="$2"; shift 2 ;;
    --namenode) [[ $# -ge 2 ]] || { echo "missing value for --namenode" >&2; exit 1; }; NAMENODE="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "${NAMENODE}" ]]; then
  NAMENODE=$([[ "${PREFIX}" == "apache" ]] && echo stand-namenode-b || echo stand-namenode)
fi

NS=${CONCURRENCY_NAMESPACE:-default}
TABLE=${CONCURRENCY_TABLE:-smoke_iceberg_concurrent}
REST_HOST_URL=${CONCURRENCY_REST_URL:-http://localhost:19183}
REST_NET_URL=http://proxy:9183
WRITER_JAR=/opt/hms-proxy/iceberg-rest-writer.jar
LOG_DIR="$(mktemp -d "${TMPDIR:-/tmp}/hms-concurrency.XXXXXX")"
trap 'rm -rf "${LOG_DIR}"' EXIT

fail() {
  printf '[iceberg-concurrency] error: %s\n' "$*" >&2
  exit 1
}

log() {
  printf '[iceberg-concurrency] %s\n' "$*"
}

writer() {
  local -a auth_args=()
  if [[ "${AUTH}" == "kerberos" ]]; then
    auth_args+=(--keytab /keytabs/smoke-user.keytab --principal smoke-user@SMOKE.LOCAL)
    auth_args+=(--rest-auth spnego)
  fi
  docker exec stand-proxy java \
    --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
    --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
    -jar "${WRITER_JAR}" "$@" \
    --uri "${REST_NET_URL}" --namespace "${NS}" --table "${TABLE}" \
    ${auth_args[@]+"${auth_args[@]}"}
}

rest_curl() {
  if [[ "${AUTH}" == "kerberos" ]]; then
    docker exec stand-proxy curl -sS --negotiate -u : "$@"
  else
    curl -sS "$@"
  fi
}

rest_url() {
  if [[ "${AUTH}" == "kerberos" ]]; then printf '%s' "${REST_NET_URL}"; else printf '%s' "${REST_HOST_URL}"; fi
}

sql_container() {
  case "${SQL_ENGINE}" in
    hdp) printf 'stand-hs2-hdp' ;;
    apache) printf 'stand-hs2' ;;
    hive4) printf 'stand-hs2-hive4' ;;
    *) fail "unknown --sql-engine '${SQL_ENGINE}'; expected hdp, apache or hive4" ;;
  esac
}

sql_url() {
  local host principal
  case "${SQL_ENGINE}" in
    hdp) host=hs2-hdp; principal=hive/hs2-hdp@SMOKE.LOCAL ;;
    apache) host=hs2; principal=hive/hs2@SMOKE.LOCAL ;;
    hive4) host=hs2-hive4; principal=hive/hs2-hive4@SMOKE.LOCAL ;;
    *) fail "unknown --sql-engine '${SQL_ENGINE}'" ;;
  esac
  if [[ "${AUTH}" == "kerberos" ]]; then
    printf 'jdbc:hive2://%s:10000/default;principal=%s' "${host}" "${principal}"
  else
    printf 'jdbc:hive2://%s:10000/default' "${host}"
  fi
}

# Hive 4 has no MapReduce engine and needs no vectorization switch; the 3.1 line needs both.
sql_init() {
  if [[ "${SQL_ENGINE}" == "hive4" ]]; then
    printf ''
  else
    printf 'set hive.execution.engine=mr; set hive.vectorized.execution.enabled=false;'
  fi
}

beeline_query() {
  local sql="$1"
  local container
  container="$(sql_container)"
  if [[ "${container}" == "stand-hs2" ]]; then
    docker exec "${container}" bash -c \
      "java -cp '/opt/hs2/conf:/opt/hs2/lib/*' org.apache.hive.beeline.BeeLine -u '$(sql_url)' -n hive \
        --silent=true --showHeader=false --outputformat=tsv2 -e \"$(sql_init) ${sql}\"" 2>/dev/null
  else
    docker exec "${container}" beeline -u "$(sql_url)" -n hive \
      --silent=true --showHeader=false --outputformat=tsv2 -e "$(sql_init) ${sql}" 2>/dev/null
  fi
}

sql_insert() {
  local id="$1"
  local container
  container="$(sql_container)"
  local sql
  sql="$(sql_init) insert into ${TABLE} values (${id}, 'sql${id}');"
  if [[ "${container}" == "stand-hs2" ]]; then
    docker exec "${container}" bash -c \
      "java -cp '/opt/hs2/conf:/opt/hs2/lib/*' org.apache.hive.beeline.BeeLine -u '$(sql_url)' -n hive \
        --silent=true --showHeader=false --outputformat=tsv2 -e \"${sql}\""
  else
    docker exec "${container}" beeline -u "$(sql_url)" -n hive \
      --silent=true --showHeader=false --outputformat=tsv2 -e "${sql}"
  fi
}

if [[ "${AUTH}" == "kerberos" ]]; then
  # Every container that opens a connection needs its own TGT: the REST writer runs in
  # stand-proxy, and beeline runs inside whichever HiveServer2 the SQL side uses.
  kinit_targets=(stand-proxy)
  [[ "${SQL_WRITERS}" -gt 0 ]] && kinit_targets+=("$(sql_container)")
  for target in "${kinit_targets[@]}"; do
    docker exec "${target}" kinit -kt /keytabs/smoke-user.keytab smoke-user@SMOKE.LOCAL \
      || fail "kinit failed in ${target}"
  done
  # The namenode container holds no ticket of its own either, and the directory cleanups below run
  # from it: without this every hdfs CLI call fails with "Client cannot authenticate via:[TOKEN,
  # KERBEROS]" and leaves the files of a failed run for the next one to inherit.
  namenode_host=${NAMENODE#stand-}
  docker exec "${NAMENODE}" kinit -kt "/keytabs/${namenode_host}.keytab" \
    "hdfs/${namenode_host}@SMOKE.LOCAL" || fail "kinit failed in ${NAMENODE}"
fi

log "preparing '${NS}.${TABLE}' on catalog '${PREFIX}' with one baseline row"
# A previous run that failed mid-way leaves the table behind, and its files with it. Drop through
# REST, then clear the directory directly, so one failed run cannot cascade into the next. -f
# already tolerates a missing path, so a non-zero status here is a real failure.
writer drop --purge >/dev/null 2>&1 || true
docker exec "${NAMENODE}" hdfs dfs -rm -r -f "/warehouse/${PREFIX}/${TABLE}" >/dev/null 2>&1 \
  || fail "could not remove /warehouse/${PREFIX}/${TABLE} on ${NAMENODE}"
writer create >/dev/null
writer append --rows 1 --marker baseline >/dev/null

if [[ "${SQL_WRITERS}" -gt 0 ]]; then
  log "launching ${SQL_WRITERS} SQL INSERT(s) (${SQL_ENGINE} front door), then REST appends in rounds of ${WRITERS} until they finish"
else
  log "launching ${WRITERS} concurrent appends"
fi

LOG_SINCE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
succeeded=0
failed=0

count_result() {
  local name="$1"
  local status="$2"
  if [[ "${status}" -eq 0 ]]; then
    succeeded=$((succeeded + 1))
    return
  fi
  failed=$((failed + 1))
  log "writer ${name} failed (that is allowed - a commit may legitimately run out of retries):"
  # The known commit-conflict shapes first; anything else gets its last error line, so a failure
  # this scenario has not seen before still says what it was.
  if ! grep -ohE '[A-Za-z.]*(CommitFailedException|ValidationException|CommitStateUnknownException)[^"]*' \
      "${LOG_DIR}/${name}.log" | head -1 | sed 's/^/    /' | grep -q .; then
    grep -ohE '(Error|FAILED|Exception)[^"]*' "${LOG_DIR}/${name}.log" | tail -1 | cut -c1-220 | sed 's/^/    /' || true
  fi
}

# The SQL side starts first and runs long: a beeline INSERT spends tens of seconds planning and
# running a MapReduce job before it reaches its commit, while a REST append commits a second or
# two after it starts. Firing both at once therefore proves nothing - the REST side is done
# before SQL even reaches its commit. So the REST appends run in rounds for as long as any SQL
# writer is still alive, which is what makes the two commit windows actually cross.
sql_pids=()
for ((i = 1; i <= SQL_WRITERS; i++)); do
  sql_insert "$((900 + i))" > "${LOG_DIR}/sql${i}.log" 2>&1 &
  sql_pids+=($!)
done

sql_still_running() {
  local pid
  for pid in ${sql_pids[@]+"${sql_pids[@]}"}; do
    kill -0 "${pid}" 2>/dev/null && return 0
  done
  return 1
}

round=0
rest_total=0
while :; do
  round=$((round + 1))
  pids=()
  names=()
  for ((i = 1; i <= WRITERS; i++)); do
    rest_total=$((rest_total + 1))
    name="w${rest_total}"
    writer append --rows 1 --marker "${name}" > "${LOG_DIR}/${name}.log" 2>&1 &
    pids+=($!)
    names+=("${name}")
  done
  for i in "${!pids[@]}"; do
    if wait "${pids[$i]}"; then count_result "${names[$i]}" 0; else count_result "${names[$i]}" 1; fi
  done
  sql_still_running || break
  # Safety cap: a wedged SQL query must not turn this into an unbounded loop.
  [[ "${round}" -ge 20 ]] && { log "stopping REST rounds after ${round}; the SQL side is still running"; break; }
done
log "${rest_total} REST append(s) in ${round} round(s)"

for i in "${!sql_pids[@]}"; do
  if wait "${sql_pids[$i]}"; then count_result "sql$((i + 1))" 0; else count_result "sql$((i + 1))" 1; fi
done
log "${succeeded} writer(s) reported success, ${failed} failed"

# A CommitStateUnknownException means the writer does not know whether its commit landed, so the
# row count is legitimately ambiguous - anything else is a hard answer.
if grep -rl 'CommitStateUnknownException' "${LOG_DIR}" >/dev/null 2>&1; then
  fail "a writer reported CommitStateUnknownException; the row count cannot be judged"
fi

expected=$((succeeded + 1))
count="$(writer count | sed -n 's/^rows=\([0-9]*\)$/\1/p')"
[[ -n "${count}" ]] || fail "could not read the row count back"
if [[ "${count}" -gt "${expected}" ]]; then
  # More rows than writers that reported success: a writer was told its commit failed and the
  # rows landed anyway. Not data loss - the opposite - but a client that retries such a write
  # duplicates it, so the run says so instead of passing quietly.
  log "WARNING: ${count} rows for ${succeeded} successful writer(s) plus the baseline - a writer"
  log "         that reported failure committed anyway; a retry of that write would duplicate it"
elif [[ "${count}" != "${expected}" ]]; then
  # Name the writer whose row is missing: every writer tags its rows with its own marker, so a
  # group-by says immediately whether the REST side or the SQL side lost one.
  if [[ "${SQL_WRITERS}" -gt 0 ]]; then
    log "rows present, by writer marker:"
    beeline_query "select src, count(*) from ${TABLE} group by src;" | sed 's/^/    /' || true
  fi
  fail "lost update: ${succeeded} writer(s) committed successfully on top of 1 baseline row, so the table must hold ${expected} rows, but it holds ${count}"
fi
log "no lost update: ${count} rows for 1 baseline + ${succeeded} successful writer(s)"

[[ "${succeeded}" -ge 2 ]] \
  || fail "only ${succeeded} writer(s) succeeded - the run proves nothing about concurrency; check the proxy log"

# The row count alone does not prove cross-path contention: if every REST append finished before
# the first SQL commit started, the invariant holds trivially. So require the two sides' commit
# windows to actually overlap. Each Iceberg commit ends in an alter_table on the metastore, and
# the proxy logs the thread it ran on - REST requests on hms-proxy-rest-*, Thrift (SQL) requests
# on the pool-*-thread-* workers - which is enough to bracket each side in time.
if [[ "${SQL_WRITERS}" -gt 0 ]]; then
  commits="$(docker logs stand-proxy --since "${LOG_SINCE}" 2>&1 \
    | grep -E 'alter_table_with_environment_context' \
    | grep -oE '[0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3}|hms-proxy-rest-[0-9]+|pool-[0-9]+-thread-[0-9]+' \
    | paste - - || true)"
  window="$(awk '
    function ms(t) {
      split(t, p, /[:,]/)
      return ((p[1] * 60 + p[2]) * 60 + p[3]) * 1000 + p[4]
    }
    $2 ~ /^hms-proxy-rest-/ {
      t = ms($1); if (rmin == "" || t < rmin) rmin = t; if (t > rmax) rmax = t; next
    }
    $2 ~ /^pool-/ {
      t = ms($1); if (smin == "" || t < smin) smin = t; if (t > smax) smax = t
    }
    END { print (rmin == "" ? -1 : rmin), (rmax == "" ? -1 : rmax), (smin == "" ? -1 : smin), (smax == "" ? -1 : smax) }
  ' <<< "${commits}")"
  read -r rest_min rest_max sql_min sql_max <<< "${window}"
  [[ "${rest_min}" != "-1" ]] || fail "no REST commit found in the proxy log; cannot judge overlap"
  [[ "${sql_min}" != "-1" ]] \
    || fail "no SQL commit found in the proxy log, so nothing was proven about REST-versus-SQL contention"
  if [[ "${rest_min}" -gt "${sql_max}" || "${sql_min}" -gt "${rest_max}" ]]; then
    fail "the REST and SQL commit windows did not overlap (REST ${rest_min}-${rest_max} ms, SQL ${sql_min}-${sql_max} ms), so this run proves nothing about cross-path contention"
  fi
  log "commit windows overlapped: REST spans $((rest_max - rest_min)) ms, SQL spans $((sql_max - sql_min)) ms, and they intersect"
fi

code="$(rest_curl -o /dev/null -w '%{http_code}' -X DELETE \
  "$(rest_url)/v1/${PREFIX}/namespaces/${NS}/tables/${TABLE}?purgeRequested=true")"
[[ "${code}" =~ ^2 ]] || fail "REST purge-drop of '${TABLE}' returned HTTP ${code}"
docker exec "${NAMENODE}" hdfs dfs -rm -r -f "/warehouse/${PREFIX}/${TABLE}" >/dev/null 2>&1 \
  || fail "could not remove /warehouse/${PREFIX}/${TABLE} on ${NAMENODE}"

log "writer-isolation smoke passed (auth=${AUTH}, catalog '${PREFIX}', ${WRITERS} concurrent writers)"
