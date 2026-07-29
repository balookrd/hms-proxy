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
NAMENODE=${CONCURRENCY_NAMENODE:-}
while [[ $# -gt 0 ]]; do
  case "$1" in
    --kerberos) AUTH=kerberos; shift ;;
    --prefix) [[ $# -ge 2 ]] || { echo "missing value for --prefix" >&2; exit 1; }; PREFIX="$2"; shift 2 ;;
    --writers) [[ $# -ge 2 ]] || { echo "missing value for --writers" >&2; exit 1; }; WRITERS="$2"; shift 2 ;;
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

if [[ "${AUTH}" == "kerberos" ]]; then
  docker exec stand-proxy kinit -kt /keytabs/smoke-user.keytab smoke-user@SMOKE.LOCAL \
    || fail "kinit failed in stand-proxy"
fi

log "preparing '${NS}.${TABLE}' on catalog '${PREFIX}' with one baseline row"
writer drop --purge >/dev/null 2>&1 || true
writer create >/dev/null
writer append --rows 1 --marker baseline >/dev/null

log "launching ${WRITERS} concurrent appends"
pids=()
for ((i = 1; i <= WRITERS; i++)); do
  writer append --rows 1 --marker "w${i}" > "${LOG_DIR}/w${i}.log" 2>&1 &
  pids+=($!)
done

succeeded=0
failed=0
for i in "${!pids[@]}"; do
  if wait "${pids[$i]}"; then
    succeeded=$((succeeded + 1))
  else
    failed=$((failed + 1))
    log "writer $((i + 1)) failed (that is allowed - a commit may legitimately run out of retries):"
    grep -oE '[A-Za-z.]*(CommitFailedException|ValidationException|CommitStateUnknownException)[^"]*' \
      "${LOG_DIR}/w$((i + 1)).log" | head -1 | sed 's/^/    /' || true
  fi
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
[[ "${count}" == "${expected}" ]] \
  || fail "lost update: ${succeeded} writer(s) committed successfully on top of 1 baseline row, so the table must hold ${expected} rows, but it holds ${count}"
log "no lost update: ${count} rows for 1 baseline + ${succeeded} successful writer(s)"

[[ "${succeeded}" -ge 2 ]] \
  || fail "only ${succeeded} writer(s) succeeded - the run proves nothing about concurrency; check the proxy log"

code="$(rest_curl -o /dev/null -w '%{http_code}' -X DELETE \
  "$(rest_url)/v1/${PREFIX}/namespaces/${NS}/tables/${TABLE}?purgeRequested=true")"
[[ "${code}" =~ ^2 ]] || fail "REST purge-drop of '${TABLE}' returned HTTP ${code}"
docker exec "${NAMENODE}" hdfs dfs -rm -r -f "/warehouse/${PREFIX}/${TABLE}" >/dev/null 2>&1 || true

log "writer-isolation smoke passed (auth=${AUTH}, catalog '${PREFIX}', ${WRITERS} concurrent writers)"
