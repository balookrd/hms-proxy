#!/usr/bin/env bash
# Multi-table transaction under contention: what happens to a two-table
# POST /v1/{prefix}/transactions/commit when a competing writer has moved one of the two tables
# out from under it?
#
# Clients can reasonably read "transaction" as "all or nothing", so the answer has to be measured
# rather than assumed. The contention here is real, not injected: a second writer appends to one
# of the two tables through the same front door, which advances that table's `main` ref, and the
# transaction then arrives carrying the snapshot id it read before that append. No timing games
# are needed - the stale requirement is what a losing racer would have sent.
#
# Two cases, and the second is what keeps the first honest:
#   negative - the transaction carries a stale `assert-ref-snapshot-id` for table B. It must be
#              refused, and *neither* table may end up with the property it tried to set.
#   positive - the same transaction with B's current snapshot id must be accepted and set the
#              property on both tables. Without this the negative case would also pass on a
#              malformed body, a wrong URL or a table that is not writable at all.
#
#   smoke-stand/run-iceberg-txn-contention-smoke.sh --prefix hive4
#   smoke-stand/run-iceberg-txn-contention-smoke.sh --prefix hdp --kerberos
set -euo pipefail

AUTH=plain
PREFIX=${TXN_CONTENTION_PREFIX:-hive4}
while [[ $# -gt 0 ]]; do
  case "$1" in
    --kerberos) AUTH=kerberos; shift ;;
    --prefix) [[ $# -ge 2 ]] || { echo "missing value for --prefix" >&2; exit 1; }; PREFIX="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 1 ;;
  esac
done

NS=${TXN_CONTENTION_NAMESPACE:-default}
TABLE_A=${TXN_CONTENTION_TABLE_A:-smoke_iceberg_txn_a}
TABLE_B=${TXN_CONTENTION_TABLE_B:-smoke_iceberg_txn_b}
REST_HOST_URL=${TXN_CONTENTION_REST_URL:-http://localhost:19183}
REST_NET_URL=http://proxy:9183
WRITER_JAR=/opt/hms-proxy/iceberg-rest-writer.jar
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/hms-txn-contention.XXXXXX")"
trap 'rm -rf "${WORK_DIR}"' EXIT

fail() {
  printf '[iceberg-txn-contention] error: %s\n' "$*" >&2
  exit 1
}

log() {
  printf '[iceberg-txn-contention] %s\n' "$*"
}

writer() {
  local table="$1"; shift
  local -a auth_args=()
  if [[ "${AUTH}" == "kerberos" ]]; then
    auth_args+=(--keytab /keytabs/smoke-user.keytab --principal smoke-user@SMOKE.LOCAL)
    auth_args+=(--rest-auth spnego)
  fi
  docker exec stand-proxy java \
    --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
    --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
    -jar "${WRITER_JAR}" "$@" \
    --uri "${REST_NET_URL}" --namespace "${NS}" --table "${table}" \
    ${auth_args[@]+"${auth_args[@]}"} 2>&1 | grep -vE '^SLF4J' || true
}

# Under Kerberos curl runs inside stand-proxy - the KDC and the `proxy` hostname only resolve
# in-network - so nothing here may name a host path: the body comes back on stdout and the caller
# writes it out, rather than curl's -o doing it.
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

# Writes the response body to $1 and prints the HTTP status code.
rest_request() {
  local out="$1"; shift
  local response
  response="$(rest_curl -w '\n%{http_code}' "$@")"
  printf '%s' "${response%$'\n'*}" > "${out}"
  printf '%s' "${response##*$'\n'}"
}

load_table() {
  local out="$1" table="$2"
  rest_request "${out}" "$(rest_url)/v1/${PREFIX}/namespaces/${NS}/tables/${table}"
}

# sed -E, not plain sed: BSD sed does not read `\?` as "optional", so a BRE written for GNU sed
# silently matches nothing here and the field comes back with its own name still attached.
json_field() {
  grep -o "\"$2\"[[:space:]]*:[[:space:]]*\"\?[^,\"}]*\"\?" "$1" | head -n 1 \
    | sed -E 's/^"[^"]*"[[:space:]]*:[[:space:]]*"?//; s/"$//'
}

# The transaction body: table A gated on its uuid (a requirement that always holds), table B on
# the snapshot id passed in - the one knob the scenario turns between its two cases.
txn_body() {
  local uuid_a="$1" snapshot_b="$2"
  printf '{"table-changes":[{"identifier":{"namespace":["%s"],"name":"%s"},' "${NS}" "${TABLE_A}"
  printf '"requirements":[{"type":"assert-table-uuid","uuid":"%s"}],' "${uuid_a}"
  printf '"updates":[{"action":"set-properties","updates":{"txn":"applied"}}]},'
  printf '{"identifier":{"namespace":["%s"],"name":"%s"},' "${NS}" "${TABLE_B}"
  printf '"requirements":[{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":%s}],' "${snapshot_b}"
  printf '"updates":[{"action":"set-properties","updates":{"txn":"applied"}}]}]}'
}

# The body is passed inline rather than through --data @file for the same reason: under Kerberos
# curl's filesystem is the container's, not this script's.
commit_txn() {
  local out="$1" uuid_a="$2" snapshot_b="$3"
  rest_request "${out}" -X POST -H 'Content-Type: application/json' \
    --data "$(txn_body "${uuid_a}" "${snapshot_b}")" \
    "$(rest_url)/v1/${PREFIX}/transactions/commit"
}

has_applied_property() {
  local out="${WORK_DIR}/check.json" code
  code="$(load_table "${out}" "$1")"
  [[ "${code}" == "200" ]] || fail "load of '$1' returned HTTP ${code}: $(cat "${out}")"
  grep -q '"txn"[[:space:]]*:[[:space:]]*"applied"' "${out}"
}

row_count() {
  writer "$1" count | grep -oE 'rows=[0-9]+' | tail -n 1 | cut -d= -f2
}

# curl --negotiate mints its token from the ticket cache, which a recreated container does not
# have; doing the kinit here rather than expecting one keeps the scenario runnable on its own.
if [[ "${AUTH}" == "kerberos" ]]; then
  docker exec stand-proxy kinit -kt /keytabs/smoke-user.keytab smoke-user@SMOKE.LOCAL \
    || fail "kinit for smoke-user inside stand-proxy failed"
fi

log "preparing '${NS}.${TABLE_A}' and '${NS}.${TABLE_B}' on catalog '${PREFIX}'"
for table in "${TABLE_A}" "${TABLE_B}"; do
  writer "${table}" drop --purge >/dev/null 2>&1 || true
  writer "${table}" create >/dev/null
  writer "${table}" append --rows 2 --marker base >/dev/null
done

code="$(load_table "${WORK_DIR}/a.json" "${TABLE_A}")"
[[ "${code}" == "200" ]] || fail "load of '${TABLE_A}' returned HTTP ${code}: $(cat "${WORK_DIR}/a.json")"
uuid_a="$(json_field "${WORK_DIR}/a.json" table-uuid)"
[[ -n "${uuid_a}" ]] || fail "no table-uuid for '${TABLE_A}': $(cat "${WORK_DIR}/a.json")"
code="$(load_table "${WORK_DIR}/b.json" "${TABLE_B}")"
[[ "${code}" == "200" ]] || fail "load of '${TABLE_B}' returned HTTP ${code}: $(cat "${WORK_DIR}/b.json")"
stale_snapshot_b="$(json_field "${WORK_DIR}/b.json" current-snapshot-id)"
[[ "${stale_snapshot_b}" =~ ^-?[0-9]+$ ]] \
  || fail "no current-snapshot-id for '${TABLE_B}': $(cat "${WORK_DIR}/b.json")"
log "read '${TABLE_B}' at snapshot ${stale_snapshot_b}"

# The contention: a competing writer commits to B through the same front door, so the snapshot the
# transaction is about to assert on is no longer B's current one.
log "competing writer appends to '${TABLE_B}'"
writer "${TABLE_B}" append --rows 3 --marker competitor >/dev/null
load_table "${WORK_DIR}/b.json" "${TABLE_B}" >/dev/null
fresh_snapshot_b="$(json_field "${WORK_DIR}/b.json" current-snapshot-id)"
[[ "${fresh_snapshot_b}" != "${stale_snapshot_b}" ]] \
  || fail "the competing append did not move '${TABLE_B}' off snapshot ${stale_snapshot_b}, so there is no contention to test"
log "'${TABLE_B}' moved to snapshot ${fresh_snapshot_b}"

log "case 1 (negative): committing both tables with the stale snapshot for '${TABLE_B}'"
code="$(commit_txn "${WORK_DIR}/txn.json" "${uuid_a}" "${stale_snapshot_b}")"
[[ "${code}" != "204" ]] \
  || fail "a transaction whose requirement for '${TABLE_B}' is stale answered 204; a lost update went unreported"
log "refused with HTTP ${code}: $(head -c 300 "${WORK_DIR}/txn.json")"

if has_applied_property "${TABLE_B}"; then
  fail "'${TABLE_B}' carries the property of a refused transaction"
fi
applied_to_a=no
if has_applied_property "${TABLE_A}"; then
  applied_to_a=yes
fi

rows_b="$(row_count "${TABLE_B}")"
[[ "${rows_b}" == "5" ]] \
  || fail "the competing writer's rows must survive the refused transaction: expected 5 in '${TABLE_B}', got '${rows_b}'"

if [[ "${applied_to_a}" == "yes" ]]; then
  log "RECORDED: the route is NOT atomic - '${TABLE_A}' kept the update while '${TABLE_B}' was refused"
else
  log "RECORDED: nothing was applied - the refusal left both tables untouched"
fi

# Without this the negative case above proves nothing: a malformed body, a wrong prefix or a
# read-only table would refuse the transaction just as convincingly.
log "case 2 (positive control): the same transaction with '${TABLE_B}' at its current snapshot"
writer "${TABLE_A}" drop --purge >/dev/null 2>&1 || true
writer "${TABLE_A}" create >/dev/null
writer "${TABLE_A}" append --rows 2 --marker base >/dev/null
load_table "${WORK_DIR}/a.json" "${TABLE_A}" >/dev/null
uuid_a="$(json_field "${WORK_DIR}/a.json" table-uuid)"

code="$(commit_txn "${WORK_DIR}/txn.json" "${uuid_a}" "${fresh_snapshot_b}")"
[[ "${code}" == "204" ]] \
  || fail "the positive control must be accepted, got HTTP ${code}: $(cat "${WORK_DIR}/txn.json")"
has_applied_property "${TABLE_A}" \
  || fail "the accepted transaction did not set the property on '${TABLE_A}'"
has_applied_property "${TABLE_B}" \
  || fail "the accepted transaction did not set the property on '${TABLE_B}'"
log "positive control accepted and applied to both tables"

log "cleaning up"
writer "${TABLE_A}" drop --purge >/dev/null 2>&1 || true
writer "${TABLE_B}" drop --purge >/dev/null 2>&1 || true

log "multi-table transaction contention smoke passed (auth=${AUTH}, catalog '${PREFIX}')"
