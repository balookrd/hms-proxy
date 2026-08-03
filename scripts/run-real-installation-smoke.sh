#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
CLI_CLASS="io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli"
DEFAULT_ENV_FILE="${HMS_SMOKE_DEFAULT_ENV_FILE:-${SCRIPT_DIR}/hms-real-installation-smoke.env}"
RUNNER_NAME="${HMS_SMOKE_RUNNER_NAME:-scripts/run-real-installation-smoke.sh}"
AUTH_OVERRIDE="${HMS_SMOKE_AUTH_OVERRIDE:-}"

SCENARIO="all"
ENV_FILE=""

usage() {
  cat <<EOF
Usage:
  ${RUNNER_NAME} [--env-file /path/to/file.env] [--scenario all|sql|txn|locks|notification|rest]

Behavior:
  - loads HMS_SMOKE_* settings from --env-file or from ${DEFAULT_ENV_FILE} when present
  - falls back to current environment variables if no env-file is provided
  - uses target/hms-proxy-*-fat.jar unless HMS_SMOKE_FAT_JAR is set
  - exits on the first failed smoke step

Scenarios:
  all           run optional beeline SQL smoke + txn + non-default DB lock + optional partition lock + optional notification + optional Iceberg REST smoke
  sql           run only beeline / HiveServer2 SQL smoke from SMOKE.md
  txn           run only the direct ACID/txn smoke
  locks         run only the non-default catalog lock smoke
  notification  run only Hortonworks add_write_notification_log smoke
  rest          run only the Iceberg REST catalog smoke (HTTP, via curl)

Important env vars:
  HMS_SMOKE_URI
EOF

  if [[ "${AUTH_OVERRIDE}" == "simple" ]]; then
    cat <<'EOF'
  HMS_SMOKE_AUTH=simple                  fixed by this runner
EOF
  elif [[ "${AUTH_OVERRIDE}" == "kerberos" ]]; then
    cat <<'EOF'
  HMS_SMOKE_AUTH=kerberos               fixed by this runner
EOF
  else
    cat <<'EOF'
  HMS_SMOKE_AUTH=simple|kerberos
EOF
  fi

  cat <<'EOF'
  HMS_SMOKE_TXN_DB
  HMS_SMOKE_TXN_TABLE
  HMS_SMOKE_LOCK_DB

Optional notification env vars:
  HMS_SMOKE_NOTIFICATION_URI             front door exposing the Hortonworks interface,
                                         when it is not HMS_SMOKE_URI
  HMS_SMOKE_NOTIFICATION_DB
  HMS_SMOKE_NOTIFICATION_TABLE
  HMS_SMOKE_NOTIFICATION_NEGATIVE_DB
  HMS_SMOKE_NOTIFICATION_NEGATIVE_TABLE
  HMS_SMOKE_NOTIFICATION_TXN_ID
  HMS_SMOKE_NOTIFICATION_WRITE_ID
  HMS_SMOKE_NOTIFICATION_FILES_ADDED     semicolon-separated
  HMS_SMOKE_HDP_STANDALONE_METASTORE_JAR

Optional partition lock env vars:
  HMS_SMOKE_LOCK_TABLE
  HMS_SMOKE_LOCK_PARTITION

Optional Iceberg REST env vars:
  HMS_SMOKE_REST_URL                     base URL of the rest-catalog listener; enables the smoke
  HMS_SMOKE_REST_PREFIX                  expected catalog prefix; default: whatever /v1/config advertises
  HMS_SMOKE_REST_NAMESPACE               default: default
  HMS_SMOKE_REST_ICEBERG_TABLE           Iceberg table to list and load; skipped when unset
  HMS_SMOKE_REST_NON_ICEBERG_TABLE       plain Hive table that must NOT appear in the listing
  HMS_SMOKE_REST_SECOND_PREFIX           non-default catalog prefix; enables warehouse discovery
                                         and clean-view checks under it; skipped when unset
  HMS_SMOKE_REST_SECOND_ICEBERG_TABLE    Iceberg table under the second prefix to load (also
                                         listed and loaded through the federated name under the
                                         default prefix); skipped when unset
  HMS_SMOKE_REST_SECOND_NON_ICEBERG_TABLE  plain Hive table of the second catalog that must NOT
                                         appear in its REST listing
  HMS_SMOKE_REST_SEPARATOR               catalog-db separator of the proxy; default: __
  HMS_SMOKE_REST_METRICS_URL             management /metrics endpoint; when set, the REST smoke
                                         checks it carries hms_proxy_rest_requests_total and
                                         hms_proxy_rest_listener_info series
  HMS_SMOKE_REST_CURL_OPTS               extra curl options for every REST request, e.g.
                                         "--negotiate -u :" on a SPNEGO-protected listener; when
                                         set, the smoke also asserts a request WITHOUT these
                                         options is rejected 401 with a Negotiate challenge and
                                         an empty body
  HMS_SMOKE_REST_WRITE_TABLE             table name to create, commit, rename and drop through
                                         the REST write routes; skipped when unset. Also gates a
                                         namespace DDL round trip (create/load/update-property/
                                         drop of HMS_SMOKE_REST_WRITE_NAMESPACE), a view round
                                         trip (create/list/drop of HMS_SMOKE_REST_WRITE_VIEW in
                                         HMS_SMOKE_REST_NAMESPACE), a multi-table transaction
                                         commit round trip (create "<name>_txn", commit through
                                         POST /v1/{prefix}/transactions/commit, assert its
                                         metadata-location changed, drop) and a register round
                                         trip (create "<name>_reg", non-purge drop, re-register
                                         from the surviving metadata file, load, drop) - the same
                                         default-catalog restriction applies to every one of
                                         these routes.
                                         Writes only work when the advertised default-prefix
                                         catalog resolves to the real backend (non-default
                                         catalogs are served by the synthetic lock shim and refuse
                                         writes with 403). All three objects (the table, the
                                         namespace and the view) are created and destroyed by this
                                         smoke run itself. The block drops leftovers under every
                                         name it can create defensively before creating, so a
                                         rerun on a dirty stand cannot half-fail; the namespace
                                         drop only goes through when the namespace does not exist
                                         yet or is already empty (no tables, no views), so a
                                         pre-existing namespace of the same name on a real
                                         installation is refused rather than silently destroyed.
                                         Requires HMS_SMOKE_REST_SECOND_PREFIX to also exercise
                                         the gate negatives - one per gated write route:
                                         CREATE_TABLE under that prefix and under its federated
                                         name, COMMIT_TRANSACTION naming a federated table,
                                         CREATE/UPDATE/DROP_NAMESPACE with a federated name,
                                         UPDATE/DROP/REGISTER_TABLE and CREATE/UPDATE/DROP_VIEW
                                         under a federated namespace, and table/view RENAME with
                                         a federated destination - all expected 403
  HMS_SMOKE_REST_WRITE_NAMESPACE         namespace created/destroyed by the write round trip
                                         above; default: smoke_rest_ns
  HMS_SMOKE_REST_WRITE_VIEW              view created/destroyed by the write round trip above,
                                         in HMS_SMOKE_REST_NAMESPACE; default: smoke_rest_view
EOF

  cat <<'EOF'

Optional beeline / SQL env vars:
  HMS_SMOKE_BEELINE_JDBC_URL
  HMS_SMOKE_BEELINE_BIN                  default: beeline
  HMS_SMOKE_BEELINE_USER                 optional
  HMS_SMOKE_BEELINE_PASSWORD             optional
  HMS_SMOKE_BEELINE_HDP_JDBC_URL         a HiveServer2 on the Hortonworks front door; when set,
                                         the whole SQL suite runs a second time against it
  HMS_SMOKE_BEELINE_HDP_BIN              default: HMS_SMOKE_BEELINE_BIN
  HMS_SMOKE_BEELINE_HDP_USER             default: HMS_SMOKE_BEELINE_USER
  HMS_SMOKE_SQL_SESSION_INIT             statements run before the suite, e.g. a `set`
  HMS_SMOKE_SQL_HDP_SESSION_INIT         same, for the Hortonworks pass only
  HMS_SMOKE_HDP_CATALOG                  default: hdp
  HMS_SMOKE_APACHE_CATALOG               default: apache
  HMS_SMOKE_HDP_READ_TABLE               required for SQL smoke
  HMS_SMOKE_APACHE_READ_TABLE            required for SQL smoke
  HMS_SMOKE_SQL_EXTERNAL_ROOT            default: /tmp/hms-proxy-smoke
  HMS_SMOKE_HDP_EXTERNAL_ROOT            default: ${HMS_SMOKE_SQL_EXTERNAL_ROOT}/${HMS_SMOKE_HDP_CATALOG}
  HMS_SMOKE_APACHE_EXTERNAL_ROOT         default: ${HMS_SMOKE_SQL_EXTERNAL_ROOT}/${HMS_SMOKE_APACHE_CATALOG}
  HMS_SMOKE_SQL_RUN_VIEW_REWRITE         default: true
  HMS_SMOKE_SQL_RUN_UDF                  default: true
  HMS_SMOKE_SQL_UDF_CLASS                default: org.apache.hadoop.hive.ql.udf.UDFReverse
  HMS_SMOKE_SQL_UDF_EXPECTED_RESULT      default: yxorp
  HMS_SMOKE_SQL_RUN_MATERIALIZED_VIEW    default: false
  HMS_SMOKE_HDP_RUN_TRANSACTIONAL_SQL    default: false
  HMS_SMOKE_APACHE_RUN_TRANSACTIONAL_SQL default: false
  HMS_SMOKE_SQL_FRONT_DOORS              default: apache hdp (which passes run)
  HMS_SMOKE_TRANSACTIONAL_SQL_FRONT_DOORS default: hdp (space-separated: apache hdp)
EOF

  if [[ "${AUTH_OVERRIDE}" != "simple" ]]; then
    cat <<'EOF'

Kerberos-only env vars:
  HMS_SMOKE_SERVER_PRINCIPAL
  HMS_SMOKE_CLIENT_PRINCIPAL
  HMS_SMOKE_KEYTAB
  HMS_SMOKE_KRB5_CONF                    optional
  HMS_SMOKE_BEELINE_KINIT                default: true when SQL smoke is enabled
EOF
  fi
}

fail() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

log() {
  printf '[hms-smoke] %s\n' "$*"
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"
}

trim() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

strip_wrapping_quotes() {
  local value="$1"
  if [[ "${value}" == \"*\" && "${value}" == *\" ]]; then
    value="${value:1:${#value}-2}"
  elif [[ "${value}" == \'*\' && "${value}" == *\' ]]; then
    value="${value:1:${#value}-2}"
  fi
  printf '%s' "$value"
}

load_env_file() {
  local file="$1"
  local line=""
  local line_no=0

  [[ -f "${file}" ]] || fail "env file not found: ${file}"

  while IFS= read -r line || [[ -n "${line}" ]]; do
    line_no=$((line_no + 1))
    line="$(trim "${line}")"
    [[ -z "${line}" || "${line:0:1}" == "#" ]] && continue
    [[ "${line}" == *=* ]] || fail "invalid env line ${line_no} in ${file}: ${line}"

    local key
    local value
    key="$(trim "${line%%=*}")"
    value="$(trim "${line#*=}")"
    value="$(strip_wrapping_quotes "${value}")"

    [[ "${key}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || fail "invalid env key '${key}' in ${file}:${line_no}"
    printf -v "${key}" '%s' "${value}"
    export "${key}"
  done < "${file}"
}

require_var() {
  local name="$1"
  local value="${!name:-}"
  [[ -n "${value}" ]] || fail "missing required setting ${name}"
}

split_semicolon_list() {
  local raw="$1"
  local old_ifs="${IFS}"
  local item=""
  local -a raw_items=()
  local -a result=()
  IFS=';'
  read -r -a raw_items <<< "${raw}"
  IFS="${old_ifs}"
  for item in "${raw_items[@]}"; do
    item="$(trim "${item}")"
    [[ -n "${item}" ]] && result+=("${item}")
  done
  printf '%s\n' "${result[@]}"
}

resolve_fat_jar() {
  if [[ -n "${HMS_SMOKE_FAT_JAR:-}" ]]; then
    [[ -f "${HMS_SMOKE_FAT_JAR}" ]] || fail "fat jar not found: ${HMS_SMOKE_FAT_JAR}"
    return
  fi

  local latest_jar=""
  latest_jar="$(ls -t "${ROOT_DIR}"/target/hms-proxy-*-fat.jar 2>/dev/null | head -n 1 || true)"
  [[ -n "${latest_jar}" ]] || fail "fat jar not found under target/. Run 'mvn -DskipTests package' first or set HMS_SMOKE_FAT_JAR"
  HMS_SMOKE_FAT_JAR="${latest_jar}"
  export HMS_SMOKE_FAT_JAR
}

build_java_cmd() {
  JAVA_CMD=("${HMS_SMOKE_JAVA_BIN:-java}")
  JAVA_CMD+=("--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED")
  JAVA_CMD+=("--add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED")
}

build_common_args() {
  require_var HMS_SMOKE_URI
  local auth="${HMS_SMOKE_AUTH:-${AUTH_OVERRIDE:-simple}}"
  if [[ -n "${AUTH_OVERRIDE}" && -n "${HMS_SMOKE_AUTH:-}" && "${HMS_SMOKE_AUTH}" != "${AUTH_OVERRIDE}" ]]; then
    fail "runner requires HMS_SMOKE_AUTH=${AUTH_OVERRIDE}, got ${HMS_SMOKE_AUTH}"
  fi
  HMS_SMOKE_AUTH="${auth}"
  export HMS_SMOKE_AUTH
  COMMON_ARGS=("--uri" "${HMS_SMOKE_URI}" "--auth" "${auth}")

  if [[ "${auth}" == "kerberos" ]]; then
    require_var HMS_SMOKE_SERVER_PRINCIPAL
    require_var HMS_SMOKE_CLIENT_PRINCIPAL
    require_var HMS_SMOKE_KEYTAB
    COMMON_ARGS+=("--server-principal" "${HMS_SMOKE_SERVER_PRINCIPAL}")
    COMMON_ARGS+=("--client-principal" "${HMS_SMOKE_CLIENT_PRINCIPAL}")
    COMMON_ARGS+=("--keytab" "${HMS_SMOKE_KEYTAB}")
    if [[ -n "${HMS_SMOKE_KRB5_CONF:-}" ]]; then
      COMMON_ARGS+=("--krb5-conf" "${HMS_SMOKE_KRB5_CONF}")
    fi
  elif [[ "${auth}" != "simple" ]]; then
    fail "unsupported HMS_SMOKE_AUTH value: ${auth}"
  fi

  if [[ -n "${HMS_SMOKE_EXTRA_CONF:-}" ]]; then
    while IFS= read -r conf_entry; do
      [[ -n "${conf_entry}" ]] && COMMON_ARGS+=("--conf" "${conf_entry}")
    done < <(split_semicolon_list "${HMS_SMOKE_EXTRA_CONF}")
  fi
}

run_txn_smoke_target() {
  local label="$1"
  local db="$2"
  local table="$3"

  local -a args=()
  args+=("--db" "${db}")
  args+=("--table" "${table}")
  args+=("--lock" "${HMS_SMOKE_TXN_LOCK:-true}")
  if [[ -n "${HMS_SMOKE_USER:-}" ]]; then
    args+=("--user" "${HMS_SMOKE_USER}")
  fi
  if [[ -n "${HMS_SMOKE_HOST:-}" ]]; then
    args+=("--host" "${HMS_SMOKE_HOST}")
  fi
  if [[ -n "${HMS_SMOKE_AGENT_INFO:-}" ]]; then
    args+=("--agent-info" "${HMS_SMOKE_AGENT_INFO}")
  fi
  if [[ -n "${HMS_SMOKE_TXN_VALID_TXN_LIST:-}" ]]; then
    args+=("--valid-txn-list" "${HMS_SMOKE_TXN_VALID_TXN_LIST}")
  fi

  run_cli "${label}" "txn" "${args[@]}"
}

run_cli() {
  local label="$1"
  shift
  local mode="$1"
  shift

  local -a cmd=()
  cmd=("${JAVA_CMD[@]}" "-cp" "${HMS_SMOKE_FAT_JAR}" "${CLI_CLASS}" "${mode}" "${COMMON_ARGS[@]}" "$@")

  log "running ${label}"
  printf '  %q' "${cmd[@]}"
  printf '\n'
  "${cmd[@]}"
}

run_txn_smoke() {
  require_var HMS_SMOKE_TXN_DB
  require_var HMS_SMOKE_TXN_TABLE
  run_txn_smoke_target "txn smoke primary" "${HMS_SMOKE_TXN_DB}" "${HMS_SMOKE_TXN_TABLE}"

  if [[ -n "${HMS_SMOKE_TXN_SECONDARY_DB:-}" || -n "${HMS_SMOKE_TXN_SECONDARY_TABLE:-}" ]]; then
    require_var HMS_SMOKE_TXN_SECONDARY_DB
    require_var HMS_SMOKE_TXN_SECONDARY_TABLE
    run_txn_smoke_target \
      "txn smoke secondary" \
      "${HMS_SMOKE_TXN_SECONDARY_DB}" \
      "${HMS_SMOKE_TXN_SECONDARY_TABLE}"
  fi
}

run_db_lock_smoke() {
  require_var HMS_SMOKE_LOCK_DB

  local -a args=()
  args+=("--db" "${HMS_SMOKE_LOCK_DB}")
  args+=("--lock-type" "${HMS_SMOKE_DB_LOCK_TYPE:-SHARED_READ}")
  args+=("--lock-level" "${HMS_SMOKE_DB_LOCK_LEVEL:-DB}")
  args+=("--operation-type" "${HMS_SMOKE_DB_LOCK_OPERATION_TYPE:-NO_TXN}")
  args+=("--transactional" "${HMS_SMOKE_DB_LOCK_TRANSACTIONAL:-false}")
  args+=("--heartbeat" "${HMS_SMOKE_LOCK_HEARTBEAT:-true}")
  args+=("--unlock" "${HMS_SMOKE_LOCK_UNLOCK:-true}")
  args+=("--close-txn" "${HMS_SMOKE_LOCK_CLOSE_TXN:-abort}")
  if [[ -n "${HMS_SMOKE_USER:-}" ]]; then
    args+=("--user" "${HMS_SMOKE_USER}")
  fi
  if [[ -n "${HMS_SMOKE_HOST:-}" ]]; then
    args+=("--host" "${HMS_SMOKE_HOST}")
  fi
  if [[ -n "${HMS_SMOKE_AGENT_INFO:-}" ]]; then
    args+=("--agent-info" "${HMS_SMOKE_AGENT_INFO}")
  fi

  run_cli "non-default DB lock smoke" "lock" "${args[@]}"
}

# A lock request whose components name two catalogs - the shape Hive builds for any query reading
# across them. The proxy routes it by one catalog and drops the other components; this checks the
# call succeeds at all, which it did not before the request was split.
run_cross_catalog_lock_smoke() {
  local second_db="${HMS_SMOKE_CROSS_CATALOG_LOCK_DB:-}"
  if [[ -z "${second_db}" ]]; then
    log "skipping cross-catalog lock smoke because HMS_SMOKE_CROSS_CATALOG_LOCK_DB is not set"
    return
  fi

  require_var HMS_SMOKE_TXN_DB

  local -a args=()
  # The first component belongs to the default catalog, which owns the TxnHandler and keeps the
  # real lock; the second one is what the proxy has to drop rather than refuse.
  args+=("--db" "${HMS_SMOKE_TXN_DB}")
  args+=("--second-db" "${second_db}")
  if [[ -n "${HMS_SMOKE_CROSS_CATALOG_LOCK_TABLE:-}" ]]; then
    args+=("--second-table" "${HMS_SMOKE_CROSS_CATALOG_LOCK_TABLE}")
  fi
  args+=("--lock-type" "${HMS_SMOKE_CROSS_CATALOG_LOCK_TYPE:-SHARED_READ}")
  args+=("--lock-level" "${HMS_SMOKE_CROSS_CATALOG_LOCK_LEVEL:-DB}")
  args+=("--operation-type" "${HMS_SMOKE_CROSS_CATALOG_LOCK_OPERATION_TYPE:-NO_TXN}")
  args+=("--transactional" "${HMS_SMOKE_CROSS_CATALOG_LOCK_TRANSACTIONAL:-false}")
  args+=("--heartbeat" "${HMS_SMOKE_LOCK_HEARTBEAT:-true}")
  # Unlike the other lock scenarios this one keeps a real backend lock: the request routes by the
  # default catalog, whose TxnHandler owns it. A metastore refuses to unlock a lock that belongs to
  # a transaction ("Unlocking locks associated with transaction not permitted") - it is released by
  # closing the transaction instead.
  args+=("--unlock" "${HMS_SMOKE_CROSS_CATALOG_LOCK_UNLOCK:-false}")
  args+=("--close-txn" "${HMS_SMOKE_CROSS_CATALOG_LOCK_CLOSE_TXN:-abort}")
  if [[ -n "${HMS_SMOKE_USER:-}" ]]; then
    args+=("--user" "${HMS_SMOKE_USER}")
  fi
  if [[ -n "${HMS_SMOKE_HOST:-}" ]]; then
    args+=("--host" "${HMS_SMOKE_HOST}")
  fi
  if [[ -n "${HMS_SMOKE_AGENT_INFO:-}" ]]; then
    args+=("--agent-info" "${HMS_SMOKE_AGENT_INFO}")
  fi

  run_cli "cross-catalog lock smoke" "lock" "${args[@]}"
}

run_partition_lock_smoke() {
  local table="${HMS_SMOKE_LOCK_TABLE:-}"
  local partition="${HMS_SMOKE_LOCK_PARTITION:-}"
  if [[ -z "${table}" && -z "${partition}" ]]; then
    log "skipping partition lock smoke because HMS_SMOKE_LOCK_TABLE/HMS_SMOKE_LOCK_PARTITION are not set"
    return
  fi

  require_var HMS_SMOKE_LOCK_DB
  require_var HMS_SMOKE_LOCK_TABLE
  require_var HMS_SMOKE_LOCK_PARTITION

  local -a args=()
  args+=("--db" "${HMS_SMOKE_LOCK_DB}")
  args+=("--table" "${HMS_SMOKE_LOCK_TABLE}")
  args+=("--partition" "${HMS_SMOKE_LOCK_PARTITION}")
  args+=("--lock-type" "${HMS_SMOKE_PARTITION_LOCK_TYPE:-EXCLUSIVE}")
  args+=("--lock-level" "${HMS_SMOKE_PARTITION_LOCK_LEVEL:-PARTITION}")
  args+=("--operation-type" "${HMS_SMOKE_PARTITION_LOCK_OPERATION_TYPE:-NO_TXN}")
  args+=("--transactional" "${HMS_SMOKE_PARTITION_LOCK_TRANSACTIONAL:-false}")
  args+=("--heartbeat" "${HMS_SMOKE_LOCK_HEARTBEAT:-true}")
  args+=("--unlock" "${HMS_SMOKE_LOCK_UNLOCK:-true}")
  args+=("--close-txn" "${HMS_SMOKE_LOCK_CLOSE_TXN:-abort}")
  if [[ -n "${HMS_SMOKE_USER:-}" ]]; then
    args+=("--user" "${HMS_SMOKE_USER}")
  fi
  if [[ -n "${HMS_SMOKE_HOST:-}" ]]; then
    args+=("--host" "${HMS_SMOKE_HOST}")
  fi
  if [[ -n "${HMS_SMOKE_AGENT_INFO:-}" ]]; then
    args+=("--agent-info" "${HMS_SMOKE_AGENT_INFO}")
  fi

  run_cli "partition lock smoke" "lock" "${args[@]}"
}

notification_is_configured() {
  [[ -n "${HMS_SMOKE_NOTIFICATION_DB:-}" \
    || -n "${HMS_SMOKE_NOTIFICATION_TABLE:-}" \
    || -n "${HMS_SMOKE_NOTIFICATION_NEGATIVE_DB:-}" \
    || -n "${HMS_SMOKE_NOTIFICATION_NEGATIVE_TABLE:-}" \
    || -n "${HMS_SMOKE_NOTIFICATION_TXN_ID:-}" \
    || -n "${HMS_SMOKE_NOTIFICATION_WRITE_ID:-}" \
    || -n "${HMS_SMOKE_NOTIFICATION_FILES_ADDED:-}" \
    || -n "${HMS_SMOKE_HDP_STANDALONE_METASTORE_JAR:-}" ]]
}

run_notification_smoke_target() {
  local label="$1"
  local db="$2"
  local table="$3"
  local txn_id="$4"
  local write_id="$5"
  local expect_failure="${6:-false}"

  local -a args=()
  local file_added=""
  # add_write_notification_log only exists on a Hortonworks front door, which Thrift cannot
  # negotiate and which therefore usually listens on its own port. A repeated --uri wins over the
  # one in COMMON_ARGS, so the other scenarios keep using the primary front door.
  if [[ -n "${HMS_SMOKE_NOTIFICATION_URI:-}" ]]; then
    args+=("--uri" "${HMS_SMOKE_NOTIFICATION_URI}")
  fi
  args+=("--db" "${db}")
  args+=("--table" "${table}")
  args+=("--txn-id" "${txn_id}")
  args+=("--write-id" "${write_id}")
  args+=("--hdp-standalone-metastore-jar" "${HMS_SMOKE_HDP_STANDALONE_METASTORE_JAR}")
  while IFS= read -r file_added; do
    [[ -n "${file_added}" ]] && args+=("--files-added" "${file_added}")
  done < <(split_semicolon_list "${HMS_SMOKE_NOTIFICATION_FILES_ADDED}")

  if [[ -n "${HMS_SMOKE_NOTIFICATION_PARTITIONS:-}" ]]; then
    while IFS= read -r partition; do
      [[ -n "${partition}" ]] && args+=("--partition" "${partition}")
    done < <(split_semicolon_list "${HMS_SMOKE_NOTIFICATION_PARTITIONS}")
  fi

  local -a cmd=()
  cmd=("${JAVA_CMD[@]}" "-cp" "${HMS_SMOKE_FAT_JAR}" "${CLI_CLASS}" "notification" "${COMMON_ARGS[@]}" "${args[@]}")

  log "running ${label}"
  printf '  %q' "${cmd[@]}"
  printf '\n'

  if [[ "${expect_failure}" == "true" ]]; then
    local output=""
    set +e
    output="$("${cmd[@]}" 2>&1)"
    local status=$?
    set -e
    printf '%s\n' "${output}"
    [[ ${status} -ne 0 ]] || fail "${label} was expected to fail but succeeded"
    # add_write_notification_log declares no exceptions in the Hive IDL (3.1.x and 4.x alike), so
    # libthrift 0.9.3 replaces every server-side failure with a fixed "Internal error processing
    # <method>" message. A real HMS loses its own error texts the same way. The rejection reason
    # therefore only exists in the proxy log, where it reads "requires a Hortonworks backend
    # runtime"; the client can confirm no more than a refusal of this exact RPC.
    if grep -F "requires a Hortonworks backend runtime" <<< "${output}" >/dev/null; then
      return
    fi
    grep -F "Internal error processing add_write_notification_log" <<< "${output}" >/dev/null \
      || fail "${label} failed, but not with a refusal of add_write_notification_log"
    log "${label}: the proxy refused the RPC; check its log for 'requires a Hortonworks backend runtime'"
    return
  fi

  "${cmd[@]}"
}

run_notification_smoke() {
  if ! notification_is_configured; then
    if [[ "${SCENARIO}" == "notification" ]]; then
      fail "notification scenario requires HMS_SMOKE_NOTIFICATION_* settings"
    fi
    log "skipping notification smoke because HMS_SMOKE_NOTIFICATION_* is not configured"
    return
  fi

  require_var HMS_SMOKE_NOTIFICATION_DB
  require_var HMS_SMOKE_NOTIFICATION_TABLE
  require_var HMS_SMOKE_NOTIFICATION_TXN_ID
  require_var HMS_SMOKE_NOTIFICATION_WRITE_ID
  require_var HMS_SMOKE_NOTIFICATION_FILES_ADDED
  require_var HMS_SMOKE_HDP_STANDALONE_METASTORE_JAR

  run_notification_smoke_target \
    "notification smoke positive" \
    "${HMS_SMOKE_NOTIFICATION_DB}" \
    "${HMS_SMOKE_NOTIFICATION_TABLE}" \
    "${HMS_SMOKE_NOTIFICATION_TXN_ID}" \
    "${HMS_SMOKE_NOTIFICATION_WRITE_ID}" \
    "false"

  if [[ -n "${HMS_SMOKE_NOTIFICATION_NEGATIVE_DB:-}" || -n "${HMS_SMOKE_NOTIFICATION_NEGATIVE_TABLE:-}" ]]; then
    require_var HMS_SMOKE_NOTIFICATION_NEGATIVE_DB
    require_var HMS_SMOKE_NOTIFICATION_NEGATIVE_TABLE
    run_notification_smoke_target \
      "notification smoke negative" \
      "${HMS_SMOKE_NOTIFICATION_NEGATIVE_DB}" \
      "${HMS_SMOKE_NOTIFICATION_NEGATIVE_TABLE}" \
      "${HMS_SMOKE_NOTIFICATION_NEGATIVE_TXN_ID:-${HMS_SMOKE_NOTIFICATION_TXN_ID}}" \
      "${HMS_SMOKE_NOTIFICATION_NEGATIVE_WRITE_ID:-${HMS_SMOKE_NOTIFICATION_WRITE_ID}}" \
      "true"
  fi
}

beeline_is_configured() {
  [[ -n "${HMS_SMOKE_BEELINE_JDBC_URL:-}" ]]
}

beeline_run_maybe_kinit() {
  if [[ "${HMS_SMOKE_AUTH}" != "kerberos" ]]; then
    return
  fi

  local do_kinit="${HMS_SMOKE_BEELINE_KINIT:-true}"
  if [[ "${do_kinit}" != "true" ]]; then
    return
  fi

  require_command kinit
  require_var HMS_SMOKE_CLIENT_PRINCIPAL
  require_var HMS_SMOKE_KEYTAB
  log "running kinit for beeline SQL smoke"
  kinit -kt "${HMS_SMOKE_KEYTAB}" "${HMS_SMOKE_CLIENT_PRINCIPAL}"
}

run_beeline_script() {
  local label="$1"
  local sql_file="$2"
  local output_file="$3"
  local beeline_bin="${HMS_SMOKE_BEELINE_BIN:-beeline}"
  require_command "${beeline_bin}"
  require_var HMS_SMOKE_BEELINE_JDBC_URL

  local -a cmd=()
  cmd=("${beeline_bin}" "-u" "${HMS_SMOKE_BEELINE_JDBC_URL}" "--showHeader=false" "--outputformat=tsv2" "-f" "${sql_file}")
  if [[ -n "${HMS_SMOKE_BEELINE_USER:-}" ]]; then
    cmd+=("-n" "${HMS_SMOKE_BEELINE_USER}")
  fi
  if [[ -n "${HMS_SMOKE_BEELINE_PASSWORD:-}" ]]; then
    cmd+=("-p" "${HMS_SMOKE_BEELINE_PASSWORD}")
  fi

  log "running ${label}"
  printf '  %q' "${cmd[@]}"
  printf '\n'
  "${cmd[@]}" | tee "${output_file}"
}

assert_file_contains() {
  local file="$1"
  local expected="$2"
  grep -F "${expected}" "${file}" >/dev/null || fail "expected '${expected}' in ${file}"
}

# Identifier quoting is a dialect difference, not a behavioural one: `SHOW CREATE TABLE` on a
# Hortonworks HiveServer2 prints `db`.`table` where the Apache one prints db.table. Both mean the
# view was rewritten to the same external name, so the backticks are stripped before comparing.
assert_file_contains_identifier() {
  local file="$1"
  local expected="$2"
  tr -d '`' < "${file}" | grep -F "${expected}" >/dev/null \
    || fail "expected identifier '${expected}' in ${file}"
}

# The proxy can expose more than one front door, and an Apache and a Hortonworks client do not
# speak the same Thrift interface - Hive has no version negotiation, so each listener answers only
# its own line. Running the SQL suite against every configured HiveServer2 is what proves both
# combinations of client and backend actually work, rather than assuming the second one follows
# from the first.
# Which front-door passes to run. The default keeps both, which is what a single-metastore
# installation wants. A stand that pairs each front door with its own metastore as the default
# catalog runs one pass per layout instead: the Hortonworks front door while the Hortonworks
# metastore is default, the Apache one while the Apache metastore is - the cross pairings are a
# different topology and are covered by their own rows rather than by this run.
sql_front_door_selected() {
  local front_door="$1"
  local selected=" ${HMS_SMOKE_SQL_FRONT_DOORS:-apache hdp} "
  [[ "${selected}" == *" ${front_door} "* ]]
}

run_sql_smoke_all() {
  if sql_front_door_selected "apache"; then
    run_sql_smoke "apache"
  else
    log "skipping the Apache front-door SQL smoke: not in HMS_SMOKE_SQL_FRONT_DOORS"
  fi

  if ! sql_front_door_selected "hdp"; then
    log "skipping the Hortonworks front-door SQL smoke: not in HMS_SMOKE_SQL_FRONT_DOORS"
  elif [[ -n "${HMS_SMOKE_BEELINE_HDP_JDBC_URL:-}" ]]; then
    HMS_SMOKE_BEELINE_JDBC_URL="${HMS_SMOKE_BEELINE_HDP_JDBC_URL}" \
    HMS_SMOKE_BEELINE_BIN="${HMS_SMOKE_BEELINE_HDP_BIN:-${HMS_SMOKE_BEELINE_BIN:-beeline}}" \
    HMS_SMOKE_BEELINE_USER="${HMS_SMOKE_BEELINE_HDP_USER:-${HMS_SMOKE_BEELINE_USER:-}}" \
      run_sql_smoke "hdp"
  else
    log "skipping the Hortonworks SQL smoke because HMS_SMOKE_BEELINE_HDP_JDBC_URL is not set"
  fi
}

# Whether this pass may run the ACID blocks. The catalog flags say which catalog gets a
# transactional table; this says through which front doors that is expected to work at all, and
# the two are different axes. Measured on the stand: the identical statements succeed through the
# Hortonworks front door and fail through the Apache one, where the insert lands but the stats
# update is refused with "Cannot change stats state for a transactional table without providing
# the transactional write state for verification (new write ID -1, valid write IDs null)" - Hive
# surfaces that as a failing StatsTask. Until that is understood, gating by catalog alone leaves
# no setting that runs ACID only where it works: on gives a red run, off proves nothing.
transactional_sql_runs_on_front_door() {
  local front_door="$1"
  local allowed=" ${HMS_SMOKE_TRANSACTIONAL_SQL_FRONT_DOORS:-hdp} "
  [[ "${allowed}" == *" ${front_door} "* ]]
}

run_sql_smoke() {
  # Names the front door this pass runs against, and keeps the objects of the two passes apart.
  local front_door="${1:-primary}"

  if ! beeline_is_configured; then
    if [[ "${SCENARIO}" == "sql" ]]; then
      fail "sql scenario requires HMS_SMOKE_BEELINE_JDBC_URL and related beeline settings"
    fi
    log "skipping beeline SQL smoke because HMS_SMOKE_BEELINE_JDBC_URL is not configured"
    return
  fi

  require_var HMS_SMOKE_HDP_READ_TABLE
  require_var HMS_SMOKE_APACHE_READ_TABLE

  local hdp_catalog="${HMS_SMOKE_HDP_CATALOG:-hdp}"
  local apache_catalog="${HMS_SMOKE_APACHE_CATALOG:-apache}"
  local external_root="${HMS_SMOKE_SQL_EXTERNAL_ROOT:-/tmp/hms-proxy-smoke}"
  local hdp_external_root="${HMS_SMOKE_HDP_EXTERNAL_ROOT:-${external_root}/${hdp_catalog}}"
  local apache_external_root="${HMS_SMOKE_APACHE_EXTERNAL_ROOT:-${external_root}/${apache_catalog}}"
  local run_view_rewrite="${HMS_SMOKE_SQL_RUN_VIEW_REWRITE:-true}"
  local run_udf="${HMS_SMOKE_SQL_RUN_UDF:-true}"
  local run_cross_catalog_join="${HMS_SMOKE_SQL_RUN_CROSS_CATALOG_JOIN:-true}"
  # Off by default: unlike everything else here it creates a database, which a real installation may
  # not allow the smoke user to do.
  local run_cross_database_join="${HMS_SMOKE_SQL_RUN_CROSS_DATABASE_JOIN:-false}"
  local udf_class="${HMS_SMOKE_SQL_UDF_CLASS:-org.apache.hadoop.hive.ql.udf.UDFReverse}"
  local udf_expected_result="${HMS_SMOKE_SQL_UDF_EXPECTED_RESULT:-yxorp}"
  local run_id=""
  run_id="$(date +%Y%m%d%H%M%S)_${front_door}"
  local managed_hdp="smoke_managed_hdp_${run_id}"
  local managed_apache="smoke_managed_apache_${run_id}"
  local external_hdp="smoke_external_hdp_${run_id}"
  local external_apache="smoke_external_apache_${run_id}"
  local txn_hdp="smoke_txn_hdp_${run_id}"
  local txn_apache="smoke_txn_apache_${run_id}"
  local udf_apache="smoke_udf_apache_${run_id}"
  local view_local="smoke_view_local_${run_id}"
  local view_cross="smoke_view_cross_${run_id}"
  local mv_local="smoke_mv_local_${run_id}"
  local cross_db="${hdp_catalog}__smoke_cross_db_${run_id}"
  local cross_db_table="smoke_cross_db_tbl_${run_id}"
  local sql_file=""
  local output_file=""
  # The X's must be the last characters of the template: BSD mktemp (macOS) only randomizes a
  # trailing run of X's and leaves a literal suffix untouched, so every run would otherwise reuse
  # the same filename - and a leftover from an interrupted run (fail() exits before the cleanup
  # trap runs) then breaks the next one. The .sql/.out suffix is appended after creation instead.
  sql_file="$(mktemp "${TMPDIR:-/tmp}/hms-proxy-sql-smoke.XXXXXX")"
  mv "${sql_file}" "${sql_file}.sql"
  sql_file="${sql_file}.sql"
  output_file="$(mktemp "${TMPDIR:-/tmp}/hms-proxy-sql-smoke.XXXXXX")"
  mv "${output_file}" "${output_file}.out"
  output_file="${output_file}.out"
  # ${var:-} guards matter: the RETURN trap stays installed after this function returns and
  # fires again for enclosing functions, where these locals no longer exist and set -u would
  # kill the whole run.
  trap 'rm -f "${sql_file:-}" "${output_file:-}"' RETURN

  beeline_run_maybe_kinit

  # Session settings applied before anything else. A Hortonworks HiveServer2 needs one: its build
  # has no MapReduce, so the engine cannot be named in hive-site.xml (the server would not start),
  # and a stand without YARN cannot use the vendor default of Tez either. `set` is the only place
  # left to choose, and it is what an operator types on a real cluster too.
  # TRUNCATE is opt-out because one backend on the stand cannot serve it. The Hortonworks
  # metastore answers a positional truncate_table by calling
  # HdfsAdmin.getEncryptionZoneForPath, which its own classpath does not carry in a matching
  # signature: it dies with NoSuchMethodError, the connection drops, and the client sees a bare
  # TTransportException behind a failed DDLTask. The same statement through the Hortonworks front
  # door succeeds, so the vendor's own truncate_table_req path does not make that call.
  local truncate_sql_managed_hdp=""
  local truncate_sql_managed_apache=""
  if [[ "${HMS_SMOKE_SQL_RUN_TRUNCATE:-true}" == "true" ]]; then
    truncate_sql_managed_hdp="truncate table ${managed_hdp};
select count(*) as managed_hdp_count_after_truncate from ${managed_hdp};"
    truncate_sql_managed_apache="truncate table ${managed_apache};
select count(*) as managed_apache_count_after_truncate from ${managed_apache};"
  fi

  local session_init="${HMS_SMOKE_SQL_SESSION_INIT:-}"
  if [[ "${front_door}" == "hdp" && -n "${HMS_SMOKE_SQL_HDP_SESSION_INIT:-}" ]]; then
    session_init="${HMS_SMOKE_SQL_HDP_SESSION_INIT}"
  fi

  cat > "${sql_file}" <<EOF
${session_init}
set hive.cli.print.header=true;

show databases;

use ${hdp_catalog}__default;
show tables;
describe formatted ${HMS_SMOKE_HDP_READ_TABLE};
select count(*) as hdp_read_count from ${HMS_SMOKE_HDP_READ_TABLE};

use ${apache_catalog}__default;
show tables;
describe formatted ${HMS_SMOKE_APACHE_READ_TABLE};
select count(*) as apache_read_count from ${HMS_SMOKE_APACHE_READ_TABLE};

use ${hdp_catalog}__default;
show tables;

use ${hdp_catalog}__default;
create table if not exists ${managed_hdp} (
  id int,
  ds string
)
partitioned by (p string)
stored as parquet;
alter table ${managed_hdp} set tblproperties ('smoke'='true', 'table_kind'='managed');
insert into ${managed_hdp} partition (p='2026-03-31') values (1, '2026-03-31');
select count(*) as ${managed_hdp}_count_before_rename from ${managed_hdp} where p='2026-03-31';
show partitions ${managed_hdp};
alter table ${managed_hdp} partition (p='2026-03-31') rename to partition (p='2026-04-01');
show partitions ${managed_hdp};
select count(*) as ${managed_hdp}_count_after_rename from ${managed_hdp} where p='2026-04-01';
alter table ${managed_hdp} add columns (extra string);
insert into ${managed_hdp} partition (p='2026-05-01') values (2, '2026-05-01', 'added_managed_hdp');
-- The value, not a count: the assertion downstream looks for it in the output, and a
-- count would print a number no matter what the added column actually holds.
select extra as managed_hdp_added_column_value from ${managed_hdp} where p='2026-05-01';
alter table ${managed_hdp} drop partition (p='2026-04-01');
show partitions ${managed_hdp};
${truncate_sql_managed_hdp}
alter table ${managed_hdp} rename to ${managed_hdp}_renamed;
describe formatted ${managed_hdp}_renamed;
-- Names the renamed table explicitly. Relying on `describe formatted` to mention it only
-- works for a managed table, whose directory moves with the rename; an external table
-- keeps its original location, so the new name appears nowhere in that output.
show tables like '${managed_hdp}_renamed';
select count(*) as managed_hdp_renamed_count from ${managed_hdp}_renamed;
drop table ${managed_hdp}_renamed;

create external table if not exists ${external_hdp} (
  id int,
  ds string
)
stored as parquet
location '${hdp_external_root}/external/${external_hdp}';
alter table ${external_hdp} set tblproperties ('smoke'='true', 'table_kind'='external');
insert into ${external_hdp} values (2, '2026-03-31');
select count(*) as ${external_hdp}_count from ${external_hdp} where id=2;
describe formatted ${external_hdp};
alter table ${external_hdp} add columns (extra string);
describe ${external_hdp};
alter table ${external_hdp} rename to ${external_hdp}_renamed;
describe formatted ${external_hdp}_renamed;
-- Names the renamed table explicitly. Relying on `describe formatted` to mention it only
-- works for a managed table, whose directory moves with the rename; an external table
-- keeps its original location, so the new name appears nowhere in that output.
show tables like '${external_hdp}_renamed';
select count(*) as external_hdp_renamed_count from ${external_hdp}_renamed;
drop table ${external_hdp}_renamed;
EOF

  if [[ "${HMS_SMOKE_HDP_RUN_TRANSACTIONAL_SQL:-false}" == "true" ]] \
      && transactional_sql_runs_on_front_door "${front_door}"; then
    cat >> "${sql_file}" <<EOF
create table if not exists ${txn_hdp} (
  id int,
  ds string
)
clustered by (id) into 1 buckets
stored as orc
tblproperties ('transactional'='true', 'smoke'='true', 'table_kind'='transactional');
insert into ${txn_hdp} values (1, '2026-03-31');
select count(*) as ${txn_hdp}_count from ${txn_hdp} where id=1;
drop table ${txn_hdp};
EOF
  fi

  cat >> "${sql_file}" <<EOF

use ${apache_catalog}__default;
create table if not exists ${managed_apache} (
  id int,
  ds string
)
partitioned by (p string)
stored as parquet;
alter table ${managed_apache} set tblproperties ('smoke'='true', 'table_kind'='managed');
insert into ${managed_apache} partition (p='2026-03-31') values (1, '2026-03-31');
select count(*) as ${managed_apache}_count_before_rename from ${managed_apache} where p='2026-03-31';
show partitions ${managed_apache};
alter table ${managed_apache} partition (p='2026-03-31') rename to partition (p='2026-04-01');
show partitions ${managed_apache};
select count(*) as ${managed_apache}_count_after_rename from ${managed_apache} where p='2026-04-01';
alter table ${managed_apache} add columns (extra string);
insert into ${managed_apache} partition (p='2026-05-01') values (2, '2026-05-01', 'added_managed_apache');
-- The value, not a count: the assertion downstream looks for it in the output, and a
-- count would print a number no matter what the added column actually holds.
select extra as managed_apache_added_column_value from ${managed_apache} where p='2026-05-01';
alter table ${managed_apache} drop partition (p='2026-04-01');
show partitions ${managed_apache};
${truncate_sql_managed_apache}
alter table ${managed_apache} rename to ${managed_apache}_renamed;
describe formatted ${managed_apache}_renamed;
-- Names the renamed table explicitly. Relying on `describe formatted` to mention it only
-- works for a managed table, whose directory moves with the rename; an external table
-- keeps its original location, so the new name appears nowhere in that output.
show tables like '${managed_apache}_renamed';
select count(*) as managed_apache_renamed_count from ${managed_apache}_renamed;
drop table ${managed_apache}_renamed;

create external table if not exists ${external_apache} (
  id int,
  ds string
)
stored as parquet
location '${apache_external_root}/external/${external_apache}';
alter table ${external_apache} set tblproperties ('smoke'='true', 'table_kind'='external');
insert into ${external_apache} values (2, '2026-03-31');
select count(*) as ${external_apache}_count from ${external_apache} where id=2;
describe formatted ${external_apache};
alter table ${external_apache} add columns (extra string);
describe ${external_apache};
alter table ${external_apache} rename to ${external_apache}_renamed;
describe formatted ${external_apache}_renamed;
-- Names the renamed table explicitly. Relying on `describe formatted` to mention it only
-- works for a managed table, whose directory moves with the rename; an external table
-- keeps its original location, so the new name appears nowhere in that output.
show tables like '${external_apache}_renamed';
select count(*) as external_apache_renamed_count from ${external_apache}_renamed;
drop table ${external_apache}_renamed;
EOF

  if [[ "${HMS_SMOKE_APACHE_RUN_TRANSACTIONAL_SQL:-false}" == "true" ]] \
      && transactional_sql_runs_on_front_door "${front_door}"; then
    cat >> "${sql_file}" <<EOF
create table if not exists ${txn_apache} (
  id int,
  ds string
)
clustered by (id) into 1 buckets
stored as orc
tblproperties ('transactional'='true', 'smoke'='true', 'table_kind'='transactional');
insert into ${txn_apache} values (1, '2026-03-31');
select count(*) as ${txn_apache}_count from ${txn_apache} where id=1;
drop table ${txn_apache};
EOF
  fi

  if [[ "${run_view_rewrite}" == "true" ]]; then
    cat >> "${sql_file}" <<EOF

use ${hdp_catalog}__default;
create or replace view ${view_local} as
select * from ${hdp_catalog}__default.${HMS_SMOKE_HDP_READ_TABLE};
show create table ${view_local};
describe formatted ${view_local};
select count(*) as ${view_local}_count from ${view_local};

create or replace view ${view_cross} as
select * from ${apache_catalog}__default.${HMS_SMOKE_APACHE_READ_TABLE};
show create table ${view_cross};
select count(*) as ${view_cross}_count from ${view_cross};
drop view if exists ${view_cross};
drop view if exists ${view_local};
EOF
  else
    log "skipping view rewrite SQL smoke because HMS_SMOKE_SQL_RUN_VIEW_REWRITE=${run_view_rewrite}"
  fi

  if [[ "${run_udf}" == "true" ]]; then
    cat >> "${sql_file}" <<EOF

use ${apache_catalog}__default;
drop function if exists ${udf_apache};
create function ${udf_apache} as '${udf_class}';
show functions like '*${udf_apache}';
select ${udf_apache}('proxy') as ${udf_apache}_value;
drop function if exists ${udf_apache};
EOF
  else
    log "skipping permanent UDF SQL smoke because HMS_SMOKE_SQL_RUN_UDF=${run_udf}"
  fi

  if [[ "${run_view_rewrite}" == "true" && "${HMS_SMOKE_SQL_RUN_MATERIALIZED_VIEW:-false}" == "true" ]]; then
    cat >> "${sql_file}" <<EOF
use ${hdp_catalog}__default;
create materialized view if not exists ${mv_local} as
select * from ${hdp_catalog}__default.${HMS_SMOKE_HDP_READ_TABLE};
show create table ${mv_local};
describe formatted ${mv_local};
drop materialized view if exists ${mv_local};
EOF
  fi

  # One statement reading both catalogs: Hive locks every table it touches in a single request, so
  # this is what produces a lock request whose components resolve to two namespaces. It reads only,
  # and a proxy that cannot split such a request fails it outright with "Error in acquiring locks".
  if [[ "${run_cross_catalog_join}" == "true" ]]; then
    cat >> "${sql_file}" <<EOF

use ${hdp_catalog}__default;
-- The marker is selected as a value, not as a column alias: the runner drives beeline with
-- --showHeader=false, so an alias would never reach the output being asserted on.
select 'cross_catalog_join_ok', count(*)
from ${hdp_catalog}__default.${HMS_SMOKE_HDP_READ_TABLE} h
join ${apache_catalog}__default.${HMS_SMOKE_APACHE_READ_TABLE} a on 1=1;
EOF
  else
    log "skipping cross-catalog join SQL smoke because HMS_SMOKE_SQL_RUN_CROSS_CATALOG_JOIN=${run_cross_catalog_join}"
  fi

  # Two databases of one catalog land on a single backend, but they are still two namespaces, and
  # the components have to be rewritten to their own databases rather than all to one.
  if [[ "${run_cross_database_join}" == "true" ]]; then
    cat >> "${sql_file}" <<EOF

create database if not exists ${cross_db};
use ${cross_db};
create table if not exists ${cross_db_table} (id int) stored as parquet;
insert into ${cross_db_table} values (3);
use ${hdp_catalog}__default;
select 'cross_database_join_ok', count(*)
from ${hdp_catalog}__default.${HMS_SMOKE_HDP_READ_TABLE} h
join ${cross_db}.${cross_db_table} c on 1=1;
drop table ${cross_db}.${cross_db_table};
drop database ${cross_db};
EOF
  else
    log "skipping cross-database join SQL smoke because HMS_SMOKE_SQL_RUN_CROSS_DATABASE_JOIN=${run_cross_database_join}"
  fi

  cat >> "${sql_file}" <<EOF

use ${hdp_catalog}__default;
show tables;
use ${apache_catalog}__default;
select count(*) as post_switch_apache_count from ${HMS_SMOKE_APACHE_READ_TABLE};
use ${hdp_catalog}__default;
show tables;
EOF

  run_beeline_script "beeline SQL smoke via the ${front_door} front door" "${sql_file}" "${output_file}"

  assert_file_contains "${output_file}" "${hdp_catalog}__default"
  assert_file_contains "${output_file}" "${apache_catalog}__default"
  assert_file_contains "${output_file}" "2026-04-01"
  # The metadata-breaking operations: a rename must leave the table reachable under the new name,
  # and a column added after the table existed must come back with the value written through it.
  # Both touch paths where the proxy rewrites names and locations, so a silent no-op here would be
  # a proxy defect rather than a Hive one.
  assert_file_contains "${output_file}" "added_managed_hdp"
  assert_file_contains "${output_file}" "added_managed_apache"
  assert_file_contains "${output_file}" "${managed_hdp}_renamed"
  assert_file_contains "${output_file}" "${external_hdp}_renamed"
  if [[ "${run_view_rewrite}" == "true" ]]; then
    assert_file_contains_identifier "${output_file}" "${hdp_catalog}__default.${HMS_SMOKE_HDP_READ_TABLE}"
    assert_file_contains_identifier "${output_file}" "${apache_catalog}__default.${HMS_SMOKE_APACHE_READ_TABLE}"
  fi
  if [[ "${run_udf}" == "true" ]]; then
    assert_file_contains "${output_file}" "${udf_apache}"
    assert_file_contains "${output_file}" "${udf_expected_result}"
  fi
  if [[ "${run_cross_catalog_join}" == "true" ]]; then
    assert_file_contains "${output_file}" "cross_catalog_join_ok"
  fi
  if [[ "${run_cross_database_join}" == "true" ]]; then
    assert_file_contains "${output_file}" "cross_database_join_ok"
  fi
}

# Iceberg REST catalog smoke. Discovery and loads are always checked; when HMS_SMOKE_REST_WRITE_TABLE
# is set, the default catalog's full served write surface (table, view and namespace DDL plus
# multi-table transaction commit) is exercised as round trips, not just status codes, and the
# negative checks pin down that an unknown prefix, an unknown table and a write route on any other
# catalog all fail cleanly instead of half-working.
rest_is_configured() {
  [[ -n "${HMS_SMOKE_REST_URL:-}" ]]
}

# Extra curl options for every REST request, e.g. "--negotiate -u :" on a SPNEGO-protected
# listener. Populated by run_rest_smoke from HMS_SMOKE_REST_CURL_OPTS.
REST_CURL_AUTH=()

rest_curl() {
  # ${arr[@]+...} keeps bash 3.2's set -u happy when the array is empty.
  curl -sS ${REST_CURL_AUTH[@]+"${REST_CURL_AUTH[@]}"} "$@"
}

rest_request() {
  local method="$1"
  local path="$2"
  local body_file="$3"
  rest_curl -o "${body_file}" -w '%{http_code}' -X "${method}" "${HMS_SMOKE_REST_URL}${path}"
}

run_rest_smoke() {
  if ! rest_is_configured; then
    if [[ "${SCENARIO}" == "rest" ]]; then
      fail "rest scenario requires HMS_SMOKE_REST_URL"
    fi
    log "skipping Iceberg REST smoke because HMS_SMOKE_REST_URL is not configured"
    return
  fi
  require_command curl

  local namespace="${HMS_SMOKE_REST_NAMESPACE:-default}"
  local iceberg_table="${HMS_SMOKE_REST_ICEBERG_TABLE:-}"
  local non_iceberg_table="${HMS_SMOKE_REST_NON_ICEBERG_TABLE:-}"
  local body=""
  # The X's must trail the template with nothing after them: BSD mktemp (macOS) does not
  # randomize X's followed by a literal suffix, so a fixed ".json" suffix here would make every
  # run reuse the same filename - and a leftover from an interrupted run (fail() exits before the
  # cleanup trap runs) then breaks the next one. The suffix is appended after creation instead.
  body="$(mktemp "${TMPDIR:-/tmp}/hms-proxy-rest-smoke.XXXXXX")"
  mv "${body}" "${body}.json"
  body="${body}.json"
  trap 'rm -f "${body:-}"' RETURN

  REST_CURL_AUTH=()
  if [[ -n "${HMS_SMOKE_REST_CURL_OPTS:-}" ]]; then
    # Intentional word splitting: the value is a list of curl options, e.g. "--negotiate -u :".
    read -r -a REST_CURL_AUTH <<< "${HMS_SMOKE_REST_CURL_OPTS}"
  fi

  log "running Iceberg REST smoke against ${HMS_SMOKE_REST_URL}"

  local code=""

  # An authenticated listener must challenge a bare request, not serve it: when the smoke itself
  # authenticates (HMS_SMOKE_REST_CURL_OPTS is set), a request WITHOUT those options has to be
  # rejected 401 with a Negotiate challenge and an empty body - anything else means the listener
  # serves unauthenticated reads.
  if [[ -n "${HMS_SMOKE_REST_CURL_OPTS:-}" ]]; then
    local challenge_headers=""
    challenge_headers="$(mktemp "${TMPDIR:-/tmp}/hms-proxy-rest-smoke.XXXXXX")"
    code="$(curl -sS -D "${challenge_headers}" -o "${body}" -w '%{http_code}' "${HMS_SMOKE_REST_URL}/v1/config")"
    if [[ "${code}" != "401" ]]; then
      rm -f "${challenge_headers}"
      fail "unauthenticated GET /v1/config expected HTTP 401, got ${code}: $(cat "${body}")"
    fi
    if ! grep -qi '^WWW-Authenticate:[[:space:]]*Negotiate' "${challenge_headers}"; then
      rm -f "${challenge_headers}"
      fail "unauthenticated GET /v1/config carries no 'WWW-Authenticate: Negotiate' challenge"
    fi
    rm -f "${challenge_headers}"
    [[ -s "${body}" ]] && fail "unauthenticated GET /v1/config answered a non-empty body: $(cat "${body}")"
  fi

  code="$(rest_request GET "/v1/config" "${body}")"
  [[ "${code}" == "200" ]] || fail "GET /v1/config returned HTTP ${code}: $(cat "${body}")"

  # The config response pins clients to the proxy's default catalog; every later path
  # reuses the advertised prefix instead of guessing it.
  local prefix=""
  prefix="$(grep -o '"prefix"[[:space:]]*:[[:space:]]*"[^"]*"' "${body}" | head -n 1 | sed 's/.*"\([^"]*\)"$/\1/')"
  [[ -n "${prefix}" ]] || fail "GET /v1/config carries no prefix override: $(cat "${body}")"
  if [[ -n "${HMS_SMOKE_REST_PREFIX:-}" && "${HMS_SMOKE_REST_PREFIX}" != "${prefix}" ]]; then
    fail "GET /v1/config prefix '${prefix}' does not match HMS_SMOKE_REST_PREFIX='${HMS_SMOKE_REST_PREFIX}'"
  fi

  code="$(rest_request GET "/v1/${prefix}/namespaces" "${body}")"
  [[ "${code}" == "200" ]] || fail "GET /v1/${prefix}/namespaces returned HTTP ${code}: $(cat "${body}")"
  grep -q "\"${namespace}\"" "${body}" \
    || fail "namespace '${namespace}' missing from the REST listing: $(cat "${body}")"

  code="$(rest_request GET "/v1/${prefix}/namespaces/${namespace}" "${body}")"
  [[ "${code}" == "200" ]] || fail "GET namespace '${namespace}' returned HTTP ${code}: $(cat "${body}")"

  code="$(rest_request GET "/v1/${prefix}/namespaces/${namespace}/tables" "${body}")"
  [[ "${code}" == "200" ]] || fail "GET tables of '${namespace}' returned HTTP ${code}: $(cat "${body}")"
  if [[ -n "${iceberg_table}" ]]; then
    grep -q "\"name\"[[:space:]]*:[[:space:]]*\"${iceberg_table}\"" "${body}" \
      || fail "Iceberg table '${iceberg_table}' missing from the REST listing: $(cat "${body}")"
  fi
  if [[ -n "${non_iceberg_table}" ]]; then
    if grep -q "\"name\"[[:space:]]*:[[:space:]]*\"${non_iceberg_table}\"" "${body}"; then
      fail "non-Iceberg table '${non_iceberg_table}' leaked into the Iceberg REST listing: $(cat "${body}")"
    fi
  fi

  if [[ -n "${iceberg_table}" ]]; then
    code="$(rest_request GET "/v1/${prefix}/namespaces/${namespace}/tables/${iceberg_table}" "${body}")"
    [[ "${code}" == "200" ]] || fail "REST load of '${iceberg_table}' returned HTTP ${code}: $(cat "${body}")"
    grep -q '"metadata-location"' "${body}" \
      || fail "REST load of '${iceberg_table}' carries no metadata-location: $(cat "${body}")"
  else
    log "skipping REST load-table check because HMS_SMOKE_REST_ICEBERG_TABLE is not set"
  fi

  code="$(rest_request GET "/v1/no_such_prefix_smoke/namespaces" "${body}")"
  [[ "${code}" == "404" ]] \
    || fail "unknown REST prefix expected HTTP 404, got ${code}: $(cat "${body}")"

  code="$(rest_request GET "/v1/${prefix}/namespaces/${namespace}/tables/no_such_table_smoke" "${body}")"
  [[ "${code}" == "404" ]] \
    || fail "unknown REST table expected HTTP 404, got ${code}: $(cat "${body}")"

  # Dropping a table that was never created must not silently succeed with a 2xx - true for
  # every catalog, independent of whether that catalog's writes are gated (see the write round
  # trip and its negatives further below, guarded by HMS_SMOKE_REST_WRITE_TABLE).
  code="$(rest_request DELETE "/v1/${prefix}/namespaces/${namespace}/tables/no_such_table_smoke" "${body}")"
  [[ "${code}" =~ ^2 ]] && fail "dropping a non-existent table unexpectedly succeeded with HTTP ${code}: $(cat "${body}")"

  # Error responses keep the mapped status, type and message but must not leak the server
  # stack trace: this listener may be unauthenticated, so a trace would expose internal
  # package structure, file names and line numbers.
  code="$(rest_request GET "/v1/${prefix}/namespaces/no_such_ns_smoke" "${body}")"
  [[ "${code}" == "404" ]] || fail "missing namespace expected HTTP 404, got ${code}: $(cat "${body}")"
  if grep -q '"stack":\["' "${body}"; then
    fail "error response leaks a server stack trace: $(cat "${body}")"
  fi

  # A body that fails to parse must answer 400, not fall through to a 500.
  if [[ -n "${iceberg_table}" ]]; then
    code="$(rest_curl -o "${body}" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
      --data 'not json at all' \
      "${HMS_SMOKE_REST_URL}/v1/${prefix}/namespaces/${namespace}/tables/${iceberg_table}/metrics")"
    [[ "${code}" == "400" ]] || fail "unparseable body expected HTTP 400, got ${code}: $(cat "${body}")"
  else
    log "skipping REST unparseable-body check because HMS_SMOKE_REST_ICEBERG_TABLE is not set"
  fi

  # GET /v1/config resolves to the default catalog (no warehouse override), which since phase 5a
  # is also this proxy's only write-capable catalog: it must advertise the namespaces read route
  # AND its write routes (table create, commit, drop, rename, register). Checked against both the
  # unprefixed /v1/config and the prefixed /v1/{prefix}/config, which clients pin to via the
  # "prefix" override and use identically for discovery. The matching non-default-catalog check,
  # proving the write routes stay absent there, lives below in the optional second-prefix block.
  code="$(rest_request GET "/v1/config" "${body}")"
  [[ "${code}" == "200" ]] || fail "GET /v1/config returned HTTP ${code}: $(cat "${body}")"
  grep -qF '"GET /v1/{prefix}/namespaces"' "${body}" \
    || fail "config does not advertise the namespaces read route: $(cat "${body}")"
  grep -qF '"POST /v1/{prefix}/namespaces/{namespace}/tables"' "${body}" \
    || fail "default-catalog config does not advertise the table-create write route: $(cat "${body}")"
  grep -qF '"DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}"' "${body}" \
    || fail "default-catalog config does not advertise the table-drop write route: $(cat "${body}")"

  code="$(rest_request GET "/v1/${prefix}/config" "${body}")"
  [[ "${code}" == "200" ]] || fail "GET /v1/${prefix}/config returned HTTP ${code}: $(cat "${body}")"
  grep -qF '"GET /v1/{prefix}/namespaces"' "${body}" \
    || fail "prefixed config does not advertise the namespaces read route: $(cat "${body}")"
  grep -qF '"POST /v1/{prefix}/namespaces/{namespace}/tables"' "${body}" \
    || fail "prefixed default-catalog config does not advertise the table-create write route: $(cat "${body}")"
  grep -qF '"DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}"' "${body}" \
    || fail "prefixed default-catalog config does not advertise the table-drop write route: $(cat "${body}")"

  # Optional second prefix: proves warehouse discovery and the clean view work for a
  # non-default catalog too, not only for the one /v1/config already advertised.
  local second_prefix="${HMS_SMOKE_REST_SECOND_PREFIX:-}"
  local separator="${HMS_SMOKE_REST_SEPARATOR:-__}"
  if [[ -n "${second_prefix}" ]]; then
    # The second catalog's databases appear twice on purpose: under the default
    # prefix as federated <catalog><separator><db> names, and under their own
    # prefix as bare internal names. Both sides are asserted here.
    local fed_ns="${second_prefix}${separator}${namespace}"

    code="$(rest_request GET "/v1/config?warehouse=${second_prefix}" "${body}")"
    [[ "${code}" == "200" ]] || fail "config?warehouse=${second_prefix} returned HTTP ${code}: $(cat "${body}")"
    grep -q "\"prefix\"[[:space:]]*:[[:space:]]*\"${second_prefix}\"" "${body}" \
      || fail "warehouse discovery did not advertise prefix '${second_prefix}': $(cat "${body}")"

    code="$(rest_request GET "/v1/config?warehouse=no_such_warehouse_smoke" "${body}")"
    [[ "${code}" == "400" ]] || fail "unknown warehouse expected HTTP 400, got ${code}: $(cat "${body}")"

    # Discovery must advertise the write asymmetry: the non-default catalog's own config carries
    # the namespaces read route but none of the write routes the default catalog's config asserted
    # above, since only the default catalog's writes reach a real backend lock.
    code="$(rest_request GET "/v1/${second_prefix}/config" "${body}")"
    [[ "${code}" == "200" ]] || fail "GET /v1/${second_prefix}/config returned HTTP ${code}: $(cat "${body}")"
    grep -qF '"GET /v1/{prefix}/namespaces"' "${body}" \
      || fail "non-default-catalog config does not advertise the namespaces read route: $(cat "${body}")"
    if grep -qF 'POST /v1/{prefix}/namespaces/{namespace}/tables"' "${body}"; then
      fail "non-default-catalog config unexpectedly advertises the table-create write route: $(cat "${body}")"
    fi
    if grep -qF 'DELETE ' "${body}"; then
      fail "non-default-catalog config unexpectedly advertises a write (DELETE) route: $(cat "${body}")"
    fi

    code="$(rest_request GET "/v1/${second_prefix}/namespaces" "${body}")"
    [[ "${code}" == "200" ]] || fail "GET /v1/${second_prefix}/namespaces returned HTTP ${code}: $(cat "${body}")"
    grep -q "\[\"${namespace}\"\]" "${body}" \
      || fail "namespace '${namespace}' missing under prefix '${second_prefix}': $(cat "${body}")"
    if grep -q "${second_prefix}${separator}" "${body}"; then
      fail "external names leaked into the clean view of '${second_prefix}': $(cat "${body}")"
    fi

    code="$(rest_request GET "/v1/${prefix}/namespaces" "${body}")"
    [[ "${code}" == "200" ]] || fail "GET /v1/${prefix}/namespaces returned HTTP ${code}: $(cat "${body}")"
    grep -q "\[\"${fed_ns}\"\]" "${body}" \
      || fail "federated namespace '${fed_ns}' missing under the default prefix '${prefix}': $(cat "${body}")"

    code="$(rest_request GET "/v1/${second_prefix}/namespaces/${fed_ns}" "${body}")"
    [[ "${code}" == "404" ]] \
      || fail "external namespace name '${fed_ns}' under prefix '${second_prefix}' expected HTTP 404, got ${code}: $(cat "${body}")"

    if [[ -n "${iceberg_table}" ]]; then
      code="$(rest_request GET "/v1/${second_prefix}/namespaces/${namespace}/tables/${iceberg_table}" "${body}")"
      [[ "${code}" == "404" ]] \
        || fail "default-catalog table '${iceberg_table}' under prefix '${second_prefix}' expected HTTP 404, got ${code}: $(cat "${body}")"
    fi

    if [[ -n "${HMS_SMOKE_REST_SECOND_ICEBERG_TABLE:-}" ]]; then
      code="$(rest_request GET "/v1/${second_prefix}/namespaces/${namespace}/tables" "${body}")"
      [[ "${code}" == "200" ]] || fail "GET tables under '${second_prefix}' returned HTTP ${code}: $(cat "${body}")"
      grep -q "\"name\"[[:space:]]*:[[:space:]]*\"${HMS_SMOKE_REST_SECOND_ICEBERG_TABLE}\"" "${body}" \
        || fail "Iceberg table '${HMS_SMOKE_REST_SECOND_ICEBERG_TABLE}' missing from the '${second_prefix}' listing: $(cat "${body}")"
      if [[ -n "${HMS_SMOKE_REST_SECOND_NON_ICEBERG_TABLE:-}" ]]; then
        if grep -q "\"name\"[[:space:]]*:[[:space:]]*\"${HMS_SMOKE_REST_SECOND_NON_ICEBERG_TABLE}\"" "${body}"; then
          fail "non-Iceberg table '${HMS_SMOKE_REST_SECOND_NON_ICEBERG_TABLE}' leaked into the '${second_prefix}' listing: $(cat "${body}")"
        fi
      fi

      code="$(rest_request GET "/v1/${second_prefix}/namespaces/${namespace}/tables/${HMS_SMOKE_REST_SECOND_ICEBERG_TABLE}" "${body}")"
      [[ "${code}" == "200" ]] || fail "REST load under '${second_prefix}' returned HTTP ${code}: $(cat "${body}")"
      grep -q '"metadata-location"' "${body}" \
        || fail "second-prefix load carries no metadata-location: $(cat "${body}")"

      code="$(rest_request GET "/v1/${prefix}/namespaces/${fed_ns}/tables" "${body}")"
      [[ "${code}" == "200" ]] || fail "GET tables of '${fed_ns}' under '${prefix}' returned HTTP ${code}: $(cat "${body}")"
      grep -q "\"name\"[[:space:]]*:[[:space:]]*\"${HMS_SMOKE_REST_SECOND_ICEBERG_TABLE}\"" "${body}" \
        || fail "Iceberg table '${HMS_SMOKE_REST_SECOND_ICEBERG_TABLE}' missing from the federated '${fed_ns}' listing: $(cat "${body}")"

      code="$(rest_request GET "/v1/${prefix}/namespaces/${fed_ns}/tables/${HMS_SMOKE_REST_SECOND_ICEBERG_TABLE}" "${body}")"
      [[ "${code}" == "200" ]] \
        || fail "federated load of '${fed_ns}.${HMS_SMOKE_REST_SECOND_ICEBERG_TABLE}' returned HTTP ${code}: $(cat "${body}")"
      grep -q '"metadata-location"' "${body}" \
        || fail "federated load carries no metadata-location: $(cat "${body}")"
    fi
  fi

  # Table writes: supported only where the request resolves to the default catalog, because
  # only that catalog's commit path reaches the real backend's Hive lock; every other catalog is
  # served by the synthetic lock shim, which grants locks without conflict checking, so a commit
  # there would race concurrent writers into silently lost updates. Guarded by
  # HMS_SMOKE_REST_WRITE_TABLE so the check stays off unless a stand is known to support it.
  #
  # The round trip below exercises create, a REAL commit against the just-created table, and a
  # rename - not just create/load/drop - because the commit is the lock-taking path the whole
  # phase-5a safety argument is about, and rename is a second write route entirely. The gate
  # negatives cover write routes beyond CREATE_TABLE too: COMMIT_TRANSACTION was a critical
  # bypass found during this phase and, unlike CREATE_TABLE, is otherwise only pinned down by
  # unit tests. All gate negatives prove enforcement on the *resolved* catalog, not just the
  # request's own prefix - a federated name under the default prefix is refused exactly like a
  # direct request against the non-default prefix.
  local write_table="${HMS_SMOKE_REST_WRITE_TABLE:-}"
  if [[ -n "${write_table}" ]]; then
    local renamed_write_table="${write_table}_renamed"
    # A rerun against a dirty stand must not half-fail on a leftover from a previous run left
    # under either name this block can leave the table under; neither drop result is asserted.
    local discard=""
    discard="$(rest_request DELETE "/v1/${prefix}/namespaces/${namespace}/tables/${write_table}" "${body}")"
    discard="$(rest_request DELETE "/v1/${prefix}/namespaces/${namespace}/tables/${renamed_write_table}" "${body}")"

    local create_body='{"name":"'"${write_table}"'","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":false,"type":"int"}]}}'

    code="$(rest_curl -o "${body}" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
      --data "${create_body}" "${HMS_SMOKE_REST_URL}/v1/${prefix}/namespaces/${namespace}/tables")"
    [[ "${code}" == "200" ]] || fail "REST create of '${write_table}' returned HTTP ${code}: $(cat "${body}")"

    local create_metadata_location=""
    create_metadata_location="$(grep -o '"metadata-location"[[:space:]]*:[[:space:]]*"[^"]*"' "${body}" | head -n 1 | sed 's/.*"\([^"]*\)"$/\1/')"
    [[ -n "${create_metadata_location}" ]] \
      || fail "REST create of '${write_table}' carries no metadata-location: $(cat "${body}")"

    local table_uuid=""
    table_uuid="$(grep -o '"table-uuid"[[:space:]]*:[[:space:]]*"[^"]*"' "${body}" | head -n 1 | sed 's/.*"\([^"]*\)"$/\1/')"
    [[ -n "${table_uuid}" ]] \
      || fail "REST create of '${write_table}' carries no metadata.table-uuid: $(cat "${body}")"

    code="$(rest_request GET "/v1/${prefix}/namespaces/${namespace}/tables/${write_table}" "${body}")"
    [[ "${code}" == "200" ]] || fail "REST load of freshly-created '${write_table}' returned HTTP ${code}: $(cat "${body}")"
    grep -q '"metadata-location"' "${body}" \
      || fail "REST load of '${write_table}' carries no metadata-location: $(cat "${body}")"

    # Real commit against the existing table: the phase-5a safety argument rests on this path
    # taking a real Hive lock via HiveTableOperations.commit. A commit that silently no-ops
    # would still answer 200 but hand back the SAME metadata-location, so the check that matters
    # is the new location differing from create's, not merely the status code.
    local commit_body='{"requirements":[{"type":"assert-table-uuid","uuid":"'"${table_uuid}"'"}],"updates":[{"action":"set-properties","updates":{"smoke":"committed"}}]}'
    code="$(rest_curl -o "${body}" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
      --data "${commit_body}" "${HMS_SMOKE_REST_URL}/v1/${prefix}/namespaces/${namespace}/tables/${write_table}")"
    [[ "${code}" == "200" ]] || fail "REST commit of '${write_table}' returned HTTP ${code}: $(cat "${body}")"

    local commit_metadata_location=""
    commit_metadata_location="$(grep -o '"metadata-location"[[:space:]]*:[[:space:]]*"[^"]*"' "${body}" | head -n 1 | sed 's/.*"\([^"]*\)"$/\1/')"
    [[ -n "${commit_metadata_location}" ]] \
      || fail "REST commit of '${write_table}' carries no metadata-location: $(cat "${body}")"
    [[ "${commit_metadata_location}" != "${create_metadata_location}" ]] \
      || fail "REST commit of '${write_table}' did not write a new metadata file: metadata-location is still '${commit_metadata_location}'"

    if [[ -n "${second_prefix}" ]]; then
      code="$(rest_curl -o "${body}" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
        --data "${create_body}" "${HMS_SMOKE_REST_URL}/v1/${second_prefix}/namespaces/${namespace}/tables")"
      [[ "${code}" == "403" ]] \
        || fail "REST create under non-default prefix '${second_prefix}' expected HTTP 403, got ${code}: $(cat "${body}")"

      local write_fed_ns="${second_prefix}${separator}${namespace}"
      code="$(rest_curl -o "${body}" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
        --data "${create_body}" "${HMS_SMOKE_REST_URL}/v1/${prefix}/namespaces/${write_fed_ns}/tables")"
      [[ "${code}" == "403" ]] \
        || fail "REST create under federated namespace '${write_fed_ns}' expected HTTP 403, got ${code}: $(cat "${body}")"

      # COMMIT_TRANSACTION naming a federated table: the bypass this phase actually found, and
      # otherwise only covered by unit tests.
      code="$(rest_curl -o "${body}" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
        --data '{"table-changes":[{"identifier":{"namespace":["'"${write_fed_ns}"'"],"name":"t"},"requirements":[],"updates":[]}]}' \
        "${HMS_SMOKE_REST_URL}/v1/${prefix}/transactions/commit")"
      [[ "${code}" == "403" ]] \
        || fail "REST COMMIT_TRANSACTION naming federated table in '${write_fed_ns}' expected HTTP 403, got ${code}: $(cat "${body}")"

      # CREATE_NAMESPACE with a federated name.
      code="$(rest_curl -o "${body}" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
        --data '{"namespace":["'"${second_prefix}${separator}zzz_smoke"'"]}' \
        "${HMS_SMOKE_REST_URL}/v1/${prefix}/namespaces")"
      [[ "${code}" == "403" ]] \
        || fail "REST CREATE_NAMESPACE with federated name '${second_prefix}${separator}zzz_smoke' expected HTTP 403, got ${code}: $(cat "${body}")"

      # RENAME with a federated destination: proves the destination-side check, not just the
      # source. Must run while the source table still exists under its current name.
      code="$(rest_curl -o "${body}" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
        --data '{"source":{"namespace":["'"${namespace}"'"],"name":"'"${write_table}"'"},"destination":{"namespace":["'"${write_fed_ns}"'"],"name":"'"${write_table}"'"}}' \
        "${HMS_SMOKE_REST_URL}/v1/${prefix}/tables/rename")"
      [[ "${code}" == "403" ]] \
        || fail "REST rename with federated destination namespace '${write_fed_ns}' expected HTTP 403, got ${code}: $(cat "${body}")"

      # CREATE_VIEW with a federated namespace. Needs the FULL valid view body, not a stub -
      # an earlier attempt with a minimal body got HTTP 400 because the body failed to parse
      # before the gate was ever consulted. A 400 from this assertion means the request body is
      # malformed, not that the gate let the write through.
      code="$(rest_curl -o "${body}" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
        --data '{"name":"zzz_smoke_view","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":false,"type":"int"}]},"view-version":{"version-id":1,"timestamp-ms":1753700000000,"schema-id":0,"summary":{"operation":"create"},"default-namespace":["'"${write_fed_ns}"'"],"representations":[{"type":"sql","sql":"select 1","dialect":"hive"}]},"properties":{}}' \
        "${HMS_SMOKE_REST_URL}/v1/${prefix}/namespaces/${write_fed_ns}/views")"
      [[ "${code}" == "403" ]] \
        || fail "REST CREATE_VIEW under federated namespace '${write_fed_ns}' expected HTTP 403, got ${code} (400 would mean the request body is malformed, not that the gate refused it): $(cat "${body}")"

      # DROP_VIEW with a federated namespace.
      code="$(rest_request DELETE "/v1/${prefix}/namespaces/${write_fed_ns}/views/whatever" "${body}")"
      [[ "${code}" == "403" ]] \
        || fail "REST DROP_VIEW under federated namespace '${write_fed_ns}' expected HTTP 403, got ${code}: $(cat "${body}")"

      # DROP_NAMESPACE of a federated namespace.
      code="$(rest_request DELETE "/v1/${prefix}/namespaces/${write_fed_ns}" "${body}")"
      [[ "${code}" == "403" ]] \
        || fail "REST DROP_NAMESPACE of federated namespace '${write_fed_ns}' expected HTTP 403, got ${code}: $(cat "${body}")"

      # UPDATE_NAMESPACE (properties) of a federated namespace.
      code="$(rest_curl -o "${body}" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
        --data '{"removals":[],"updates":{"x":"y"}}' \
        "${HMS_SMOKE_REST_URL}/v1/${prefix}/namespaces/${write_fed_ns}/properties")"
      [[ "${code}" == "403" ]] \
        || fail "REST UPDATE_NAMESPACE of federated namespace '${write_fed_ns}' expected HTTP 403, got ${code}: $(cat "${body}")"

      # UPDATE_TABLE (per-table commit) under a federated namespace. The gate must answer before
      # existence is ever checked, so the table named here does not need to exist - a 404 here
      # would mean the gate was consulted after the lookup, which is exactly the drift this pins.
      code="$(rest_curl -o "${body}" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
        --data '{"requirements":[],"updates":[]}' \
        "${HMS_SMOKE_REST_URL}/v1/${prefix}/namespaces/${write_fed_ns}/tables/whatever")"
      [[ "${code}" == "403" ]] \
        || fail "REST UPDATE_TABLE under federated namespace '${write_fed_ns}' expected HTTP 403, got ${code}: $(cat "${body}")"

      # DROP_TABLE under a federated namespace - same gate-before-lookup argument as above.
      code="$(rest_request DELETE "/v1/${prefix}/namespaces/${write_fed_ns}/tables/whatever" "${body}")"
      [[ "${code}" == "403" ]] \
        || fail "REST DROP_TABLE under federated namespace '${write_fed_ns}' expected HTTP 403, got ${code}: $(cat "${body}")"

      # REGISTER_TABLE under a federated namespace. The metadata-location is deliberately bogus:
      # the gate must refuse before anything tries to read it.
      code="$(rest_curl -o "${body}" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
        --data '{"name":"zzz_smoke_reg","metadata-location":"hdfs://smoke-invalid/never-read.metadata.json"}' \
        "${HMS_SMOKE_REST_URL}/v1/${prefix}/namespaces/${write_fed_ns}/register")"
      [[ "${code}" == "403" ]] \
        || fail "REST REGISTER_TABLE under federated namespace '${write_fed_ns}' expected HTTP 403, got ${code}: $(cat "${body}")"

      # UPDATE_VIEW under a federated namespace - the view need not exist, see UPDATE_TABLE above.
      code="$(rest_curl -o "${body}" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
        --data '{"requirements":[],"updates":[]}' \
        "${HMS_SMOKE_REST_URL}/v1/${prefix}/namespaces/${write_fed_ns}/views/whatever")"
      [[ "${code}" == "403" ]] \
        || fail "REST UPDATE_VIEW under federated namespace '${write_fed_ns}' expected HTTP 403, got ${code}: $(cat "${body}")"

      # RENAME_VIEW with a federated destination: proves the destination-side check for views the
      # way the table rename negative above proves it for tables.
      code="$(rest_curl -o "${body}" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
        --data '{"source":{"namespace":["'"${namespace}"'"],"name":"whatever"},"destination":{"namespace":["'"${write_fed_ns}"'"],"name":"whatever"}}' \
        "${HMS_SMOKE_REST_URL}/v1/${prefix}/views/rename")"
      [[ "${code}" == "403" ]] \
        || fail "REST RENAME_VIEW with federated destination namespace '${write_fed_ns}' expected HTTP 403, got ${code}: $(cat "${body}")"
    else
      log "skipping REST write-gate negative checks because HMS_SMOKE_REST_SECOND_PREFIX is not set"
    fi

    # Rename round trip: the real table must survive under its new name, not just answer 204.
    code="$(rest_curl -o "${body}" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
      --data '{"source":{"namespace":["'"${namespace}"'"],"name":"'"${write_table}"'"},"destination":{"namespace":["'"${namespace}"'"],"name":"'"${renamed_write_table}"'"}}' \
      "${HMS_SMOKE_REST_URL}/v1/${prefix}/tables/rename")"
    [[ "${code}" == "204" ]] || fail "REST rename of '${write_table}' returned HTTP ${code}: $(cat "${body}")"

    code="$(rest_request GET "/v1/${prefix}/namespaces/${namespace}/tables/${renamed_write_table}" "${body}")"
    [[ "${code}" == "200" ]] || fail "REST load of renamed '${renamed_write_table}' returned HTTP ${code}: $(cat "${body}")"

    code="$(rest_request DELETE "/v1/${prefix}/namespaces/${namespace}/tables/${renamed_write_table}" "${body}")"
    [[ "${code}" =~ ^2 ]] || fail "REST drop of '${renamed_write_table}' returned HTTP ${code}: $(cat "${body}")"

    # Namespace DDL round trip: create, load, update a property and drop - genuinely new since
    # RoutingMetaStoreClient only gained createDatabase/alterDatabase/dropDatabase this phase.
    # Same default-catalog-only restriction as table writes (WriteRouteGate covers CREATE_NAMESPACE
    # by namespace, not by the request's own prefix). This namespace (like the write table above,
    # and the view below) is created and destroyed by the smoke itself.
    local ns_smoke="${HMS_SMOKE_REST_WRITE_NAMESPACE:-smoke_rest_ns}"
    # Dropped defensively first so a rerun on a dirty stand cannot half-fail - but only when it is
    # either absent or already empty. A real installation may already have a namespace under this
    # exact name (especially the default), and blindly dropping it would destroy real content;
    # emptiness (no tables, no views) is the cheap signal that whatever is there was left by an
    # earlier interrupted run of this same smoke rather than being genuine.
    local ns_precheck_code=""
    ns_precheck_code="$(rest_request GET "/v1/${prefix}/namespaces/${ns_smoke}" "${body}")"
    if [[ "${ns_precheck_code}" == "200" ]]; then
      local ns_precheck_tables_code=""
      ns_precheck_tables_code="$(rest_request GET "/v1/${prefix}/namespaces/${ns_smoke}/tables" "${body}")"
      [[ "${ns_precheck_tables_code}" == "200" ]] \
        || fail "GET tables of '${ns_smoke}' before the defensive namespace drop returned HTTP ${ns_precheck_tables_code}: $(cat "${body}")"
      grep -q '"identifiers"[[:space:]]*:[[:space:]]*\[\]' "${body}" \
        || fail "namespace '${ns_smoke}' already exists and has tables; refusing to drop it defensively. Set HMS_SMOKE_REST_WRITE_NAMESPACE to a namespace name that is not in use on this installation."

      local ns_precheck_views_code=""
      ns_precheck_views_code="$(rest_request GET "/v1/${prefix}/namespaces/${ns_smoke}/views" "${body}")"
      [[ "${ns_precheck_views_code}" == "200" ]] \
        || fail "GET views of '${ns_smoke}' before the defensive namespace drop returned HTTP ${ns_precheck_views_code}: $(cat "${body}")"
      grep -q '"identifiers"[[:space:]]*:[[:space:]]*\[\]' "${body}" \
        || fail "namespace '${ns_smoke}' already exists and has views; refusing to drop it defensively. Set HMS_SMOKE_REST_WRITE_NAMESPACE to a namespace name that is not in use on this installation."

      discard="$(rest_request DELETE "/v1/${prefix}/namespaces/${ns_smoke}" "${body}")"
    elif [[ "${ns_precheck_code}" != "404" ]]; then
      fail "GET namespace '${ns_smoke}' before the defensive namespace drop returned HTTP ${ns_precheck_code}: $(cat "${body}")"
    fi

    code="$(rest_curl -o "${body}" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
      --data '{"namespace":["'"${ns_smoke}"'"]}' "${HMS_SMOKE_REST_URL}/v1/${prefix}/namespaces")"
    [[ "${code}" == "200" ]] || fail "REST namespace create of '${ns_smoke}' returned HTTP ${code}: $(cat "${body}")"

    code="$(rest_request GET "/v1/${prefix}/namespaces/${ns_smoke}" "${body}")"
    [[ "${code}" == "200" ]] || fail "REST namespace load of '${ns_smoke}' returned HTTP ${code}: $(cat "${body}")"

    code="$(rest_curl -o "${body}" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
      --data '{"removals":[],"updates":{"smoke":"yes"}}' \
      "${HMS_SMOKE_REST_URL}/v1/${prefix}/namespaces/${ns_smoke}/properties")"
    [[ "${code}" == "200" ]] || fail "REST namespace property update of '${ns_smoke}' returned HTTP ${code}: $(cat "${body}")"

    # The effect that matters: the property must actually be there, not just a 200 status - a
    # no-op update route would still answer 200.
    code="$(rest_request GET "/v1/${prefix}/namespaces/${ns_smoke}" "${body}")"
    [[ "${code}" == "200" ]] || fail "REST namespace reload of '${ns_smoke}' returned HTTP ${code}: $(cat "${body}")"
    grep -q '"smoke"[[:space:]]*:[[:space:]]*"yes"' "${body}" \
      || fail "REST namespace property update of '${ns_smoke}' did not stick: $(cat "${body}")"

    code="$(rest_request DELETE "/v1/${prefix}/namespaces/${ns_smoke}" "${body}")"
    [[ "${code}" == "204" ]] || fail "REST namespace drop of '${ns_smoke}' returned HTTP ${code}: $(cat "${body}")"

    code="$(rest_request GET "/v1/${prefix}/namespaces/${ns_smoke}" "${body}")"
    [[ "${code}" == "404" ]] \
      || fail "REST namespace load of dropped '${ns_smoke}' expected HTTP 404, got ${code}: $(cat "${body}")"

    # View write round trip: create (asserting a real metadata-location), list, update and
    # rename (asserting effects, not just status codes), and drop. View writes were already
    # reachable after phase 5a's commit path; this smoke makes that officially covered rather
    # than merely advertised, under the same default-catalog-only restriction. This view is
    # created and destroyed by the smoke itself. Both the original and the renamed name are
    # dropped defensively first so a rerun on a dirty stand cannot half-fail - a single named
    # view is a narrower blast radius than a whole namespace, so no emptiness check is needed
    # here the way there is for the namespace above.
    local view_smoke="${HMS_SMOKE_REST_WRITE_VIEW:-smoke_rest_view}"
    local renamed_view_smoke="${view_smoke}_renamed"
    discard="$(rest_request DELETE "/v1/${prefix}/namespaces/${namespace}/views/${view_smoke}" "${body}")"
    discard="$(rest_request DELETE "/v1/${prefix}/namespaces/${namespace}/views/${renamed_view_smoke}" "${body}")"

    local view_create_body='{"name":"'"${view_smoke}"'","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":false,"type":"int"}]},"view-version":{"version-id":1,"timestamp-ms":1753700000000,"schema-id":0,"summary":{"operation":"create"},"default-namespace":["'"${namespace}"'"],"representations":[{"type":"sql","sql":"select 1","dialect":"hive"}]},"properties":{}}'
    code="$(rest_curl -o "${body}" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
      --data "${view_create_body}" "${HMS_SMOKE_REST_URL}/v1/${prefix}/namespaces/${namespace}/views")"
    [[ "${code}" == "200" ]] || fail "REST view create of '${view_smoke}' returned HTTP ${code}: $(cat "${body}")"
    grep -q '"metadata-location"' "${body}" \
      || fail "REST view create of '${view_smoke}' carries no metadata-location: $(cat "${body}")"

    local view_uuid=""
    view_uuid="$(grep -o '"view-uuid"[[:space:]]*:[[:space:]]*"[^"]*"' "${body}" | head -n 1 | sed 's/.*"\([^"]*\)"$/\1/')"
    [[ -n "${view_uuid}" ]] \
      || fail "REST view create of '${view_smoke}' carries no metadata.view-uuid: $(cat "${body}")"

    code="$(rest_request GET "/v1/${prefix}/namespaces/${namespace}/views" "${body}")"
    [[ "${code}" == "200" ]] || fail "REST view listing of '${namespace}' returned HTTP ${code}: $(cat "${body}")"
    grep -q "\"name\"[[:space:]]*:[[:space:]]*\"${view_smoke}\"" "${body}" \
      || fail "view '${view_smoke}' missing from the REST view listing: $(cat "${body}")"

    # Update: the effect that matters is the property actually landing, not merely a 200 status -
    # a no-op update route would still answer 200.
    local view_update_body='{"requirements":[{"type":"assert-view-uuid","uuid":"'"${view_uuid}"'"}],"updates":[{"action":"set-properties","updates":{"smoke":"updated"}}]}'
    code="$(rest_curl -o "${body}" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
      --data "${view_update_body}" "${HMS_SMOKE_REST_URL}/v1/${prefix}/namespaces/${namespace}/views/${view_smoke}")"
    [[ "${code}" == "200" ]] || fail "REST view update of '${view_smoke}' returned HTTP ${code}: $(cat "${body}")"

    code="$(rest_request GET "/v1/${prefix}/namespaces/${namespace}/views/${view_smoke}" "${body}")"
    [[ "${code}" == "200" ]] || fail "REST view reload of '${view_smoke}' returned HTTP ${code}: $(cat "${body}")"
    grep -q '"smoke"[[:space:]]*:[[:space:]]*"updated"' "${body}" \
      || fail "REST view update of '${view_smoke}' did not stick: $(cat "${body}")"

    # Rename: the pair below (200 under the new name, 404 under the old) is what proves the
    # rename moved the view rather than copying it - a bare 204 status would not distinguish
    # the two.
    code="$(rest_curl -o "${body}" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
      --data '{"source":{"namespace":["'"${namespace}"'"],"name":"'"${view_smoke}"'"},"destination":{"namespace":["'"${namespace}"'"],"name":"'"${renamed_view_smoke}"'"}}' \
      "${HMS_SMOKE_REST_URL}/v1/${prefix}/views/rename")"
    [[ "${code}" == "204" ]] || fail "REST rename of '${view_smoke}' returned HTTP ${code}: $(cat "${body}")"

    code="$(rest_request GET "/v1/${prefix}/namespaces/${namespace}/views/${renamed_view_smoke}" "${body}")"
    [[ "${code}" == "200" ]] || fail "REST load of renamed '${renamed_view_smoke}' returned HTTP ${code}: $(cat "${body}")"

    code="$(rest_request GET "/v1/${prefix}/namespaces/${namespace}/views/${view_smoke}" "${body}")"
    [[ "${code}" == "404" ]] \
      || fail "REST load of pre-rename view name '${view_smoke}' expected HTTP 404, got ${code}: $(cat "${body}")"

    code="$(rest_request DELETE "/v1/${prefix}/namespaces/${namespace}/views/${renamed_view_smoke}" "${body}")"
    [[ "${code}" == "204" ]] || fail "REST view drop of '${renamed_view_smoke}' returned HTTP ${code}: $(cat "${body}")"

    # Transaction commit round trip: a fresh table committed through
    # POST /v1/{prefix}/transactions/commit rather than the per-table commit route already
    # exercised above - the multi-table atomic-commit path is what phase 5a's write gate had a
    # critical bypass in (COMMIT_TRANSACTION naming a federated table), so it gets its own
    # positive proof here too. A no-op commit would still answer 204, so the check that matters is
    # the metadata-location actually changing, not the status code.
    local txn_table="${write_table}_txn"
    discard="$(rest_request DELETE "/v1/${prefix}/namespaces/${namespace}/tables/${txn_table}" "${body}")"

    code="$(rest_curl -o "${body}" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
      --data '{"name":"'"${txn_table}"'","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":false,"type":"int"}]}}' \
      "${HMS_SMOKE_REST_URL}/v1/${prefix}/namespaces/${namespace}/tables")"
    [[ "${code}" == "200" ]] || fail "REST create of '${txn_table}' returned HTTP ${code}: $(cat "${body}")"

    local txn_create_metadata_location=""
    txn_create_metadata_location="$(grep -o '"metadata-location"[[:space:]]*:[[:space:]]*"[^"]*"' "${body}" | head -n 1 | sed 's/.*"\([^"]*\)"$/\1/')"
    [[ -n "${txn_create_metadata_location}" ]] \
      || fail "REST create of '${txn_table}' carries no metadata-location: $(cat "${body}")"

    local txn_table_uuid=""
    txn_table_uuid="$(grep -o '"table-uuid"[[:space:]]*:[[:space:]]*"[^"]*"' "${body}" | head -n 1 | sed 's/.*"\([^"]*\)"$/\1/')"
    [[ -n "${txn_table_uuid}" ]] \
      || fail "REST create of '${txn_table}' carries no metadata.table-uuid: $(cat "${body}")"

    code="$(rest_curl -o "${body}" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
      --data '{"table-changes":[{"identifier":{"namespace":["'"${namespace}"'"],"name":"'"${txn_table}"'"},"requirements":[{"type":"assert-table-uuid","uuid":"'"${txn_table_uuid}"'"}],"updates":[{"action":"set-properties","updates":{"txn":"yes"}}]}]}' \
      "${HMS_SMOKE_REST_URL}/v1/${prefix}/transactions/commit")"
    [[ "${code}" == "204" ]] || fail "REST transaction commit of '${txn_table}' returned HTTP ${code}: $(cat "${body}")"

    code="$(rest_request GET "/v1/${prefix}/namespaces/${namespace}/tables/${txn_table}" "${body}")"
    [[ "${code}" == "200" ]] \
      || fail "REST load of '${txn_table}' after transaction commit returned HTTP ${code}: $(cat "${body}")"

    local txn_commit_metadata_location=""
    txn_commit_metadata_location="$(grep -o '"metadata-location"[[:space:]]*:[[:space:]]*"[^"]*"' "${body}" | head -n 1 | sed 's/.*"\([^"]*\)"$/\1/')"
    [[ -n "${txn_commit_metadata_location}" ]] \
      || fail "REST load of '${txn_table}' after transaction commit carries no metadata-location: $(cat "${body}")"
    [[ "${txn_commit_metadata_location}" != "${txn_create_metadata_location}" ]] \
      || fail "REST transaction commit of '${txn_table}' did not write a new metadata file: metadata-location is still '${txn_commit_metadata_location}'"

    code="$(rest_request DELETE "/v1/${prefix}/namespaces/${namespace}/tables/${txn_table}" "${body}")"
    [[ "${code}" =~ ^2 ]] || fail "REST drop of '${txn_table}' returned HTTP ${code}: $(cat "${body}")"

    # REGISTER_TABLE round trip: the last advertised write route without a positive proof. A
    # fresh table is created, dropped WITHOUT purge (the drop removes the HMS entry but leaves
    # the metadata file on the filesystem), then re-registered from that same metadata file. The
    # check that matters is the registered table loading back with a real metadata-location -
    # register is the route an operator uses to adopt an existing Iceberg table, so a route that
    # answered 200 without the table actually coming back would defeat its whole point.
    local reg_table="${write_table}_reg"
    discard="$(rest_request DELETE "/v1/${prefix}/namespaces/${namespace}/tables/${reg_table}" "${body}")"

    code="$(rest_curl -o "${body}" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
      --data '{"name":"'"${reg_table}"'","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":false,"type":"int"}]}}' \
      "${HMS_SMOKE_REST_URL}/v1/${prefix}/namespaces/${namespace}/tables")"
    [[ "${code}" == "200" ]] || fail "REST create of '${reg_table}' returned HTTP ${code}: $(cat "${body}")"

    local reg_metadata_location=""
    reg_metadata_location="$(grep -o '"metadata-location"[[:space:]]*:[[:space:]]*"[^"]*"' "${body}" | head -n 1 | sed 's/.*"\([^"]*\)"$/\1/')"
    [[ -n "${reg_metadata_location}" ]] \
      || fail "REST create of '${reg_table}' carries no metadata-location: $(cat "${body}")"

    code="$(rest_request DELETE "/v1/${prefix}/namespaces/${namespace}/tables/${reg_table}" "${body}")"
    [[ "${code}" =~ ^2 ]] || fail "REST non-purge drop of '${reg_table}' returned HTTP ${code}: $(cat "${body}")"

    code="$(rest_request GET "/v1/${prefix}/namespaces/${namespace}/tables/${reg_table}" "${body}")"
    [[ "${code}" == "404" ]] \
      || fail "REST load of dropped '${reg_table}' expected HTTP 404 before register, got ${code}: $(cat "${body}")"

    code="$(rest_curl -o "${body}" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
      --data '{"name":"'"${reg_table}"'","metadata-location":"'"${reg_metadata_location}"'"}' \
      "${HMS_SMOKE_REST_URL}/v1/${prefix}/namespaces/${namespace}/register")"
    [[ "${code}" == "200" ]] || fail "REST register of '${reg_table}' returned HTTP ${code}: $(cat "${body}")"
    grep -q '"metadata-location"' "${body}" \
      || fail "REST register of '${reg_table}' carries no metadata-location: $(cat "${body}")"

    code="$(rest_request GET "/v1/${prefix}/namespaces/${namespace}/tables/${reg_table}" "${body}")"
    [[ "${code}" == "200" ]] || fail "REST load of registered '${reg_table}' returned HTTP ${code}: $(cat "${body}")"
    grep -q '"metadata-location"' "${body}" \
      || fail "REST load of registered '${reg_table}' carries no metadata-location: $(cat "${body}")"

    code="$(rest_request DELETE "/v1/${prefix}/namespaces/${namespace}/tables/${reg_table}" "${body}")"
    [[ "${code}" =~ ^2 ]] || fail "REST drop of registered '${reg_table}' returned HTTP ${code}: $(cat "${body}")"
  else
    log "skipping REST write round trip because HMS_SMOKE_REST_WRITE_TABLE is not set"
  fi

  local metrics_url="${HMS_SMOKE_REST_METRICS_URL:-}"
  if [[ -n "${metrics_url}" ]]; then
    curl -sS -o "${body}" "${metrics_url}" || fail "cannot fetch metrics from ${metrics_url}"
    grep -q 'hms_proxy_rest_requests_total{' "${body}" \
      || fail "metrics endpoint carries no hms_proxy_rest_requests_total series"
    grep -q 'hms_proxy_rest_listener_info{' "${body}" \
      || fail "metrics endpoint carries no hms_proxy_rest_listener_info series"
  fi

  log "Iceberg REST smoke passed (prefix '${prefix}', namespace '${namespace}')"
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --env-file)
        [[ $# -ge 2 ]] || fail "missing value for --env-file"
        ENV_FILE="$2"
        shift 2
        ;;
      --scenario)
        [[ $# -ge 2 ]] || fail "missing value for --scenario"
        SCENARIO="$2"
        shift 2
        ;;
      --help|-h)
        usage
        exit 0
        ;;
      *)
        fail "unknown argument: $1"
        ;;
    esac
  done
}

main() {
  parse_args "$@"

  if [[ -n "${ENV_FILE}" ]]; then
    log "loading env file ${ENV_FILE}"
    load_env_file "${ENV_FILE}"
  elif [[ -f "${DEFAULT_ENV_FILE}" ]]; then
    log "loading default env file ${DEFAULT_ENV_FILE}"
    load_env_file "${DEFAULT_ENV_FILE}"
  fi

  resolve_fat_jar
  build_java_cmd
  build_common_args

  case "${SCENARIO}" in
    all)
      run_sql_smoke_all
      run_txn_smoke
      run_db_lock_smoke
      run_partition_lock_smoke
      run_cross_catalog_lock_smoke
      run_notification_smoke
      run_rest_smoke
      ;;
    sql)
      run_sql_smoke_all
      ;;
    txn)
      run_txn_smoke
      ;;
    locks)
      run_db_lock_smoke
      run_partition_lock_smoke
      run_cross_catalog_lock_smoke
      ;;
    notification)
      run_notification_smoke
      ;;
    rest)
      run_rest_smoke
      ;;
    *)
      fail "unsupported scenario '${SCENARIO}'. Expected one of: all, sql, txn, locks, notification, rest"
      ;;
  esac

  log "scenario '${SCENARIO}' completed successfully"
}

main "$@"
