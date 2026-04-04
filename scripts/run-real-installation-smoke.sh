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
  ${RUNNER_NAME} [--env-file /path/to/file.env] [--scenario all|sql|txn|locks|notification]

Behavior:
  - loads HMS_SMOKE_* settings from --env-file or from ${DEFAULT_ENV_FILE} when present
  - falls back to current environment variables if no env-file is provided
  - uses target/hms-proxy-*-fat.jar unless HMS_SMOKE_FAT_JAR is set
  - exits on the first failed smoke step

Scenarios:
  all           run optional beeline SQL smoke + txn + non-default DB lock + optional partition lock + optional notification
  sql           run only beeline / HiveServer2 SQL smoke from SMOKE.md
  txn           run only the direct ACID/txn smoke
  locks         run only the non-default catalog lock smoke
  notification  run only Hortonworks add_write_notification_log smoke

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
EOF

  cat <<'EOF'

Optional beeline / SQL env vars:
  HMS_SMOKE_BEELINE_JDBC_URL
  HMS_SMOKE_BEELINE_BIN                  default: beeline
  HMS_SMOKE_BEELINE_USER                 optional
  HMS_SMOKE_BEELINE_PASSWORD             optional
  HMS_SMOKE_HDP_CATALOG                  default: hdp
  HMS_SMOKE_APACHE_CATALOG               default: apache
  HMS_SMOKE_HDP_READ_TABLE               required for SQL smoke
  HMS_SMOKE_APACHE_READ_TABLE            required for SQL smoke
  HMS_SMOKE_SQL_EXTERNAL_ROOT            default: /tmp/hms-proxy-smoke
  HMS_SMOKE_SQL_RUN_MATERIALIZED_VIEW    default: false
  HMS_SMOKE_HDP_RUN_TRANSACTIONAL_SQL    default: false
  HMS_SMOKE_APACHE_RUN_TRANSACTIONAL_SQL default: false
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
    grep -F "requires a Hortonworks backend runtime" <<< "${output}" >/dev/null \
      || fail "${label} failed, but did not mention the expected Hortonworks runtime error"
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

run_sql_smoke() {
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
  local run_id=""
  run_id="$(date +%Y%m%d%H%M%S)"
  local managed_hdp="smoke_managed_hdp_${run_id}"
  local managed_apache="smoke_managed_apache_${run_id}"
  local external_hdp="smoke_external_hdp_${run_id}"
  local external_apache="smoke_external_apache_${run_id}"
  local txn_hdp="smoke_txn_hdp_${run_id}"
  local txn_apache="smoke_txn_apache_${run_id}"
  local view_local="smoke_view_local_${run_id}"
  local view_cross="smoke_view_cross_${run_id}"
  local mv_local="smoke_mv_local_${run_id}"
  local sql_file=""
  local output_file=""
  sql_file="$(mktemp "${TMPDIR:-/tmp}/hms-proxy-sql-smoke.XXXXXX.sql")"
  output_file="$(mktemp "${TMPDIR:-/tmp}/hms-proxy-sql-smoke.XXXXXX.out")"
  trap 'rm -f "${sql_file}" "${output_file}"' RETURN

  beeline_run_maybe_kinit

  cat > "${sql_file}" <<EOF
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
drop table ${managed_hdp};

create external table if not exists ${external_hdp} (
  id int,
  ds string
)
stored as parquet
location '${external_root}/${hdp_catalog}/external/${external_hdp}';
alter table ${external_hdp} set tblproperties ('smoke'='true', 'table_kind'='external');
insert into ${external_hdp} values (2, '2026-03-31');
select count(*) as ${external_hdp}_count from ${external_hdp} where id=2;
describe formatted ${external_hdp};
drop table ${external_hdp};
EOF

  if [[ "${HMS_SMOKE_HDP_RUN_TRANSACTIONAL_SQL:-false}" == "true" ]]; then
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
drop table ${managed_apache};

create external table if not exists ${external_apache} (
  id int,
  ds string
)
stored as parquet
location '${external_root}/${apache_catalog}/external/${external_apache}';
alter table ${external_apache} set tblproperties ('smoke'='true', 'table_kind'='external');
insert into ${external_apache} values (2, '2026-03-31');
select count(*) as ${external_apache}_count from ${external_apache} where id=2;
describe formatted ${external_apache};
drop table ${external_apache};
EOF

  if [[ "${HMS_SMOKE_APACHE_RUN_TRANSACTIONAL_SQL:-false}" == "true" ]]; then
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

  if [[ "${HMS_SMOKE_SQL_RUN_MATERIALIZED_VIEW:-false}" == "true" ]]; then
    cat >> "${sql_file}" <<EOF
create materialized view if not exists ${mv_local} as
select * from ${hdp_catalog}__default.${HMS_SMOKE_HDP_READ_TABLE};
show create table ${mv_local};
describe formatted ${mv_local};
drop materialized view if exists ${mv_local};
EOF
  fi

  cat >> "${sql_file}" <<EOF

use ${hdp_catalog}__default;
show tables;
use ${apache_catalog}__default;
select count(*) as post_switch_apache_count from ${HMS_SMOKE_APACHE_READ_TABLE};
use ${hdp_catalog}__default;
show tables;
EOF

  run_beeline_script "beeline SQL smoke" "${sql_file}" "${output_file}"

  assert_file_contains "${output_file}" "${hdp_catalog}__default"
  assert_file_contains "${output_file}" "${apache_catalog}__default"
  assert_file_contains "${output_file}" "2026-04-01"
  assert_file_contains "${output_file}" "${hdp_catalog}__default.${HMS_SMOKE_HDP_READ_TABLE}"
  assert_file_contains "${output_file}" "${apache_catalog}__default.${HMS_SMOKE_APACHE_READ_TABLE}"
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
      run_sql_smoke
      run_txn_smoke
      run_db_lock_smoke
      run_partition_lock_smoke
      run_notification_smoke
      ;;
    sql)
      run_sql_smoke
      ;;
    txn)
      run_txn_smoke
      ;;
    locks)
      run_db_lock_smoke
      run_partition_lock_smoke
      ;;
    notification)
      run_notification_smoke
      ;;
    *)
      fail "unsupported scenario '${SCENARIO}'. Expected one of: all, sql, txn, locks, notification"
      ;;
  esac

  log "scenario '${SCENARIO}' completed successfully"
}

main "$@"
