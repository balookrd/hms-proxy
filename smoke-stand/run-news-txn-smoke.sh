#!/usr/bin/env bash
# Smoke test for inserting into a partitioned transactional (ACID) table
# replicating the query:
#   INSERT INTO b2b_mass_pers_dds.news PARTITION (source = 'crawler')
#       SELECT 
#           article_identity_fingerprint AS id,
#           title AS title,
#           excerpt AS annotation,
#           CAST(COALESCE(published_at, CURRENT_DATE) AS date) AS news_date,
#           url AS url,
#           NULL AS region,
#           NULL AS industry,
#           current_timestamp AS ts,
#           NULL AS picture,
#           parse_url(url, 'HOST') AS host
#       FROM b2b_mass_pers_dds.news_crawler_stg
set -euo pipefail

STAND_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log() {
  printf '[news-txn-smoke] %s\n' "$*"
}

fail() {
  printf '[news-txn-smoke] ERROR: %s\n' "$*" >&2
  exit 1
}

CONTAINER_NAME="${CONTAINER_NAME:-stand-hs2-hdp}"
BEELINE_JDBC_URL="${BEELINE_JDBC_URL:-jdbc:hive2://localhost:10000/default}"
BEELINE_USER="${BEELINE_USER:-hive}"
TEST_DB="${TEST_DB:-b2b_mass_pers_dds}"
STG_TABLE="${STG_TABLE:-news_crawler_stg}"
TXN_TABLE="${TXN_TABLE:-news}"

RUN_ID="$(date +%s)_$$"
SQL_FILE="/tmp/news_txn_smoke_${RUN_ID}.sql"
OUT_FILE="/tmp/news_txn_smoke_${RUN_ID}.out"

cat <<EOF > "${SQL_FILE}"
set hive.cli.print.header=true;
set hive.execution.engine=mr;
set hive.vectorized.execution.enabled=false;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.stats.autogather=true;

-- 1. Setup database
create database if not exists ${TEST_DB};
use ${TEST_DB};

-- 2. Drop existing tables if any
drop table if exists ${TXN_TABLE};
drop table if exists ${STG_TABLE};

-- 3. Create source staging table
create table if not exists ${STG_TABLE} (
    article_identity_fingerprint string,
    title string,
    excerpt string,
    published_at string,
    url string
) stored as orc;

-- 4. Insert test data into staging
insert into ${STG_TABLE} values (
    'fp-smoke-1001',
    'Sample News Title',
    'Sample News Excerpt for Smoke Test',
    '2026-09-17',
    'https://news.yandex.ru/rubric/article-1001.html'
);

-- 4b. Verify SELECT * from non-transactional staging table under DbTxnManager
-- (HiveServer2 issues get_valid_write_ids with empty fullTableNames: [] for this query)
select * from ${STG_TABLE};

-- 5. Create partitioned transactional table
create table if not exists ${TXN_TABLE} (
    id string,
    title string,
    annotation string,
    news_date date,
    url string,
    region string,
    industry string,
    ts timestamp,
    picture string,
    host string
)
partitioned by (source string)
clustered by (id) into 2 buckets
stored as orc
tblproperties ('transactional'='true', 'smoke'='true');

-- 6. Execute user target query (exercises StatsTask and set_aggr_stats_for / update_table_column_statistics_req)
INSERT INTO ${TEST_DB}.${TXN_TABLE} PARTITION (source = 'crawler')
    SELECT 
        article_identity_fingerprint AS id,
        title AS title,
        excerpt AS annotation,
        CAST(COALESCE(published_at, CURRENT_DATE) AS date) AS news_date,
        url AS url,
        NULL AS region,
        NULL AS industry,
        current_timestamp AS ts,
        NULL AS picture,
        parse_url(url, 'HOST') AS host
    FROM ${TEST_DB}.${STG_TABLE};

-- 6b. Verify SELECT * from transactional table under DbTxnManager
select * from ${TEST_DB}.${TXN_TABLE};

-- 7. Verify inserted data
select
    'NEWS_TXN_SMOKE_COUNT_OK' as count_marker,
    count(*) as row_count,
    source,
    host,
    id
from ${TEST_DB}.${TXN_TABLE}
where source = 'crawler'
group by source, host, id;

-- 8. Cleanup
drop table ${TXN_TABLE};
drop table ${STG_TABLE};
EOF

log "Generated SQL script for test in ${SQL_FILE}"

if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
  log "Running smoke test inside docker container '${CONTAINER_NAME}'..."
  docker cp "${SQL_FILE}" "${CONTAINER_NAME}:/tmp/news_txn_test.sql"
  
  BEELINE_BIN="/usr/hdp/3.1.0.0-78/hive/bin/beeline"
  docker exec "${CONTAINER_NAME}" test -x "${BEELINE_BIN}" || BEELINE_BIN="/opt/hs2/bin/beeline"
  docker exec "${CONTAINER_NAME}" test -x "${BEELINE_BIN}" || BEELINE_BIN="beeline"

  docker exec "${CONTAINER_NAME}" bash -c \
    "${BEELINE_BIN} -u '${BEELINE_JDBC_URL}' -n '${BEELINE_USER}' --showHeader=false --outputformat=tsv2 -f /tmp/news_txn_test.sql" \
    | tee "${OUT_FILE}"
else
  log "Container '${CONTAINER_NAME}' not found running; running local beeline..."
  beeline -u "${BEELINE_JDBC_URL}" -n "${BEELINE_USER}" --showHeader=false --outputformat=tsv2 -f "${SQL_FILE}" \
    | tee "${OUT_FILE}"
fi

log "Validating test results..."
if grep -E "^NEWS_TXN_SMOKE_COUNT_OK[[:space:]]+1[[:space:]]+crawler[[:space:]]+news\.yandex\.ru[[:space:]]+fp-smoke-1001" "${OUT_FILE}" >/dev/null; then
  log "SUCCESS: Transactional partition insert verified with expected data!"
else
  fail "Verification marker 'NEWS_TXN_SMOKE_COUNT_OK' not found in output."
fi

# Cleanup host temp files
rm -f "${SQL_FILE}" "${OUT_FILE}"
log "Smoke test finished successfully."
