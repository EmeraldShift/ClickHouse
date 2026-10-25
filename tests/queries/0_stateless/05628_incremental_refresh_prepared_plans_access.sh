#!/usr/bin/env bash
# Tags: atomic-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# An incremental refresh that reuses the analysis and the plan of an earlier refresh skips the checks of the analysis. It
# still fails after a REVOKE from the definer, and filters the new rows by a row policy added later.

USER="u_${CLICKHOUSE_TEST_UNIQUE_NAME}_$$"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS enable_block_number_column = 1;
    DROP USER IF EXISTS ${USER};
    CREATE USER ${USER};
    GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.* TO ${USER};
    CREATE MATERIALIZED VIEW v REFRESH EVERY 10 YEAR SETTINGS refresh_retries = 0 INCREMENTAL
        ENGINE = MergeTree ORDER BY k EMPTY DEFINER = ${USER} SQL SECURITY DEFINER AS SELECT k FROM t;"

refresh() {
    $CLICKHOUSE_CLIENT -q "INSERT INTO t SELECT number FROM numbers($1, 10); SYSTEM REFRESH VIEW v;"
    $CLICKHOUSE_CLIENT -q "SYSTEM WAIT VIEW v" 2>&1 | grep -qF ACCESS_DENIED && echo ACCESS_DENIED
    $CLICKHOUSE_CLIENT -q "SELECT '$2', count() FROM v"
}

refresh 0 "first refresh"
refresh 10 "second refresh"
refresh 20 "third refresh"
$CLICKHOUSE_CLIENT -q "REVOKE SELECT ON ${CLICKHOUSE_DATABASE}.t FROM ${USER}"
refresh 30 "after REVOKE"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t TO ${USER}"
refresh 40 "after GRANT"
refresh 50 "after GRANT again"
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY p_${CLICKHOUSE_TEST_UNIQUE_NAME} ON ${CLICKHOUSE_DATABASE}.t USING k % 2 = 0 TO ${USER}"
refresh 60 "after a row policy"

# Which refreshes ran a plan of an earlier one.
$CLICKHOUSE_CLIENT -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT groupArray(ProfileEvents['InsertSelectReusedPlans']) FROM (SELECT ProfileEvents FROM system.query_log
        WHERE event_date >= yesterday() AND type = 'QueryFinish' AND log_comment = 'refresh of ' || currentDatabase() || '.v'
        ORDER BY event_time_microseconds);
    DROP ROW POLICY p_${CLICKHOUSE_TEST_UNIQUE_NAME} ON ${CLICKHOUSE_DATABASE}.t;
    DROP TABLE v;
    DROP USER ${USER};"
