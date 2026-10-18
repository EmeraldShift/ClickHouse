#!/usr/bin/env bash
# Tags: atomic-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# `deltaQuery` and `deltaQueryFrontier` need the grants that running their query needs, since the frontier exposes the
# partitions and block numbers of its tables. `deltaQuery` also reads the virtual columns `_block_number`, `_partition_id`
# and `_part` of its tables.

USER="u_${CLICKHOUSE_TEST_UNIQUE_NAME}_$$"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t (k UInt64, secret UInt64) ENGINE = MergeTree ORDER BY k SETTINGS enable_block_number_column = 1;
    INSERT INTO t VALUES (1, 2);
    DROP USER IF EXISTS ${USER};
    CREATE USER ${USER};"

T_UUID=$($CLICKHOUSE_CLIENT -q "SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 't'")
FRONTIER="SELECT * FROM deltaQueryFrontier((SELECT k FROM t))"
SECRET_FRONTIER="SELECT * FROM deltaQueryFrontier((SELECT secret FROM t))"
DELTA="SELECT * FROM deltaQuery((SELECT k FROM t), map(), map('${T_UUID}.all.block_number', 2))"

# Prints the result of the query run by the user, or ACCESS_DENIED.
run() {
    local output
    if output=$($CLICKHOUSE_CLIENT --user "${USER}" -q "$1" 2>&1); then
        echo "${output//${T_UUID}/uuid}"
    elif grep -qF ACCESS_DENIED <<< "$output"; then
        echo ACCESS_DENIED
    else
        echo "$output"
    fi
}

echo "Without grants"
run "$FRONTIER"
run "$DELTA"

echo "With SHOW COLUMNS"
$CLICKHOUSE_CLIENT -q "GRANT SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.t TO ${USER}"
run "$FRONTIER"

echo "With SELECT(k)"
$CLICKHOUSE_CLIENT -q "GRANT SELECT(k) ON ${CLICKHOUSE_DATABASE}.t TO ${USER}"
run "$FRONTIER"
run "$SECRET_FRONTIER"
run "$DELTA"

echo "With SELECT(k) and the virtual columns"
$CLICKHOUSE_CLIENT -q "GRANT SELECT(_block_number, _partition_id, _part) ON ${CLICKHOUSE_DATABASE}.t TO ${USER}"
run "$DELTA"

echo "With SELECT"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t TO ${USER}"
run "$SECRET_FRONTIER"

$CLICKHOUSE_CLIENT -q "DROP USER ${USER}"
