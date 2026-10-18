#!/usr/bin/env bash
# Tags: atomic-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# `deltaQuery` and `deltaQueryFrontier` need the same grants as running their query, since the frontier exposes the
# partitions and block numbers of its tables.

USER="u_${CLICKHOUSE_TEST_UNIQUE_NAME}_$$"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS enable_block_number_column = 1;
    INSERT INTO t VALUES (1);
    DROP USER IF EXISTS ${USER};
    CREATE USER ${USER};"

FRONTIER="SELECT * FROM deltaQueryFrontier((SELECT k FROM t))"
T_UUID=$($CLICKHOUSE_CLIENT -q "SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 't'")
DELTA="SELECT * FROM deltaQuery((SELECT k FROM t), map(), map('${T_UUID}.all.block_number', 2))"
denied() { grep -qF "necessary to have the grant SELECT ON ${CLICKHOUSE_DATABASE}.t" && echo "ACCESS_DENIED"; }

echo "Without SELECT"
$CLICKHOUSE_CLIENT --user "${USER}" -q "$FRONTIER" 2>&1 | denied
$CLICKHOUSE_CLIENT --user "${USER}" -q "$DELTA" 2>&1 | denied

echo "With SELECT"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t TO ${USER}"
$CLICKHOUSE_CLIENT --user "${USER}" -q "$FRONTIER" | sed "s/${T_UUID}/uuid/"
$CLICKHOUSE_CLIENT --user "${USER}" -q "$DELTA"

$CLICKHOUSE_CLIENT -q "DROP USER ${USER}"
