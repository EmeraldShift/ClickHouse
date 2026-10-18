#!/usr/bin/env bash

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
DELTA="SELECT * FROM deltaQuery((SELECT k FROM t), map('${CLICKHOUSE_DATABASE}.t', map()), map('${CLICKHOUSE_DATABASE}.t', map('all', 1)))"
denied() { grep -qF "necessary to have the grant SELECT ON ${CLICKHOUSE_DATABASE}.t" && echo "ACCESS_DENIED"; }

echo "Without SELECT"
$CLICKHOUSE_CLIENT --user "${USER}" -q "$FRONTIER" 2>&1 | denied
$CLICKHOUSE_CLIENT --user "${USER}" -q "$DELTA" 2>&1 | denied

echo "With SELECT"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t TO ${USER}"
$CLICKHOUSE_CLIENT --user "${USER}" -q "$FRONTIER" | sed "s/${CLICKHOUSE_DATABASE}/db/"
$CLICKHOUSE_CLIENT --user "${USER}" -q "$DELTA"

$CLICKHOUSE_CLIENT -q "DROP USER ${USER}"
