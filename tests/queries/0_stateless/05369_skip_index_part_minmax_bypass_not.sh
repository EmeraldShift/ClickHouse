#!/usr/bin/env bash
# Tags: no-parallel-replicas, no-random-settings, no-random-merge-tree-settings
# Tag no-parallel-replicas: the index analysis that writes the log runs on the coordinator.
# Tag no-random-*-settings: the bypass needs the index to be evaluated before reading, and a single part.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# On the single part `c` is in [0, 999]. The skip index on `c` cannot evaluate `v = 3`, but it keeps every granule
# whatever `v = 3` is when the rest of its condition is certainly true on the part. Then it is not evaluated on
# the granules. That holds also when the unknown atom is under `NOT`.

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS tab;
    CREATE TABLE tab (c Int32, v Int32, INDEX c_index c TYPE minmax)
    ENGINE = MergeTree ORDER BY tuple() PARTITION BY intDiv(c, 1000)
    SETTINGS index_granularity = 10, index_granularity_bytes = 0, add_minmax_index_for_numeric_columns = 0;
    INSERT INTO tab SELECT number, number % 7 FROM numbers(1000);"

for condition in "c < 2000 AND v = 3" "NOT (c >= 2000 OR v = 3)" "NOT (c >= 500 OR v = 3)"
do
    bypassed=$($CLICKHOUSE_CLIENT --send_logs_level=trace --use_skip_indexes_on_data_read=0 --use_query_condition_cache=0 \
        -q "SELECT count() FROM tab WHERE $condition FORMAT Null" 2>&1 \
        | grep -c "Skipping granule-level evaluation of skip index")
    echo "$condition: bypassed $bypassed"
    $CLICKHOUSE_CLIENT -q "
        SELECT count() FROM tab WHERE $condition;
        SELECT count() FROM tab WHERE $condition SETTINGS use_skip_indexes = 0;"
done

$CLICKHOUSE_CLIENT -q "DROP TABLE tab"
