#!/usr/bin/env bash
# Tags: atomic-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The merges of the stage table of an incremental view sum the signs of equal rows. After OPTIMIZE FINAL, the stage table
# holds one row per user, although each refresh retracted the old count of each user. The view stays equal to its query.

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE events (user UInt64) ENGINE = MergeTree ORDER BY user SETTINGS enable_block_number_column = 1;
    CREATE MATERIALIZED VIEW histogram REFRESH EVERY 10 YEAR APPEND INCREMENTAL ENGINE = SummingMergeTree ORDER BY n EMPTY AS
        SELECT n, count() AS users FROM (SELECT user, count() AS n FROM events GROUP BY user) GROUP BY n
        SETTINGS delta_query_sign_column = 'sign';"

refresh() { $CLICKHOUSE_CLIENT -q "SYSTEM REFRESH VIEW histogram; SYSTEM WAIT VIEW histogram;"; }

for _ in 1 2 3; do
    $CLICKHOUSE_CLIENT -q "INSERT INTO events VALUES (0), (1), (2)"
    refresh
done

stage=$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_stage.%'")
$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE \`$stage\` FINAL"
$CLICKHOUSE_CLIENT -q "SELECT 'stage rows', count(), sum(sign) FROM \`$stage\`"

$CLICKHOUSE_CLIENT -q "INSERT INTO events VALUES (0)"
refresh
$CLICKHOUSE_CLIENT -q "
    SELECT 'histogram', groupArray((n, users)) FROM (SELECT n, sum(users) AS users FROM histogram GROUP BY n HAVING users != 0 ORDER BY n);
    SELECT 'expected', groupArray((n, users)) FROM (SELECT n, count() AS users FROM (SELECT user, count() AS n FROM events GROUP BY user) GROUP BY n ORDER BY n);"
