#!/usr/bin/env bash
# Tags: atomic-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The merges of the stage table of an incremental view sum the signs of equal rows. After OPTIMIZE FINAL, the stage table
# holds one row per user, although each refresh retracted the old count of each user. The view stays equal to its query.
# A stage table that sums 200 equal rows into one row of weight 200 gives the view 200 rows of sign 1.

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE events (user UInt64) ENGINE = MergeTree ORDER BY user SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;
    CREATE MATERIALIZED VIEW histogram REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = SummingMergeTree ORDER BY n EMPTY AS
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

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE signed (k UInt64, v UInt64, sign Int8) ENGINE = MergeTree ORDER BY k
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;
    CREATE MATERIALIZED VIEW keys REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = MergeTree ORDER BY k EMPTY AS
        SELECT k FROM (SELECT k FROM signed) SETTINGS delta_query_sign_column = 'sign';
    INSERT INTO signed SELECT 1, number, 1 FROM numbers(200);
    SYSTEM REFRESH VIEW keys; SYSTEM WAIT VIEW keys;
    SELECT 'keys', count(), sum(sign), max(abs(sign)) FROM keys;"

# The stage table of a linear stage adds up the partial sums of each account. After OPTIMIZE FINAL, it holds one row per
# account. The view stays equal to its query when only the rates change, and after a mutation corrects the stage table.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE facts (grp UInt64, amt Int64) ENGINE = MergeTree ORDER BY grp SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;
    CREATE TABLE rates (grp UInt64, w Float64, sign Int8) ENGINE = MergeTree ORDER BY grp
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;
    INSERT INTO rates VALUES (0, 1, 1), (1, 1, 1);
    CREATE MATERIALIZED VIEW total REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
        SELECT sum(p.q * m.w) AS total, sum(p.n) AS n
        FROM (SELECT grp, sum(amt) AS q, count() AS n FROM facts GROUP BY grp) AS p JOIN rates AS m ON p.grp = m.grp
        SETTINGS delta_query_sign_column = 'sign';"

refresh_total() { $CLICKHOUSE_CLIENT -q "SYSTEM REFRESH VIEW total; SYSTEM WAIT VIEW total;"; }
check_total() {
    $CLICKHOUSE_CLIENT -q "
        SELECT '$1', sum(total), sum(n) FROM total;
        SELECT '$1 expected', sum(p.q * m.w), sum(p.n) FROM (SELECT grp, sum(amt) AS q, count() AS n FROM facts GROUP BY grp) AS p
            JOIN (SELECT grp, w FROM rates GROUP BY grp, w HAVING sum(sign) > 0) AS m ON p.grp = m.grp;"
}

for r in 1 2 3; do
    $CLICKHOUSE_CLIENT -q "INSERT INTO facts VALUES (0, $r), (1, $((10 * r)))"
    refresh_total
done
stage=$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_stage.%' AND engine_full LIKE 'SummingMergeTree((%'")
$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE \`$stage\` FINAL"
$CLICKHOUSE_CLIENT -q "SELECT 'linear stage rows', count() FROM \`$stage\`"

$CLICKHOUSE_CLIENT -q "INSERT INTO rates VALUES (0, 1, -1), (0, 2, 1)"
refresh_total
check_total 'new rate'

$CLICKHOUSE_CLIENT -q "ALTER TABLE facts DELETE WHERE amt = 20 SETTINGS mutations_sync = 2"
refresh_total
check_total 'corrected'
