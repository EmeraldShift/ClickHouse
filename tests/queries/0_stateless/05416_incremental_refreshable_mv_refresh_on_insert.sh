#!/usr/bin/env bash
# Tags: atomic-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# With refresh_on_insert, an incremental view refreshes after an insert into an input, although its schedule is yearly.
# An insert into a lookup table doesn't refresh it.

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE facts (id UInt64, dim_id UInt64, v Int64) ENGINE = MergeTree ORDER BY id SETTINGS enable_block_number_column = 1;
    CREATE TABLE dims (id UInt64, name String) ENGINE = MergeTree ORDER BY id SETTINGS enable_block_number_column = 1;
    INSERT INTO dims VALUES (1, 'a');
    CREATE MATERIALIZED VIEW v REFRESH EVERY 1 YEAR SETTINGS refresh_on_insert = 1 APPEND INCREMENTAL
        ENGINE = SummingMergeTree ORDER BY name AS
        SELECT d.name AS name, sum(f.v) AS v FROM facts AS f INNER JOIN dims AS d ON d.id = f.dim_id GROUP BY name
        SETTINGS delta_query_lookup_tables = 'dims';
    SYSTEM WAIT VIEW v;"

# Prints the rows of the view once they equal `$1`, or the rows after 30 seconds.
wait_for_rows()
{
    local rows
    for _ in $(seq 300); do
        rows=$($CLICKHOUSE_CLIENT -q "SELECT name, sum(v) FROM v GROUP BY name ORDER BY name FORMAT CSV" | tr '\n' ' ')
        [ "$rows" == "$1" ] && break
        sleep 0.1
    done
    echo "$rows"
}
cursor() { $CLICKHOUSE_CLIENT -q "SELECT cursor FROM system.view_refreshes WHERE database = currentDatabase() AND view = 'v'"; }

$CLICKHOUSE_CLIENT -q "INSERT INTO facts VALUES (1, 1, 10)"
echo "after a fact: $(wait_for_rows '"a",10 ')"

before=$(cursor)
$CLICKHOUSE_CLIENT -q "INSERT INTO dims VALUES (2, 'b')"
sleep 2
[ "$before" == "$(cursor)" ] && echo "after a dimension: no refresh" || echo "after a dimension: refreshed"

$CLICKHOUSE_CLIENT -q "INSERT INTO facts VALUES (2, 2, 5)"
echo "after a fact of the new dimension: $(wait_for_rows '"a",10 "b",5 ')"
