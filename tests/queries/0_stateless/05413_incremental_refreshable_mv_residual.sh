#!/usr/bin/env bash
# Tags: atomic-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Maintain queries with residuals in incremental refreshable materialized views: HAVING, expressions over aggregates, avg,
# a LIMIT over all groups, LIMIT BY, a window with QUALIFY, DISTINCT, aggregates like uniqExact over an unsigned table,
# sums and averages of Nullable values, a global aggregate over rows that all fail WHERE, and residuals over the rows of
# an unsigned table, which keep no integral.
# After each round of inserts, deletes and updates, compare the net rows of each view with its query over the rows that no
# row of sign -1 cancels.

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE orders (id UInt64, product String, qty Int64, sign Int8)
        ENGINE = MergeTree ORDER BY id SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;
    CREATE TABLE quotes (order_id UInt64, price Int64, sign Int8)
        ENGINE = MergeTree ORDER BY order_id SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;
    CREATE TABLE products (product String, category String)
        ENGINE = MergeTree ORDER BY product SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;
    CREATE TABLE events (k UInt8, v UInt32) ENGINE = MergeTree ORDER BY k SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;
    CREATE TABLE readings (k UInt8, v Nullable(Int64), sign Int8)
        ENGINE = MergeTree ORDER BY k SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;
"

# Each query reads the tables \$1, \$2 and \$3 in place of orders, quotes and readings.
q1() { echo "SELECT p.category AS category, sum(o.qty) AS qty, round(avg(q.price), 2) AS avg_price
    FROM $1 AS o INNER JOIN $2 AS q ON q.order_id = o.id INNER JOIN products AS p ON p.product = o.product
    GROUP BY category HAVING count() > 1"; }
q2() { echo "SELECT o.id AS id, sum(o.qty * q.price) AS value FROM $1 AS o INNER JOIN $2 AS q ON q.order_id = o.id
    GROUP BY id ORDER BY value DESC, id LIMIT 2"; }
q3() { echo "SELECT p.category AS category, o.id AS id, sum(o.qty) AS qty FROM $1 AS o INNER JOIN products AS p ON p.product = o.product
    GROUP BY category, id ORDER BY qty DESC, id LIMIT 1 BY category"; }
q4() { echo "SELECT p.category AS category, o.id AS id, o.qty AS qty, rank() OVER (PARTITION BY category ORDER BY qty DESC) AS r
    FROM $1 AS o INNER JOIN products AS p ON p.product = o.product QUALIFY r <= 2"; }
q5() { echo "SELECT DISTINCT o.product AS product FROM $1 AS o"; }
q6() { echo "SELECT k, uniqExact(v) AS u, min(v) AS m, max(v) - min(v) AS spread FROM events GROUP BY k HAVING count() >= 2"; }
q7() { echo "SELECT k, sum(v) + 1 AS s, avg(v) AS a, avgIf(v, v > 0) AS ai FROM ${3:-readings} GROUP BY k HAVING count() > 0"; }
q8() { echo "SELECT count() + 1 AS c, sum(v) AS s FROM ${3:-readings} WHERE k > 100"; }
# Residuals over the rows of the unsigned table events keep no integral.
q9() { echo "SELECT k, v FROM events ORDER BY v DESC, k LIMIT 3"; }
q10() { echo "SELECT k, v FROM events ORDER BY v DESC LIMIT 1 BY k"; }
q11() { echo "SELECT k, v, row_number() OVER (PARTITION BY k ORDER BY v) AS rn FROM events QUALIFY rn <= 2"; }
q12() { echo "SELECT DISTINCT v FROM events"; }

VIEWS="1 2 3 4 5 6 7 8 9 10 11 12"
COLUMNS=("category, qty, avg_price" "id, value" "category, id, qty" "category, id, qty, r" "product" "k, u, m, spread" "k, s, a, ai" "c, s" "k, v" "k, v" "k, v, rn" "v")
NET_ORDERS="(SELECT id, product, qty FROM orders GROUP BY ALL HAVING sum(sign) > 0)"
NET_QUOTES="(SELECT order_id, price FROM quotes GROUP BY ALL HAVING sum(sign) > 0)"
NET_READINGS="(SELECT k, v FROM readings GROUP BY ALL HAVING sum(sign) > 0)"
S="SETTINGS delta_query_sign_column = 'sign'"

for i in $VIEWS; do
    $CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW view_$i REFRESH EVERY 10 YEAR APPEND INCREMENTAL
        ENGINE = CollapsingMergeTree(sign) ORDER BY (${COLUMNS[$((i - 1))]}) SETTINGS allow_nullable_key = 1
        EMPTY AS $(q$i orders quotes) $S"
done

# Refresh the views and compare the net rows of each with its query over the net inputs.
end_round()
{
    for i in $VIEWS; do
        $CLICKHOUSE_CLIENT -q "SYSTEM REFRESH VIEW view_$i; SYSTEM WAIT VIEW view_$i;"
        local columns=${COLUMNS[$((i - 1))]}
        $CLICKHOUSE_CLIENT -q "
            SELECT 'round $1', 'view $i',
                (SELECT groupArray(toString(row)) FROM (SELECT tuple($columns, n) AS row FROM
                    (SELECT $columns, sum(sign) AS n FROM view_$i GROUP BY $columns HAVING n != 0) ORDER BY row))
                = (SELECT groupArray(toString(row)) FROM (SELECT tuple($columns, n) AS row FROM
                    (SELECT $columns, toInt64(count()) AS n FROM ($(q$i "$NET_ORDERS" "$NET_QUOTES" "$NET_READINGS")) GROUP BY $columns) ORDER BY row))"
    done
}

$CLICKHOUSE_CLIENT -q "
    INSERT INTO products VALUES ('A', 'toys'), ('B', 'games');
    INSERT INTO orders VALUES (1, 'A', 10, 1), (2, 'B', 5, 1), (3, 'A', 7, 1);
    INSERT INTO quotes VALUES (1, 100, 1), (2, 200, 1), (3, 50, 1);
    INSERT INTO events VALUES (1, 10), (1, 20), (2, 5);
    INSERT INTO readings VALUES (1, 10, 1), (1, NULL, 1), (2, NULL, 1), (3, 5, 1);"
end_round 1

# Reprice order 1, delete order 2 and add order 4.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO quotes VALUES (1, 100, -1), (1, 120, 1);
    INSERT INTO orders VALUES (2, 'B', 5, -1), (4, 'B', 3, 1);
    INSERT INTO quotes VALUES (4, 150, 1);
    INSERT INTO events VALUES (2, 5), (2, 7), (3, 1);
    INSERT INTO readings VALUES (1, 10, -1);"
end_round 2

# Update order 3, add order 5 with a product of a new category, and delete order 4 with its quote, the last rows of
# category games.
$CLICKHOUSE_CLIENT -q "
    OPTIMIZE TABLE orders FINAL;
    INSERT INTO orders VALUES (3, 'A', 7, -1), (3, 'A', 12, 1), (5, 'C', 2, 1), (4, 'B', 3, -1);
    INSERT INTO quotes VALUES (4, 150, -1), (5, 80, 1);
    INSERT INTO products VALUES ('C', 'toys');
    INSERT INTO readings VALUES (2, 7, 1), (3, 5, -1);
    INSERT INTO events VALUES (1, 30), (4, 2), (2, 9);"
end_round 3

# Only quotes change, and nothing changes after that.
$CLICKHOUSE_CLIENT -q "INSERT INTO quotes VALUES (5, 80, -1), (5, 900, 1);"
end_round 4
end_round 5

# The views keep their cursors across DETACH and ATTACH.
for i in $VIEWS; do
    $CLICKHOUSE_CLIENT -q "DETACH TABLE view_$i; ATTACH TABLE view_$i;"
done
$CLICKHOUSE_CLIENT -q "INSERT INTO readings VALUES (2, 1, 1); INSERT INTO events VALUES (3, 1), (2, 9), (5, 40);"
end_round 6

echo "Net rows of the views"
for i in $VIEWS; do
    columns=${COLUMNS[$((i - 1))]}
    $CLICKHOUSE_CLIENT -q "SELECT $i, $columns, sum(sign) AS n FROM view_$i GROUP BY $columns HAVING n != 0 ORDER BY ALL"
done

echo "Result types"
$CLICKHOUSE_CLIENT -q "DESCRIBE TABLE view_1 SETTINGS describe_compact_output = 1"

# Each view keeps its integral in a hidden table, which DROP VIEW drops.
echo "Integrals"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_integral.%'"

# A view can't move to another database after it creates its integral.
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${CLICKHOUSE_DATABASE}_other ENGINE = Atomic"
$CLICKHOUSE_CLIENT -q "RENAME TABLE view_1 TO ${CLICKHOUSE_DATABASE}_other.view_1" 2>&1 | grep -o -m1 NOT_IMPLEMENTED
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${CLICKHOUSE_DATABASE}_other"

for i in $VIEWS; do
    $CLICKHOUSE_CLIENT -q "DROP TABLE view_$i"
done
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_%'"
