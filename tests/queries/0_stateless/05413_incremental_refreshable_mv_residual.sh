#!/usr/bin/env bash
# Tags: atomic-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Maintain queries with residuals in incremental refreshable materialized views: HAVING, expressions over aggregates, avg,
# a LIMIT over all groups, LIMIT BY, a window with QUALIFY, DISTINCT, and aggregates like uniqExact over an unsigned table.
# After each round of inserts, deletes and updates, compare the net rows of each view with its query over the rows that no
# row of sign -1 cancels.

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE orders (id UInt64, product String, qty Int64, sign Int8)
        ENGINE = MergeTree ORDER BY id SETTINGS enable_block_number_column = 1;
    CREATE TABLE quotes (order_id UInt64, price Int64, sign Int8)
        ENGINE = MergeTree ORDER BY order_id SETTINGS enable_block_number_column = 1;
    CREATE TABLE products (product String, category String)
        ENGINE = MergeTree ORDER BY product SETTINGS enable_block_number_column = 1;
    CREATE TABLE events (k UInt8, v UInt32) ENGINE = MergeTree ORDER BY k SETTINGS enable_block_number_column = 1;
"

# Each query reads the tables \$1 and \$2 in place of orders and quotes.
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

COLUMNS=("category, qty, avg_price" "id, value" "category, id, qty" "category, id, qty, r" "product" "k, u, m, spread")
NET_ORDERS="(SELECT id, product, qty FROM orders GROUP BY ALL HAVING sum(sign) > 0)"
NET_QUOTES="(SELECT order_id, price FROM quotes GROUP BY ALL HAVING sum(sign) > 0)"
S="SETTINGS delta_query_sign_column = 'sign'"

for i in 1 2 3 4 5 6; do
    $CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW view_$i REFRESH EVERY 10 YEAR APPEND INCREMENTAL
        ENGINE = CollapsingMergeTree(sign) ORDER BY (${COLUMNS[$((i - 1))]}) EMPTY AS $(q$i orders quotes) $S"
done

# Refresh the views and compare the net rows of each with its query over the net inputs.
end_round()
{
    for i in 1 2 3 4 5 6; do
        $CLICKHOUSE_CLIENT -q "SYSTEM REFRESH VIEW view_$i; SYSTEM WAIT VIEW view_$i;"
        local columns=${COLUMNS[$((i - 1))]}
        $CLICKHOUSE_CLIENT -q "
            SELECT 'round $1', 'view $i',
                (SELECT groupArray(row) FROM (SELECT tuple($columns, n) AS row FROM
                    (SELECT $columns, sum(sign) AS n FROM view_$i GROUP BY $columns HAVING n != 0) ORDER BY row))
                = (SELECT groupArray(row) FROM (SELECT tuple($columns, n) AS row FROM
                    (SELECT $columns, toInt64(count()) AS n FROM ($(q$i "$NET_ORDERS" "$NET_QUOTES")) GROUP BY $columns) ORDER BY row))"
    done
}

$CLICKHOUSE_CLIENT -q "
    INSERT INTO products VALUES ('A', 'toys'), ('B', 'games');
    INSERT INTO orders VALUES (1, 'A', 10, 1), (2, 'B', 5, 1), (3, 'A', 7, 1);
    INSERT INTO quotes VALUES (1, 100, 1), (2, 200, 1), (3, 50, 1);
    INSERT INTO events VALUES (1, 10), (1, 20), (2, 5);"
end_round 1

# Reprice order 1, delete order 2 and add order 4.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO quotes VALUES (1, 100, -1), (1, 120, 1);
    INSERT INTO orders VALUES (2, 'B', 5, -1), (4, 'B', 3, 1);
    INSERT INTO quotes VALUES (4, 150, 1);
    INSERT INTO events VALUES (2, 5), (2, 7), (3, 1);"
end_round 2

# Update order 3, add order 5 with a product of a new category, and delete order 4 with its quote, the last rows of
# category games.
$CLICKHOUSE_CLIENT -q "
    OPTIMIZE TABLE orders FINAL;
    INSERT INTO orders VALUES (3, 'A', 7, -1), (3, 'A', 12, 1), (5, 'C', 2, 1), (4, 'B', 3, -1);
    INSERT INTO quotes VALUES (4, 150, -1), (5, 80, 1);
    INSERT INTO products VALUES ('C', 'toys');"
end_round 3

# Only quotes change, and nothing changes after that.
$CLICKHOUSE_CLIENT -q "INSERT INTO quotes VALUES (5, 80, -1), (5, 900, 1);"
end_round 4
end_round 5

echo "Net rows of the views"
for i in 1 2 3 4 5 6; do
    columns=${COLUMNS[$((i - 1))]}
    $CLICKHOUSE_CLIENT -q "SELECT $i, $columns, sum(sign) AS n FROM view_$i GROUP BY $columns HAVING n != 0 ORDER BY ALL"
done

echo "Result types"
$CLICKHOUSE_CLIENT -q "DESCRIBE TABLE view_1 SETTINGS describe_compact_output = 1"

# Each view keeps its integral in a hidden table, which DROP VIEW drops.
echo "Integrals"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_integral.%'"
for i in 1 2 3 4 5 6; do
    $CLICKHOUSE_CLIENT -q "DROP TABLE view_$i"
done
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_%'"
