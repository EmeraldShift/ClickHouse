#!/usr/bin/env bash
# Tags: atomic-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Maintain two join queries with `deltaQuery`, and the first one with an incremental refreshable materialized view, over
# five rounds of inserts. Then compare the maintained results with the queries evaluated from scratch.

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE orders (id UInt64, day Date, product String, qty Int64)
        ENGINE = MergeTree PARTITION BY day ORDER BY id SETTINGS enable_block_number_column = 1;
    CREATE TABLE quotes (order_id UInt64, version UInt8, price Int64)
        ENGINE = MergeTree ORDER BY order_id SETTINGS enable_block_number_column = 1;
    CREATE TABLE products (product String, category String)
        ENGINE = MergeTree ORDER BY product SETTINGS enable_block_number_column = 1;

    CREATE TABLE cursors (query UInt8, round UInt8, cursor Map(String, Int64)) ENGINE = MergeTree ORDER BY (query, round);
    CREATE TABLE revenue (category String, product String, revenue SimpleAggregateFunction(sum, Int64), n SimpleAggregateFunction(sum, UInt64),
        orders AggregateFunction(uniqExact, UInt64)) ENGINE = AggregatingMergeTree ORDER BY (category, product);
    CREATE TABLE revenue_deltas (round UInt8, category String, product String, revenue Int64, n UInt64, orders UInt64) ENGINE = MergeTree ORDER BY round;
    CREATE TABLE pairs (product String, pairs SimpleAggregateFunction(sum, UInt64), qq SimpleAggregateFunction(sum, Int64))
        ENGINE = AggregatingMergeTree ORDER BY product;
    CREATE TABLE view_revenue AS revenue;
"

Q1="SELECT p.category AS category, o.product AS product, sum(o.qty * q.price) AS revenue, count() AS n, uniqExactState(o.id) AS orders
    FROM orders AS o INNER JOIN quotes AS q ON q.order_id = o.id INNER JOIN products AS p ON p.product = o.product
    WHERE q.version < 5 GROUP BY category, product"
Q2="SELECT a.product AS product, count() AS pairs, sum(a.qty * b.qty) AS qq FROM orders AS a INNER JOIN orders AS b ON a.product = b.product GROUP BY product"

$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW revenue_view REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO view_revenue EMPTY AS $Q1"

cursor() { echo "(SELECT cursor FROM cursors WHERE query = $1 AND round = $2)"; }

# Store each query's frontier as the cursor of round $1, then apply the delta since the previous round.
end_round()
{
    local r=$1
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO cursors SELECT 1, $r, cursor FROM deltaQueryFrontier(($Q1));
        INSERT INTO cursors SELECT 2, $r, cursor FROM deltaQueryFrontier(($Q2));"
    [ "$r" = 0 ] && return
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO revenue SELECT * FROM deltaQuery(($Q1), $(cursor 1 $((r - 1))), $(cursor 1 "$r"));
        INSERT INTO revenue_deltas SELECT $r, category, product, revenue, n, finalizeAggregation(orders)
            FROM deltaQuery(($Q1), $(cursor 1 $((r - 1))), $(cursor 1 "$r"));
        INSERT INTO pairs SELECT * FROM deltaQuery(($Q2), $(cursor 2 $((r - 1))), $(cursor 2 "$r"));
        SYSTEM REFRESH VIEW revenue_view;
        SYSTEM WAIT VIEW revenue_view;"
}

end_round 0

$CLICKHOUSE_CLIENT -q "
    INSERT INTO products VALUES ('A', 'toys'), ('B', 'games');
    INSERT INTO orders VALUES (1, '2026-01-01', 'A', 10), (2, '2026-01-01', 'B', -5);
    INSERT INTO orders VALUES (3, '2026-01-02', 'A', 7);
    INSERT INTO quotes VALUES (1, 0, 100), (2, 0, 200), (3, 0, 50);"
end_round 1

# Late quotes for round 1 orders, and new orders with their quotes in the same round.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO products VALUES ('C', 'toys');
    INSERT INTO quotes VALUES (1, 1, 101), (2, 1, 199);
    INSERT INTO orders VALUES (4, '2026-01-02', 'C', 3), (5, '2026-01-03', 'B', 2);
    INSERT INTO quotes VALUES (4, 0, 30), (5, 0, 70), (1, 5, 999);"
end_round 2

# Merge everything inserted so far. Rows keep their block numbers, so later deltas don't see them again.
# Order 6 goes into an old partition, and order 7 has a product that arrives in round 4.
$CLICKHOUSE_CLIENT -q "
    OPTIMIZE TABLE orders FINAL;
    OPTIMIZE TABLE quotes FINAL;
    INSERT INTO orders VALUES (6, '2026-01-01', 'C', -4), (7, '2026-01-03', 'D', 6);
    INSERT INTO quotes VALUES (3, 1, 52), (4, 1, 31), (6, 0, 40), (7, 0, 60);"
end_round 3

$CLICKHOUSE_CLIENT -q "
    INSERT INTO products VALUES ('D', 'food');
    INSERT INTO quotes VALUES (5, 1, 72), (6, 1, 41), (7, 1, 61);
    OPTIMIZE TABLE products FINAL;"
end_round 4

end_round 5

echo "Rows of each round's revenue delta"
$CLICKHOUSE_CLIENT -q "SELECT round, category, product, revenue, n, orders FROM revenue_deltas ORDER BY ALL"

echo "Maintained revenue"
$CLICKHOUSE_CLIENT -q "SELECT category, product, sum(revenue), sum(n), uniqExactMerge(orders) FROM revenue GROUP BY category, product ORDER BY ALL"

echo "Maintained and recomputed revenue differences"
$CLICKHOUSE_CLIENT -q "
    WITH
        maintained AS (SELECT category, product, sum(revenue), sum(n), uniqExactMerge(orders) FROM revenue GROUP BY category, product),
        recomputed AS (SELECT category, product, revenue, n, finalizeAggregation(orders) FROM ($Q1))
    SELECT 'maintained only', * FROM (SELECT * FROM maintained EXCEPT ALL SELECT * FROM recomputed)
    UNION ALL
    SELECT 'recomputed only', * FROM (SELECT * FROM recomputed EXCEPT ALL SELECT * FROM maintained)"

echo "View and recomputed revenue differences"
$CLICKHOUSE_CLIENT -q "
    WITH
        maintained AS (SELECT category, product, sum(revenue), sum(n), uniqExactMerge(orders) FROM view_revenue GROUP BY category, product),
        recomputed AS (SELECT category, product, revenue, n, finalizeAggregation(orders) FROM ($Q1))
    SELECT 'view only', * FROM (SELECT * FROM maintained EXCEPT ALL SELECT * FROM recomputed)
    UNION ALL
    SELECT 'recomputed only', * FROM (SELECT * FROM recomputed EXCEPT ALL SELECT * FROM maintained)"

echo "Maintained and recomputed self-join differences"
$CLICKHOUSE_CLIENT -q "
    WITH
        maintained AS (SELECT product, sum(pairs), sum(qq) FROM pairs GROUP BY product),
        recomputed AS ($Q2)
    SELECT 'maintained only', * FROM (SELECT * FROM maintained EXCEPT ALL SELECT * FROM recomputed)
    UNION ALL
    SELECT 'recomputed only', * FROM (SELECT * FROM recomputed EXCEPT ALL SELECT * FROM maintained)"

# Rerunning an old delta after later inserts and merges returns the same rows.
echo "Round 2 delta rerun differences"
$CLICKHOUSE_CLIENT -q "
    WITH
        rerun AS (SELECT category, product, revenue, n, finalizeAggregation(orders) FROM deltaQuery(($Q1), $(cursor 1 1), $(cursor 1 2))),
        logged AS (SELECT category, product, revenue, n, orders FROM revenue_deltas WHERE round = 2)
    SELECT 'rerun only', * FROM (SELECT * FROM rerun EXCEPT ALL SELECT * FROM logged)
    UNION ALL
    SELECT 'logged only', * FROM (SELECT * FROM logged EXCEPT ALL SELECT * FROM rerun)"
