#!/usr/bin/env bash
# Tags: atomic-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Maintain queries over signed inputs with `deltaQuery` and incremental refreshable materialized views, over five rounds
# of inserts, deletes and updates. Then compare the maintained results with the queries over the rows that no row of
# sign -1 cancels.

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE orders (id UInt64, product String, qty Int64, note Nullable(String), sign Int8)
        ENGINE = MergeTree ORDER BY id SETTINGS enable_block_number_column = 1;
    CREATE TABLE quotes (order_id UInt64, price Int64, t UInt32, sign Int8)
        ENGINE = MergeTree ORDER BY order_id SETTINGS enable_block_number_column = 1;
    CREATE TABLE products (product String, category String)
        ENGINE = MergeTree ORDER BY product SETTINGS enable_block_number_column = 1;

    CREATE TABLE cursors (query UInt8, round UInt8, cursor Map(String, Int64)) ENGINE = MergeTree ORDER BY (query, round);
    CREATE TABLE revenue (category String, revenue Int64, n Int64, noted Int64, expensive Int64, expensive_qty Int64)
        ENGINE = SummingMergeTree ORDER BY category;
    CREATE TABLE view_revenue AS revenue;
    CREATE TABLE matches (id UInt64, price Int64, sign Int8) ENGINE = CollapsingMergeTree(sign) ORDER BY (id, price);
    CREATE TABLE match_deltas (round UInt8, id UInt64, price Int64, sign Int8) ENGINE = MergeTree ORDER BY round;
    CREATE TABLE totals (qty Int64, n Int64) ENGINE = SummingMergeTree ORDER BY tuple();
"

# Each query reads the tables \$1 and \$2 in place of orders and quotes.
q1() { echo "SELECT p.category AS category, sum(o.qty * q.price) AS revenue, count() AS n, count(o.note) AS noted,
    countIf(q.price > 100) AS expensive, sumIf(o.qty, q.price > 100) AS expensive_qty
    FROM $1 AS o INNER JOIN $2 AS q ON q.order_id = o.id INNER JOIN products AS p ON p.product = o.product GROUP BY category"; }
q2() { echo "SELECT o.id AS id, q.price AS price FROM $1 AS o INNER JOIN $2 AS q ON q.order_id = o.id"; }
q3() { echo "SELECT sum(qty) AS qty, count() AS n FROM $1"; }

S="SETTINGS delta_query_sign_column = 'sign'"
Q1="$(q1 orders quotes) $S"
# Q2 gets the setting from the query that reads it.
Q2="$(q2 orders quotes)"
Q3="$(q3 orders) $S"

# The rows of the inputs that no row of sign -1 cancels.
NET_ORDERS="(SELECT id, product, qty, note FROM orders GROUP BY ALL HAVING sum(sign) > 0)"
NET_QUOTES="(SELECT order_id, price, t FROM quotes GROUP BY ALL HAVING sum(sign) > 0)"

$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW revenue_view REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO view_revenue EMPTY AS $Q1"
# The inner table of this view gets the sign column from the deltas.
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW match_view REFRESH EVERY 10 YEAR APPEND INCREMENTAL
    ENGINE = CollapsingMergeTree(sign) ORDER BY (id, price) EMPTY AS $Q2 $S"

cursor() { echo "(SELECT cursor FROM cursors WHERE query = $1 AND round = $2)"; }

# Store each query's frontier as the cursor of round $1, then apply the delta since the previous round.
end_round()
{
    local r=$1
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO cursors SELECT 1, $r, cursor FROM deltaQueryFrontier(($Q1));
        INSERT INTO cursors SELECT 2, $r, cursor FROM deltaQueryFrontier(($Q2)) $S;
        INSERT INTO cursors SELECT 3, $r, cursor FROM deltaQueryFrontier(($Q3));"
    [ "$r" = 0 ] && return
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO revenue SELECT * FROM deltaQuery(($Q1), $(cursor 1 $((r - 1))), $(cursor 1 "$r"));
        INSERT INTO matches SELECT * FROM deltaQuery(($Q2), $(cursor 2 $((r - 1))), $(cursor 2 "$r")) $S;
        INSERT INTO match_deltas SELECT $r, * FROM deltaQuery(($Q2), $(cursor 2 $((r - 1))), $(cursor 2 "$r")) $S;
        INSERT INTO totals SELECT * FROM deltaQuery(($Q3), $(cursor 3 $((r - 1))), $(cursor 3 "$r"));
        SYSTEM REFRESH VIEW revenue_view;
        SYSTEM WAIT VIEW revenue_view;
        SYSTEM REFRESH VIEW match_view;
        SYSTEM WAIT VIEW match_view;"
}

end_round 0

$CLICKHOUSE_CLIENT -q "
    INSERT INTO products VALUES ('A', 'toys'), ('B', 'games');
    INSERT INTO orders VALUES (1, 'A', 10, NULL, 1), (2, 'B', 5, 'x', 1), (3, 'A', 7, 'z', 1);
    INSERT INTO quotes VALUES (1, 100, 1, 1), (2, 200, 1, 1), (3, 50, 1, 1);"
end_round 1

# Reprice order 1, delete order 2 and add order 4.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO quotes VALUES (1, 100, 1, -1), (1, 120, 2, 1);
    INSERT INTO orders VALUES (2, 'B', 5, 'x', -1), (4, 'B', 3, 'y', 1);
    INSERT INTO quotes VALUES (4, 150, 1, 1);"
end_round 2

# Merges keep the block numbers of rows. Update order 3, add order 5 with a product of a new category, and delete order 4
# with its quote, the last rows of category games.
$CLICKHOUSE_CLIENT -q "
    OPTIMIZE TABLE orders FINAL;
    OPTIMIZE TABLE quotes FINAL;
    INSERT INTO orders VALUES (3, 'A', 7, 'z', -1), (3, 'A', 9, 'z', 1), (5, 'C', 2, NULL, 1), (4, 'B', 3, 'y', -1);
    INSERT INTO quotes VALUES (4, 150, 1, -1), (5, 80, 1, 1);
    INSERT INTO products VALUES ('C', 'toys');"
end_round 3

# Only quotes change. The deltas of Q3 read no rows.
$CLICKHOUSE_CLIENT -q "INSERT INTO quotes VALUES (5, 80, 1, -1), (5, 90, 2, 1);"
end_round 4

end_round 5

echo "Result types"
$CLICKHOUSE_CLIENT -q "DESCRIBE TABLE deltaQuery(($Q1), map(), map()) SETTINGS describe_compact_output = 1"
$CLICKHOUSE_CLIENT -q "DESCRIBE TABLE deltaQuery(($Q2), map(), map()) $S, describe_compact_output = 1"
$CLICKHOUSE_CLIENT -q "DESCRIBE TABLE match_view SETTINGS describe_compact_output = 1"

echo "Rows of each round's match delta"
$CLICKHOUSE_CLIENT -q "SELECT * FROM match_deltas ORDER BY ALL"

echo "Maintained, view and recomputed revenue"
$CLICKHOUSE_CLIENT -q "SELECT category, sum(revenue), sum(n), sum(noted), sum(expensive), sum(expensive_qty) FROM revenue GROUP BY category HAVING sum(n) != 0 ORDER BY ALL"
$CLICKHOUSE_CLIENT -q "SELECT category, sum(revenue), sum(n), sum(noted), sum(expensive), sum(expensive_qty) FROM view_revenue GROUP BY category HAVING sum(n) != 0 ORDER BY ALL"
$CLICKHOUSE_CLIENT -q "$(q1 "$NET_ORDERS" "$NET_QUOTES") ORDER BY ALL"

echo "Maintained, view and recomputed matches"
$CLICKHOUSE_CLIENT -q "SELECT id, price, sum(sign) FROM matches GROUP BY id, price HAVING sum(sign) != 0 ORDER BY ALL"
$CLICKHOUSE_CLIENT -q "SELECT id, price, sum(sign) FROM match_view GROUP BY id, price HAVING sum(sign) != 0 ORDER BY ALL"
$CLICKHOUSE_CLIENT -q "SELECT *, 1 FROM ($(q2 "$NET_ORDERS" "$NET_QUOTES")) ORDER BY ALL"

echo "Maintained and recomputed totals"
$CLICKHOUSE_CLIENT -q "SELECT sum(qty), sum(n) FROM totals"
$CLICKHOUSE_CLIENT -q "$(q3 "$NET_ORDERS")"

# A weighted sum of UInt64 values is an Int128, which holds sums above the maximum Int64.
echo "Sum of UInt64"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE big (u UInt64, sign Int8) ENGINE = MergeTree ORDER BY u SETTINGS enable_block_number_column = 1;
    INSERT INTO big VALUES (9223372036854775808, 1), (1, 1), (5, 1), (5, -1);
    SELECT s, toTypeName(s) FROM deltaQuery((SELECT sum(u) AS s FROM big $S), map(),
        (SELECT cursor FROM deltaQueryFrontier((SELECT sum(u) FROM big $S))));"

# Group 1 retracts its latest sample, so its moving average stays as of time 11, like the average of its other sample and
# a sample of 0 at time 11. Group 2 corrects its latest sample at the same time, so its moving average matches the
# recomputed one.
echo "Maintained and recomputed moving averages"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE samples (g UInt8, v Float64, t UInt32, sign Int8) ENGINE = MergeTree ORDER BY g SETTINGS enable_block_number_column = 1;
    CREATE TABLE emas (g UInt8, ema AggregateFunction(exponentialMovingAverage(10), Float64, UInt32)) ENGINE = AggregatingMergeTree ORDER BY g;
    INSERT INTO samples VALUES (1, 10, 1, 1), (1, 20, 11, 1), (2, 5, 1, 1), (2, 7, 2, 1);
    INSERT INTO cursors SELECT 4, 1, cursor FROM deltaQueryFrontier((SELECT g, exponentialMovingAverageState(10)(v, t) FROM samples GROUP BY g $S));
    INSERT INTO samples VALUES (1, 20, 11, -1), (2, 7, 2, -1), (2, 8, 2, 1);
    INSERT INTO cursors SELECT 4, 2, cursor FROM deltaQueryFrontier((SELECT g, exponentialMovingAverageState(10)(v, t) FROM samples GROUP BY g $S));"
for r in 1 2; do
    $CLICKHOUSE_CLIENT -q "INSERT INTO emas SELECT * FROM deltaQuery((SELECT g, exponentialMovingAverageState(10)(v, t) AS ema FROM samples GROUP BY g $S),
        $([ $r = 1 ] && echo 'map()' || cursor 4 1), $(cursor 4 $r))"
done
$CLICKHOUSE_CLIENT -q "SELECT g, round(exponentialMovingAverageMerge(10)(ema), 6) FROM emas GROUP BY g ORDER BY g"
$CLICKHOUSE_CLIENT -q "SELECT g, round(exponentialMovingAverage(10)(v, t), 6) FROM (SELECT g, v, t FROM samples GROUP BY ALL HAVING sum(sign) > 0) GROUP BY g ORDER BY g"
$CLICKHOUSE_CLIENT -q "SELECT round(exponentialMovingAverage(10)(v, t), 6) FROM values('v Float64, t UInt32', (10, 1), (0, 11))"
