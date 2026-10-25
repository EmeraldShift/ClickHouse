-- Tags: atomic-database
-- Incremental views over ASOF joins with delta_query_asof_lookback match their queries. The rounds add orders whose keys
-- have a quote within the lookback, orders whose keys have only older quotes or none, and late quotes that change the
-- matches of earlier orders. The times of v_64 are in milliseconds. The views cover >=, >, <= and <, an ASOF LEFT JOIN, two keys, DateTime64 and
-- two ASOF joins in a row.

CREATE TABLE quotes (k UInt32, k2 UInt8, t Int64, px Int64) ENGINE = MergeTree ORDER BY (k, k2, t)
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0, index_granularity = 1;
CREATE TABLE orders (id UInt32, k UInt32, k2 UInt8, t Int64) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;
CREATE TABLE marks (k UInt32, t Int64) ENGINE = MergeTree ORDER BY (k, t)
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;
CREATE TABLE squotes (k UInt32, t Int64, px Int64, sign Int8) ENGINE = MergeTree ORDER BY (k, t)
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;
CREATE TABLE quotes64 (k UInt32, t DateTime64(3), px Int64) ENGINE = MergeTree ORDER BY (k, t)
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0, index_granularity = 1;
CREATE TABLE orders64 (id UInt32, k UInt32, t DateTime64(3)) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;

CREATE MATERIALIZED VIEW v_ge REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT o.id AS id, q.px AS px FROM orders AS o ASOF JOIN quotes AS q ON o.k = q.k AND o.t >= q.t
    SETTINGS delta_query_sign_column = 'sign', delta_query_asof_lookback = 2;
CREATE MATERIALIZED VIEW v_ge_full REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT o.id AS id, q.px AS px FROM orders AS o ASOF JOIN quotes AS q ON o.k = q.k AND o.t >= q.t
    SETTINGS delta_query_sign_column = 'sign';
CREATE MATERIALIZED VIEW v_gt REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT o.id AS id, q.px AS px FROM orders AS o ASOF JOIN quotes AS q ON o.k = q.k AND o.t > q.t
    SETTINGS delta_query_sign_column = 'sign', delta_query_asof_lookback = 2;
CREATE MATERIALIZED VIEW v_le REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT o.id AS id, q.px AS px FROM orders AS o ASOF LEFT JOIN quotes AS q ON o.k = q.k AND o.t <= q.t
    SETTINGS delta_query_sign_column = 'sign', delta_query_asof_lookback = 2;
CREATE MATERIALIZED VIEW v_lt REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT o.id AS id, q.px AS px FROM orders AS o ASOF JOIN quotes AS q ON q.k = o.k AND q.t > o.t
    SETTINGS delta_query_sign_column = 'sign', delta_query_asof_lookback = 2;
CREATE MATERIALIZED VIEW v_two REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT o.id AS id, q.px AS px FROM orders AS o ASOF JOIN quotes AS q ON o.k = q.k AND o.k2 = q.k2 AND o.t >= q.t
    SETTINGS delta_query_sign_column = 'sign', delta_query_asof_lookback = 2;
CREATE MATERIALIZED VIEW v_64 REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT o.id AS id, q.px AS px FROM orders64 AS o ASOF JOIN quotes64 AS q ON o.k = q.k AND o.t >= q.t
    SETTINGS delta_query_sign_column = 'sign', delta_query_asof_lookback = 0.002;
-- The marks are the right side of one ASOF join and the left side of the next.
CREATE MATERIALIZED VIEW v_chain REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT o.id AS id, q.px AS px FROM orders AS o ASOF JOIN marks AS m ON o.k = m.k AND o.t >= m.t
        ASOF JOIN quotes AS q ON m.k = q.k AND m.t >= q.t
    SETTINGS delta_query_sign_column = 'sign', delta_query_asof_lookback = 2;
-- An order without a mark pads it with key 0 and time 0.
CREATE MATERIALIZED VIEW v_pad REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT o.id AS id, q.px AS px FROM orders AS o LEFT JOIN marks AS m ON o.k = m.k
        ASOF LEFT JOIN quotes AS q ON m.k = q.k AND m.t >= q.t
    SETTINGS delta_query_sign_column = 'sign', delta_query_asof_lookback = 2;
-- The quotes are signed, so a quote within the lookback can be cancelled.
CREATE MATERIALIZED VIEW v_signed REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT o.id AS id, q.px AS px FROM orders AS o ASOF JOIN squotes AS q ON o.k = q.k AND o.t >= q.t
    SETTINGS delta_query_sign_column = 'sign', delta_query_asof_lookback = 2;

-- Prints whether each view has the rows of its query.
CREATE TEMPORARY TABLE checks (round UInt8);
CREATE VIEW check_views AS SELECT view, (SELECT max(round) FROM checks) AS round, got = want AS matches FROM
(
    SELECT 'v_ge' AS view, (SELECT groupArray((id, px, s)) FROM (SELECT id, px, sum(sign) AS s FROM v_ge GROUP BY ALL HAVING s != 0 ORDER BY ALL)) AS got,
        (SELECT groupArray((id, px, s)) FROM (SELECT id, px, toInt64(count()) AS s FROM (SELECT o.id AS id, q.px AS px FROM orders AS o ASOF JOIN quotes AS q ON o.k = q.k AND o.t >= q.t) GROUP BY ALL ORDER BY ALL)) AS want
    UNION ALL
    SELECT 'v_chain', (SELECT groupArray((id, px, s)) FROM (SELECT id, px, sum(sign) AS s FROM v_chain GROUP BY ALL HAVING s != 0 ORDER BY ALL)),
        (SELECT groupArray((id, px, s)) FROM (SELECT id, px, toInt64(count()) AS s FROM (SELECT o.id AS id, q.px AS px FROM orders AS o ASOF JOIN marks AS m ON o.k = m.k AND o.t >= m.t ASOF JOIN quotes AS q ON m.k = q.k AND m.t >= q.t) GROUP BY ALL ORDER BY ALL))
    UNION ALL
    SELECT 'v_pad', (SELECT groupArray((id, px, s)) FROM (SELECT id, px, sum(sign) AS s FROM v_pad GROUP BY ALL HAVING s != 0 ORDER BY ALL)),
        (SELECT groupArray((id, px, s)) FROM (SELECT id, px, toInt64(count()) AS s FROM (SELECT o.id AS id, q.px AS px FROM orders AS o LEFT JOIN marks AS m ON o.k = m.k ASOF LEFT JOIN quotes AS q ON m.k = q.k AND m.t >= q.t) GROUP BY ALL ORDER BY ALL))
    UNION ALL
    SELECT 'v_signed', (SELECT groupArray((id, px, s)) FROM (SELECT id, px, sum(sign) AS s FROM v_signed GROUP BY ALL HAVING s != 0 ORDER BY ALL)),
        (SELECT groupArray((id, px, s)) FROM (SELECT id, px, toInt64(count()) AS s FROM (SELECT o.id AS id, q.px AS px FROM orders AS o ASOF JOIN (SELECT k, t, px FROM squotes GROUP BY ALL HAVING sum(sign) > 0) AS q ON o.k = q.k AND o.t >= q.t) GROUP BY ALL ORDER BY ALL))
    UNION ALL
    SELECT 'v_ge_full', (SELECT groupArray((id, px, s)) FROM (SELECT id, px, sum(sign) AS s FROM v_ge_full GROUP BY ALL HAVING s != 0 ORDER BY ALL)),
        (SELECT groupArray((id, px, s)) FROM (SELECT id, px, toInt64(count()) AS s FROM (SELECT o.id AS id, q.px AS px FROM orders AS o ASOF JOIN quotes AS q ON o.k = q.k AND o.t >= q.t) GROUP BY ALL ORDER BY ALL))
    UNION ALL
    SELECT 'v_gt', (SELECT groupArray((id, px, s)) FROM (SELECT id, px, sum(sign) AS s FROM v_gt GROUP BY ALL HAVING s != 0 ORDER BY ALL)),
        (SELECT groupArray((id, px, s)) FROM (SELECT id, px, toInt64(count()) AS s FROM (SELECT o.id AS id, q.px AS px FROM orders AS o ASOF JOIN quotes AS q ON o.k = q.k AND o.t > q.t) GROUP BY ALL ORDER BY ALL))
    UNION ALL
    SELECT 'v_le', (SELECT groupArray((id, px, s)) FROM (SELECT id, px, sum(sign) AS s FROM v_le GROUP BY ALL HAVING s != 0 ORDER BY ALL)),
        (SELECT groupArray((id, px, s)) FROM (SELECT id, px, toInt64(count()) AS s FROM (SELECT o.id AS id, q.px AS px FROM orders AS o ASOF LEFT JOIN quotes AS q ON o.k = q.k AND o.t <= q.t) GROUP BY ALL ORDER BY ALL))
    UNION ALL
    SELECT 'v_lt', (SELECT groupArray((id, px, s)) FROM (SELECT id, px, sum(sign) AS s FROM v_lt GROUP BY ALL HAVING s != 0 ORDER BY ALL)),
        (SELECT groupArray((id, px, s)) FROM (SELECT id, px, toInt64(count()) AS s FROM (SELECT o.id AS id, q.px AS px FROM orders AS o ASOF JOIN quotes AS q ON q.k = o.k AND q.t > o.t) GROUP BY ALL ORDER BY ALL))
    UNION ALL
    SELECT 'v_two', (SELECT groupArray((id, px, s)) FROM (SELECT id, px, sum(sign) AS s FROM v_two GROUP BY ALL HAVING s != 0 ORDER BY ALL)),
        (SELECT groupArray((id, px, s)) FROM (SELECT id, px, toInt64(count()) AS s FROM (SELECT o.id AS id, q.px AS px FROM orders AS o ASOF JOIN quotes AS q ON o.k = q.k AND o.k2 = q.k2 AND o.t >= q.t) GROUP BY ALL ORDER BY ALL))
    UNION ALL
    SELECT 'v_64', (SELECT groupArray((id, px, s)) FROM (SELECT id, px, sum(sign) AS s FROM v_64 GROUP BY ALL HAVING s != 0 ORDER BY ALL)),
        (SELECT groupArray((id, px, s)) FROM (SELECT id, px, toInt64(count()) AS s FROM (SELECT o.id AS id, q.px AS px FROM orders64 AS o ASOF JOIN quotes64 AS q ON o.k = q.k AND o.t >= q.t) GROUP BY ALL ORDER BY ALL))
)
ORDER BY view;

-- Round 1: the first refresh.
INSERT INTO quotes VALUES (1, 0, 0, 100), (1, 0, 10, 110), (1, 0, 20, 120), (1, 1, 10, 115), (2, 0, 0, 200), (2, 0, 30, 230);
INSERT INTO orders VALUES (1, 1, 0, 5), (2, 1, 0, 15), (3, 1, 1, 25), (4, 2, 0, 5), (5, 3, 0, 5);
INSERT INTO quotes64 SELECT k, fromUnixTimestamp64Milli(t), px FROM quotes;
INSERT INTO orders64 SELECT id, k, fromUnixTimestamp64Milli(t) FROM orders;
INSERT INTO checks VALUES (1);
SYSTEM REFRESH VIEW v_ge; SYSTEM WAIT VIEW v_ge;
SYSTEM REFRESH VIEW v_ge_full; SYSTEM WAIT VIEW v_ge_full;
SYSTEM REFRESH VIEW v_gt; SYSTEM WAIT VIEW v_gt;
SYSTEM REFRESH VIEW v_le; SYSTEM WAIT VIEW v_le;
SYSTEM REFRESH VIEW v_lt; SYSTEM WAIT VIEW v_lt;
SYSTEM REFRESH VIEW v_two; SYSTEM WAIT VIEW v_two;
SYSTEM REFRESH VIEW v_64; SYSTEM WAIT VIEW v_64;
SYSTEM REFRESH VIEW v_chain; SYSTEM WAIT VIEW v_chain;
SYSTEM REFRESH VIEW v_pad; SYSTEM WAIT VIEW v_pad;
SYSTEM REFRESH VIEW v_signed; SYSTEM WAIT VIEW v_signed;
SELECT * FROM check_views;

-- Round 2: new orders. Key 1 has a quote within 2 of time 21 and 20, but not of 30 or 9. Key 2 has none of 21.
INSERT INTO orders VALUES (6, 1, 0, 21), (7, 1, 0, 30), (8, 2, 0, 21), (9, 1, 0, 20), (10, 1, 1, 9), (11, 1, 1, 12);
INSERT INTO orders64 SELECT id, k, fromUnixTimestamp64Milli(t) FROM orders WHERE id >= 6;
INSERT INTO checks VALUES (2);
SYSTEM REFRESH VIEW v_ge; SYSTEM WAIT VIEW v_ge;
SYSTEM REFRESH VIEW v_ge_full; SYSTEM WAIT VIEW v_ge_full;
SYSTEM REFRESH VIEW v_gt; SYSTEM WAIT VIEW v_gt;
SYSTEM REFRESH VIEW v_le; SYSTEM WAIT VIEW v_le;
SYSTEM REFRESH VIEW v_lt; SYSTEM WAIT VIEW v_lt;
SYSTEM REFRESH VIEW v_two; SYSTEM WAIT VIEW v_two;
SYSTEM REFRESH VIEW v_64; SYSTEM WAIT VIEW v_64;
SYSTEM REFRESH VIEW v_chain; SYSTEM WAIT VIEW v_chain;
SYSTEM REFRESH VIEW v_pad; SYSTEM WAIT VIEW v_pad;
SYSTEM REFRESH VIEW v_signed; SYSTEM WAIT VIEW v_signed;
SELECT * FROM check_views;

-- Round 3: late quotes. Key 1 at 12 changes the match of order 2. Key 3 gets its first quote. Key 2 at 25 changes the
-- matches of the forward joins of orders 4 and 8.
INSERT INTO quotes VALUES (1, 0, 12, 112), (3, 0, 1, 301), (2, 0, 25, 225), (1, 1, 11, 116);
INSERT INTO quotes64 SELECT k, fromUnixTimestamp64Milli(t), px FROM quotes WHERE px IN (112, 301, 225, 116);
INSERT INTO checks VALUES (3);
SYSTEM REFRESH VIEW v_ge; SYSTEM WAIT VIEW v_ge;
SYSTEM REFRESH VIEW v_ge_full; SYSTEM WAIT VIEW v_ge_full;
SYSTEM REFRESH VIEW v_gt; SYSTEM WAIT VIEW v_gt;
SYSTEM REFRESH VIEW v_le; SYSTEM WAIT VIEW v_le;
SYSTEM REFRESH VIEW v_lt; SYSTEM WAIT VIEW v_lt;
SYSTEM REFRESH VIEW v_two; SYSTEM WAIT VIEW v_two;
SYSTEM REFRESH VIEW v_64; SYSTEM WAIT VIEW v_64;
SYSTEM REFRESH VIEW v_chain; SYSTEM WAIT VIEW v_chain;
SYSTEM REFRESH VIEW v_pad; SYSTEM WAIT VIEW v_pad;
SYSTEM REFRESH VIEW v_signed; SYSTEM WAIT VIEW v_signed;
SELECT * FROM check_views;

-- Round 4: quotes and orders together, with an order before quotes it doesn't see yet and a quote at the time of an order.
INSERT INTO quotes VALUES (1, 0, 40, 140), (2, 0, 21, 221), (4, 0, 7, 407);
INSERT INTO orders VALUES (12, 1, 0, 40), (13, 1, 0, 39), (14, 4, 0, 8), (15, 2, 0, 22);
INSERT INTO quotes64 SELECT k, fromUnixTimestamp64Milli(t), px FROM quotes WHERE px IN (140, 221, 407);
INSERT INTO orders64 SELECT id, k, fromUnixTimestamp64Milli(t) FROM orders WHERE id >= 12;
INSERT INTO checks VALUES (4);
SYSTEM REFRESH VIEW v_ge; SYSTEM WAIT VIEW v_ge;
SYSTEM REFRESH VIEW v_ge_full; SYSTEM WAIT VIEW v_ge_full;
SYSTEM REFRESH VIEW v_gt; SYSTEM WAIT VIEW v_gt;
SYSTEM REFRESH VIEW v_le; SYSTEM WAIT VIEW v_le;
SYSTEM REFRESH VIEW v_lt; SYSTEM WAIT VIEW v_lt;
SYSTEM REFRESH VIEW v_two; SYSTEM WAIT VIEW v_two;
SYSTEM REFRESH VIEW v_64; SYSTEM WAIT VIEW v_64;
SYSTEM REFRESH VIEW v_chain; SYSTEM WAIT VIEW v_chain;
SYSTEM REFRESH VIEW v_pad; SYSTEM WAIT VIEW v_pad;
SYSTEM REFRESH VIEW v_signed; SYSTEM WAIT VIEW v_signed;
SELECT * FROM check_views;

-- Round 5: changes at the times of rows of the other side, each in its own refresh. Order 18 at 10 has a quote at 10,
-- which > doesn't match, and an older quote outside the lookback. The quote of key 6 at 10 changes the match of order 17.
-- The mark of key 7 at 100 changes the mark of order 20 from 10 to 100, and its quote from 5 to 99. The quote of key 0 at
-- 100 doesn't change the match of the padded mark of order 21. The signed quote of key 9 at 49 is cancelled in the refresh
-- of order 22 at 50, which matches the quote at 0.
INSERT INTO marks VALUES (1, 0), (2, 0), (7, 10);
INSERT INTO quotes VALUES (5, 0, 0, 500), (5, 0, 10, 510), (6, 0, 0, 600), (7, 0, 5, 705), (7, 0, 99, 799), (0, 0, -5, 5);
INSERT INTO orders VALUES (20, 7, 0, 150), (21, 8, 0, 1);
INSERT INTO squotes VALUES (9, 0, 900, 1);
INSERT INTO orders VALUES (17, 6, 0, 10);
INSERT INTO quotes64 SELECT k, fromUnixTimestamp64Milli(t), px FROM quotes WHERE k IN (5, 6);
INSERT INTO orders64 SELECT id, k, fromUnixTimestamp64Milli(t) FROM orders WHERE id = 17;
SYSTEM REFRESH VIEW v_ge; SYSTEM WAIT VIEW v_ge;
SYSTEM REFRESH VIEW v_ge_full; SYSTEM WAIT VIEW v_ge_full;
SYSTEM REFRESH VIEW v_gt; SYSTEM WAIT VIEW v_gt;
SYSTEM REFRESH VIEW v_le; SYSTEM WAIT VIEW v_le;
SYSTEM REFRESH VIEW v_lt; SYSTEM WAIT VIEW v_lt;
SYSTEM REFRESH VIEW v_two; SYSTEM WAIT VIEW v_two;
SYSTEM REFRESH VIEW v_64; SYSTEM WAIT VIEW v_64;
SYSTEM REFRESH VIEW v_chain; SYSTEM WAIT VIEW v_chain;
SYSTEM REFRESH VIEW v_pad; SYSTEM WAIT VIEW v_pad;
SYSTEM REFRESH VIEW v_signed; SYSTEM WAIT VIEW v_signed;
INSERT INTO orders VALUES (18, 5, 0, 10);
INSERT INTO squotes VALUES (9, 49, 949, 1);
INSERT INTO orders64 SELECT id, k, fromUnixTimestamp64Milli(t) FROM orders WHERE id = 18;
SYSTEM REFRESH VIEW v_ge; SYSTEM WAIT VIEW v_ge;
SYSTEM REFRESH VIEW v_ge_full; SYSTEM WAIT VIEW v_ge_full;
SYSTEM REFRESH VIEW v_gt; SYSTEM WAIT VIEW v_gt;
SYSTEM REFRESH VIEW v_le; SYSTEM WAIT VIEW v_le;
SYSTEM REFRESH VIEW v_lt; SYSTEM WAIT VIEW v_lt;
SYSTEM REFRESH VIEW v_two; SYSTEM WAIT VIEW v_two;
SYSTEM REFRESH VIEW v_64; SYSTEM WAIT VIEW v_64;
SYSTEM REFRESH VIEW v_chain; SYSTEM WAIT VIEW v_chain;
SYSTEM REFRESH VIEW v_pad; SYSTEM WAIT VIEW v_pad;
SYSTEM REFRESH VIEW v_signed; SYSTEM WAIT VIEW v_signed;
INSERT INTO quotes VALUES (6, 0, 10, 610);
INSERT INTO marks VALUES (7, 100);
INSERT INTO quotes VALUES (0, 0, 100, 1000);
INSERT INTO squotes VALUES (9, 49, 949, -1);
INSERT INTO orders VALUES (22, 9, 0, 50);
INSERT INTO quotes64 VALUES (6, fromUnixTimestamp64Milli(10), 610);
SYSTEM REFRESH VIEW v_ge; SYSTEM WAIT VIEW v_ge;
SYSTEM REFRESH VIEW v_ge_full; SYSTEM WAIT VIEW v_ge_full;
SYSTEM REFRESH VIEW v_gt; SYSTEM WAIT VIEW v_gt;
SYSTEM REFRESH VIEW v_le; SYSTEM WAIT VIEW v_le;
SYSTEM REFRESH VIEW v_lt; SYSTEM WAIT VIEW v_lt;
SYSTEM REFRESH VIEW v_two; SYSTEM WAIT VIEW v_two;
SYSTEM REFRESH VIEW v_64; SYSTEM WAIT VIEW v_64;
SYSTEM REFRESH VIEW v_chain; SYSTEM WAIT VIEW v_chain;
SYSTEM REFRESH VIEW v_pad; SYSTEM WAIT VIEW v_pad;
SYSTEM REFRESH VIEW v_signed; SYSTEM WAIT VIEW v_signed;
INSERT INTO checks VALUES (5);
SELECT * FROM check_views;

-- Round 6: a late quote of key 1 just after a quote within the lookback, behind an order and many older quotes. The
-- refresh with the lookback reads a few of the quotes of key 1, and the one without reads all of them.
INSERT INTO quotes SELECT 1, 0, 100 + 2 * number, 1000 + number FROM numbers(1000);
INSERT INTO orders VALUES (19, 1, 0, 2200);
INSERT INTO quotes64 SELECT k, fromUnixTimestamp64Milli(t), px FROM quotes WHERE k = 1 AND t >= 100;
INSERT INTO orders64 SELECT id, k, fromUnixTimestamp64Milli(t) FROM orders WHERE id = 19;
SYSTEM REFRESH VIEW v_ge; SYSTEM WAIT VIEW v_ge;
SYSTEM REFRESH VIEW v_ge_full; SYSTEM WAIT VIEW v_ge_full;
SYSTEM REFRESH VIEW v_64; SYSTEM WAIT VIEW v_64;
INSERT INTO quotes VALUES (1, 0, 2099, 3099);
INSERT INTO quotes64 VALUES (1, fromUnixTimestamp64Milli(2099), 3099);
SYSTEM REFRESH VIEW v_ge; SYSTEM WAIT VIEW v_ge;
SYSTEM REFRESH VIEW v_ge_full; SYSTEM WAIT VIEW v_ge_full;
SYSTEM REFRESH VIEW v_64; SYSTEM WAIT VIEW v_64;
INSERT INTO checks VALUES (6);
SELECT * FROM check_views WHERE view IN ('v_ge', 'v_ge_full', 'v_64');
SYSTEM FLUSH LOGS query_log;
SELECT view, (SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish'
        AND log_comment LIKE '%.' || view AND query_kind = 'Insert' ORDER BY event_time_microseconds DESC LIMIT 1) * 10
    < (SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish'
        AND log_comment LIKE '%v_ge_full' AND query_kind = 'Insert' ORDER BY event_time_microseconds DESC LIMIT 1) AS reads_fewer
FROM (SELECT arrayJoin(['v_ge', 'v_64']) AS view) ORDER BY view;
