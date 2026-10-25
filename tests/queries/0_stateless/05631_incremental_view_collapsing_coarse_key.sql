-- Tags: atomic-database
-- Incremental views into CollapsingMergeTree targets ordered by fewer columns than the result, so an update writes the
-- retraction and the new row with the same sorting key. The target keeps the new row in every round.

CREATE TABLE events (g UInt32, v UInt64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;
CREATE TABLE orders (id UInt32, k UInt32) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;
CREATE TABLE quotes (k UInt32, px UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;

-- The top groups by sum, with a residual.
CREATE MATERIALIZED VIEW v_top REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = CollapsingMergeTree(sign) ORDER BY g EMPTY AS
    SELECT g, sum(v) AS s, count() AS c FROM events GROUP BY g ORDER BY s DESC, g LIMIT 20
    SETTINGS delta_query_sign_column = 'sign';
-- A new quote replaces the padded row of each of its orders.
CREATE MATERIALIZED VIEW v_left REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = CollapsingMergeTree(sign) ORDER BY id EMPTY AS
    SELECT o.id AS id, q.px AS px FROM orders AS o LEFT JOIN quotes AS q ON o.k = q.k
    SETTINGS delta_query_sign_column = 'sign';

CREATE TEMPORARY TABLE rounds (r UInt8);
CREATE VIEW check_views AS SELECT (SELECT max(r) FROM rounds) AS round,
    (SELECT groupArray((g, s, c)) FROM (SELECT g, s, c FROM v_top FINAL ORDER BY s DESC, g))
        = (SELECT groupArray((g, s, c)) FROM (SELECT g, sum(v) AS s, count() AS c FROM events GROUP BY g ORDER BY s DESC, g LIMIT 20)) AS top,
    (SELECT groupArray((id, px)) FROM (SELECT id, px FROM v_left FINAL ORDER BY id))
        = (SELECT groupArray((id, px)) FROM (SELECT o.id AS id, q.px AS px FROM orders AS o LEFT JOIN quotes AS q ON o.k = q.k ORDER BY id)) AS left;

INSERT INTO orders SELECT number, number % 50 FROM numbers(200);

-- Round 1 adds 1000 events over 200 groups, and later rounds 100. Each round quotes 10 new keys.
INSERT INTO events SELECT cityHash64(number, 1) % 200, cityHash64(number, 2) % 1000 FROM numbers(0, 1000);
INSERT INTO quotes SELECT number, 100 + number FROM numbers(0, 10);
INSERT INTO rounds VALUES (1);
SYSTEM REFRESH VIEW v_top; SYSTEM WAIT VIEW v_top;
SYSTEM REFRESH VIEW v_left; SYSTEM WAIT VIEW v_left;
SELECT * FROM check_views;

INSERT INTO events SELECT cityHash64(number, 1) % 200, cityHash64(number, 2) % 1000 FROM numbers(1000, 100);
INSERT INTO quotes SELECT number, 100 + number FROM numbers(10, 10);
INSERT INTO rounds VALUES (2);
SYSTEM REFRESH VIEW v_top; SYSTEM WAIT VIEW v_top;
SYSTEM REFRESH VIEW v_left; SYSTEM WAIT VIEW v_left;
SELECT * FROM check_views;

INSERT INTO events SELECT cityHash64(number, 1) % 200, cityHash64(number, 2) % 1000 FROM numbers(1100, 100);
INSERT INTO quotes SELECT number, 100 + number FROM numbers(20, 10);
INSERT INTO rounds VALUES (3);
SYSTEM REFRESH VIEW v_top; SYSTEM WAIT VIEW v_top;
SYSTEM REFRESH VIEW v_left; SYSTEM WAIT VIEW v_left;
SELECT * FROM check_views;

-- After merges too.
OPTIMIZE TABLE v_top FINAL;
OPTIMIZE TABLE v_left FINAL;
INSERT INTO events SELECT cityHash64(number, 1) % 200, cityHash64(number, 2) % 1000 FROM numbers(1200, 100);
INSERT INTO quotes SELECT number, 100 + number FROM numbers(30, 10);
INSERT INTO rounds VALUES (4);
SYSTEM REFRESH VIEW v_top; SYSTEM WAIT VIEW v_top;
SYSTEM REFRESH VIEW v_left; SYSTEM WAIT VIEW v_left;
SELECT * FROM check_views;
