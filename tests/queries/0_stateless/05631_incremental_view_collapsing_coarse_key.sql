-- Tags: atomic-database
-- Incremental views into CollapsingMergeTree targets ordered by fewer columns than the result, so an update writes the
-- retraction and the new row with the same sorting key. The target keeps the new row in every round.

CREATE TABLE orders (id UInt32, k UInt32) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;
CREATE TABLE quotes (k UInt32, px UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;

-- A new quote replaces the padded row of each of its orders.
CREATE MATERIALIZED VIEW v_left REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = CollapsingMergeTree(sign) ORDER BY id EMPTY AS
    SELECT o.id AS id, q.px AS px FROM orders AS o LEFT JOIN quotes AS q ON o.k = q.k
    SETTINGS delta_query_sign_column = 'sign';

CREATE TEMPORARY TABLE rounds (r UInt8);
CREATE VIEW check_views AS SELECT (SELECT max(r) FROM rounds) AS round,
    (SELECT groupArray((id, px)) FROM (SELECT id, px FROM v_left FINAL ORDER BY id))
        = (SELECT groupArray((id, px)) FROM (SELECT o.id AS id, q.px AS px FROM orders AS o LEFT JOIN quotes AS q ON o.k = q.k ORDER BY id)) AS left;

INSERT INTO orders SELECT number, number % 50 FROM numbers(200);

-- Each round quotes 10 new keys.
INSERT INTO quotes SELECT number, 100 + number FROM numbers(0, 10);
INSERT INTO rounds VALUES (1);
SYSTEM REFRESH VIEW v_left; SYSTEM WAIT VIEW v_left;
SELECT * FROM check_views;

INSERT INTO quotes SELECT number, 100 + number FROM numbers(10, 10);
INSERT INTO rounds VALUES (2);
SYSTEM REFRESH VIEW v_left; SYSTEM WAIT VIEW v_left;
SELECT * FROM check_views;

INSERT INTO quotes SELECT number, 100 + number FROM numbers(20, 10);
INSERT INTO rounds VALUES (3);
SYSTEM REFRESH VIEW v_left; SYSTEM WAIT VIEW v_left;
SELECT * FROM check_views;

-- After merges too.
OPTIMIZE TABLE v_left FINAL;
INSERT INTO quotes SELECT number, 100 + number FROM numbers(30, 10);
INSERT INTO rounds VALUES (4);
SYSTEM REFRESH VIEW v_left; SYSTEM WAIT VIEW v_left;
SELECT * FROM check_views;
