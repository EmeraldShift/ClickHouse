-- Tags: atomic-database
-- An incremental view can read the target of another incremental view. The target is a MergeTree with block numbers that
-- keeps every delta row with its sign, so the second view reads it as a signed input.

CREATE TABLE orders (id UInt64, cust UInt64, amount Int64) ENGINE = MergeTree ORDER BY id SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;
CREATE TABLE customers (id UInt64, region String) ENGINE = MergeTree ORDER BY id SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;
CREATE TABLE enriched (id UInt64, region String, amount Int64, sign Int8) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;
CREATE TABLE totals (region String, amount Int64) ENGINE = SummingMergeTree ORDER BY region;

CREATE MATERIALIZED VIEW enriched_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO enriched EMPTY AS
    SELECT o.id AS id, c.region AS region, o.amount AS amount FROM orders AS o LEFT JOIN customers AS c ON c.id = o.cust
    SETTINGS delta_query_sign_column = 'sign';
CREATE MATERIALIZED VIEW totals_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO totals EMPTY AS
    SELECT region, sum(amount) AS amount FROM enriched GROUP BY region SETTINGS delta_query_sign_column = 'sign';

-- Round 1: orders without customers. Round 2: the customer of order 1 arrives, which moves its amount to a region.
INSERT INTO orders VALUES (1, 1, 10), (2, 2, 5);
SYSTEM REFRESH VIEW enriched_mv; SYSTEM WAIT VIEW enriched_mv;
SYSTEM REFRESH VIEW totals_mv; SYSTEM WAIT VIEW totals_mv;
SELECT 'round 1', region, sum(amount) AS s FROM totals GROUP BY region HAVING s != 0 ORDER BY region;

INSERT INTO customers VALUES (1, 'east');
SYSTEM REFRESH VIEW enriched_mv; SYSTEM WAIT VIEW enriched_mv;
SYSTEM REFRESH VIEW totals_mv; SYSTEM WAIT VIEW totals_mv;
SELECT 'round 2', region, sum(amount) AS s FROM totals GROUP BY region HAVING s != 0 ORDER BY region;
