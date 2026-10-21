-- The index analysis of `k IN set` over the first key column k starts from the marks that can hold an element of the set.
-- Each key spans marks, and the last mark holds the keys from its row on.

DROP TABLE IF EXISTS t;
CREATE TABLE t (k Int64, x UInt64) ENGINE = MergeTree ORDER BY (k, x) SETTINGS index_granularity = 4, index_granularity_bytes = 0;
INSERT INTO t SELECT intDiv(number, 3), number FROM numbers(100);

SELECT 'k IN values', count(), sum(x) FROM t WHERE k IN (-1, 0, 5, 6, 33, 100);
SELECT 'k IN values', count(), sum(x) FROM t WHERE k IN (-1, 0, 5, 6, 33, 100) SETTINGS use_primary_key = 0;
SELECT 'k IN subquery', count(), sum(x) FROM t WHERE k IN (SELECT number * 7 FROM numbers(10));
SELECT 'k IN subquery', count(), sum(x) FROM t WHERE k IN (SELECT number * 7 FROM numbers(10)) SETTINGS use_primary_key = 0;
SELECT '(k, x) IN', count(), sum(x) FROM t WHERE (k, x) IN ((5, 15), (6, 19), (6, 20));
SELECT '(k, x) IN', count(), sum(x) FROM t WHERE (k, x) IN ((5, 15), (6, 19), (6, 20)) SETTINGS use_primary_key = 0;
SELECT 'AND', count(), sum(x) FROM t WHERE k IN (2, 20, 30) AND x % 2 = 0;
SELECT 'AND', count(), sum(x) FROM t WHERE k IN (2, 20, 30) AND x % 2 = 0 SETTINGS use_primary_key = 0;
SELECT 'OR', count(), sum(x) FROM t WHERE k IN (2, 20) OR x = 99;
SELECT 'OR', count(), sum(x) FROM t WHERE k IN (2, 20) OR x = 99 SETTINGS use_primary_key = 0;
SELECT 'NOT', count(), sum(x) FROM t WHERE NOT k IN (2, 20);
SELECT 'NOT', count(), sum(x) FROM t WHERE NOT k IN (2, 20) SETTINGS use_primary_key = 0;

-- The granules that the analysis selects out of 25.
SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT * FROM t WHERE k IN (-1, 0, 5, 6, 33, 100)) WHERE explain LIKE '%Granules%';
SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT * FROM t WHERE k IN (2, 20, 30) AND x % 2 = 0) WHERE explain LIKE '%Granules%';
SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT * FROM t WHERE (k, x) IN ((5, 15), (6, 19), (6, 20))) WHERE explain LIKE '%Granules%';
SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT * FROM t WHERE k IN (2, 20) OR x = 99) WHERE explain LIKE '%Granules%';

DROP TABLE t;
