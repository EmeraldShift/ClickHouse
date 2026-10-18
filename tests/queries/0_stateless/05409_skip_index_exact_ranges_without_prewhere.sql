-- Tags: no-parallel-replicas, no-random-merge-tree-settings, no-random-settings
-- no-parallel-replicas: `max_bytes_to_read` checks the bytes that one server reads.
-- no-random-merge-tree-settings, no-random-settings: the byte limits depend on the granules.

-- A minmax index checks at read time where the filter is true on every row of a granule. The read of those granules
-- skips the PREWHERE steps and reads only the columns that the query returns.

SET use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, use_query_condition_cache = 0;
SET optimize_use_projections = 0, optimize_use_implicit_projections = 0;

DROP TABLE IF EXISTS t_exact_ranges;
CREATE TABLE t_exact_ranges (k UInt64, x UInt64, s String, INDEX ix x TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 100, index_granularity_bytes = 0;
-- Each granule holds one value of `x`.
INSERT INTO t_exact_ranges SELECT number, intDiv(number, 1000), repeat('a', 100) FROM numbers(100000);

-- The index keeps only exact granules, and the count reads no column.
SELECT count() FROM t_exact_ranges WHERE x >= 50 SETTINGS max_bytes_to_read = 100000;
SELECT count() FROM t_exact_ranges WHERE x >= 50 SETTINGS use_skip_indexes = 0;

-- The index finds the exact granules of an `OR` with a column that it doesn't store. The count reads `x` and `k` on the
-- other granules only.
SELECT count() FROM t_exact_ranges WHERE x >= 50 OR k = 3 SETTINGS max_bytes_to_read = 1000000;
SELECT count() FROM t_exact_ranges WHERE x >= 50 OR k = 3 SETTINGS use_skip_indexes = 0;

-- The read of the exact granules reads `k` only.
SELECT sum(k) FROM t_exact_ranges WHERE x >= 50 SETTINGS max_bytes_to_read = 500000;
SELECT sum(k) FROM t_exact_ranges WHERE x >= 50 SETTINGS use_skip_indexes = 0;

-- Exact granules and other granules alternate in a read task.
SELECT count(), sum(k), sum(length(s)) FROM t_exact_ranges WHERE x % 2 = 0 AND x >= 50 OR k % 7 = 0;
SELECT count(), sum(k), sum(length(s)) FROM t_exact_ranges WHERE x % 2 = 0 AND x >= 50 OR k % 7 = 0 SETTINGS use_skip_indexes = 0;
SELECT count(), sum(k) FROM t_exact_ranges WHERE x IN (10, 30, 50) OR k % 7 = 0;
SELECT count(), sum(k) FROM t_exact_ranges WHERE x IN (10, 30, 50) OR k % 7 = 0 SETTINGS use_skip_indexes = 0;

-- Reads in the order of the key, forward and in reverse.
SELECT groupArray(k) FROM (SELECT k FROM t_exact_ranges WHERE x = 98 OR k % 1000 = 1 ORDER BY k DESC LIMIT 3);
SELECT groupArray(k) FROM (SELECT k FROM t_exact_ranges WHERE x = 98 OR k % 1000 = 1 ORDER BY k LIMIT 3 OFFSET 97);
SELECT sum(cityHash64(k, x)) FROM (SELECT k, x FROM t_exact_ranges WHERE x >= 90 OR k % 1000 = 1 ORDER BY k);
SELECT sum(cityHash64(k, x)) FROM (SELECT k, x FROM t_exact_ranges WHERE x >= 90 OR k % 1000 = 1 ORDER BY k) SETTINGS use_skip_indexes = 0;

-- The query returns the filter column.
SELECT x >= 50 AS c, count() FROM t_exact_ranges WHERE c GROUP BY c;

-- The read of the exact granules still drops the rows that a lightweight delete removed.
DELETE FROM t_exact_ranges WHERE k % 10 = 0 SETTINGS lightweight_deletes_sync = 2;
SELECT count() FROM t_exact_ranges WHERE x >= 50;
SELECT count() FROM t_exact_ranges WHERE x >= 50 SETTINGS use_skip_indexes = 0;
SELECT count(), sum(k) FROM t_exact_ranges WHERE x IN (10, 30, 50) OR k % 7 = 0;
SELECT count(), sum(k) FROM t_exact_ranges WHERE x IN (10, 30, 50) OR k % 7 = 0 SETTINGS use_skip_indexes = 0;

DROP TABLE t_exact_ranges;
