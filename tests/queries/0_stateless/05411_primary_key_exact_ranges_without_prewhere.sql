-- Tags: no-parallel-replicas, no-random-merge-tree-settings, no-random-settings
-- no-parallel-replicas: `max_bytes_to_read` checks the bytes that one server reads.
-- no-random-merge-tree-settings, no-random-settings: the byte limits depend on the granules.

-- The primary key finds the ranges where the filter is true on every row. The read of those ranges skips the PREWHERE
-- steps and reads only the columns that the query returns.

SET use_query_condition_cache = 0, optimize_use_projections = 0, optimize_use_implicit_projections = 0;

DROP TABLE IF EXISTS t_pk_exact_ranges;
CREATE TABLE t_pk_exact_ranges (k UInt64, b UInt8)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 100, index_granularity_bytes = 0;
INSERT INTO t_pk_exact_ranges SELECT number, number % 10 FROM numbers(100000);

-- The count reads no column, and the sum reads `b` only.
SELECT count() FROM t_pk_exact_ranges WHERE k >= 50000 SETTINGS max_bytes_to_read = 5000;
SELECT sum(b) FROM t_pk_exact_ranges WHERE k >= 50000 SETTINGS max_bytes_to_read = 100000;
SELECT sum(b) FROM t_pk_exact_ranges WHERE k >= 50000 SETTINGS use_primary_key = 0;

-- The first granule is not exact.
SELECT count(), sum(b) FROM t_pk_exact_ranges WHERE k >= 50050 SETTINGS max_bytes_to_read = 100000;
SELECT count(), sum(b) FROM t_pk_exact_ranges WHERE k >= 50050 SETTINGS use_primary_key = 0;

-- Exact ranges and other ranges alternate in a read task.
SELECT count(), sum(b) FROM t_pk_exact_ranges WHERE k BETWEEN 10000 AND 19999 OR k BETWEEN 30050 AND 39999
SETTINGS max_bytes_to_read = 50000;
SELECT count(), sum(b) FROM t_pk_exact_ranges WHERE k BETWEEN 10000 AND 19999 OR k BETWEEN 30050 AND 39999
SETTINGS use_primary_key = 0;

-- Reads in the order of the key, forward and in reverse.
SELECT groupArray(k) FROM (SELECT k, b FROM t_pk_exact_ranges WHERE k >= 99900 OR k BETWEEN 50 AND 149 ORDER BY k LIMIT 10 OFFSET 95);
SELECT groupArray(k) FROM (SELECT k, b FROM t_pk_exact_ranges WHERE k >= 99900 OR k BETWEEN 50 AND 149 ORDER BY k DESC LIMIT 10 OFFSET 95);

DROP TABLE t_pk_exact_ranges;
