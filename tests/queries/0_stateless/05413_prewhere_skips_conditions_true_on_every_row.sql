-- Tags: no-parallel-replicas, no-random-merge-tree-settings, no-random-settings
-- no-parallel-replicas: `max_bytes_to_read` checks the bytes that one server reads.
-- no-random-merge-tree-settings, no-random-settings: the byte limits depend on the granules.

-- Index analysis finds the conditions of the PREWHERE that are true on every row of a granule. The PREWHERE steps of
-- that granule skip those conditions and do not read their columns.

SET use_query_condition_cache = 0, optimize_use_projections = 0, optimize_use_implicit_projections = 0;

DROP TABLE IF EXISTS t_prewhere_true_conditions;
CREATE TABLE t_prewhere_true_conditions (k UInt64, v UInt64)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 100, index_granularity_bytes = 0;
INSERT INTO t_prewhere_true_conditions SELECT number, number * 7 FROM numbers(100000);

-- The primary key finds `k >= 50050` true on every row after the first granule. Those granules read `v` only.
SELECT count() FROM t_prewhere_true_conditions PREWHERE k >= 50050 AND v % 10 = 3 SETTINGS max_bytes_to_read = 500000;
SELECT count() FROM t_prewhere_true_conditions PREWHERE v % 10 = 3 AND k >= 50050 SETTINGS max_bytes_to_read = 500000;
-- An unsplit PREWHERE is one condition, and the primary key does not find it true.
SELECT count() FROM t_prewhere_true_conditions PREWHERE k >= 50050 AND v % 10 = 3 SETTINGS enable_multiple_prewhere_read_steps = 0;

-- The query returns `k`, so it is read anyway.
SELECT sum(k) FROM t_prewhere_true_conditions WHERE k >= 50050 AND v % 10 = 3;

-- Each condition is true on different granules.
SELECT count(), sum(v) FROM t_prewhere_true_conditions
PREWHERE k >= 20050 AND k < 79950 AND v % 10 = 3 SETTINGS max_bytes_to_read = 700000;

-- Index analysis rewrites `NOT (k < 20050 OR k >= 79950)` into `k >= 20050 AND k < 79950`. The condition is true where both are.
SELECT count(), sum(v) FROM t_prewhere_true_conditions
PREWHERE NOT (k < 20050 OR k >= 79950) AND v % 10 = 3 SETTINGS max_bytes_to_read = 700000;

-- The query condition cache records the granules without matching rows for the whole PREWHERE.
SELECT count() FROM t_prewhere_true_conditions WHERE k >= 50050 AND v % 1000 = 3 SETTINGS use_query_condition_cache = 1;
SELECT count() FROM t_prewhere_true_conditions WHERE k >= 50050 AND v % 1000 = 3 SETTINGS use_query_condition_cache = 1;

-- A row policy is a condition too.
DROP ROW POLICY IF EXISTS p_05413 ON t_prewhere_true_conditions;
CREATE ROW POLICY p_05413 ON t_prewhere_true_conditions USING k >= 50000 TO ALL;
SELECT count() FROM t_prewhere_true_conditions PREWHERE v % 10 = 3 SETTINGS max_bytes_to_read = 500000;
DROP ROW POLICY p_05413 ON t_prewhere_true_conditions;

DROP TABLE t_prewhere_true_conditions;

-- The partition key finds `k >= 25000` true on every row of the parts that are read.
DROP TABLE IF EXISTS t_prewhere_true_conditions_partitioned;
CREATE TABLE t_prewhere_true_conditions_partitioned (k UInt64, v UInt64)
ENGINE = MergeTree PARTITION BY intDiv(k, 25000) ORDER BY tuple() SETTINGS index_granularity = 100, index_granularity_bytes = 0;
INSERT INTO t_prewhere_true_conditions_partitioned SELECT number, number * 7 FROM numbers(100000);

SELECT count() FROM t_prewhere_true_conditions_partitioned PREWHERE k >= 25000 AND v % 10 = 3 SETTINGS max_bytes_to_read = 700000;

DROP TABLE t_prewhere_true_conditions_partitioned;
