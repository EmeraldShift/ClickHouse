-- Tags: no-parallel-replicas
-- no-parallel-replicas: the test checks EXPLAIN of plans with reading steps replaced by prepared sources.

-- Column statistics answer min, max, and count on a part where the filter is true on every row. The other parts are
-- read.

SET use_statistics_for_min_max_aggregation = 1, materialize_statistics_on_insert = 1;
SET optimize_use_projections = 1, optimize_use_implicit_projections = 1, enable_analyzer = 1;
SET optimize_aggregation_in_order = 0, force_aggregation_in_order = 0, aggregate_functions_null_for_empty = 0;

DROP TABLE IF EXISTS t_stats_exact_parts;
CREATE TABLE t_stats_exact_parts (d Date, v Int64)
ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY tuple()
SETTINGS auto_statistics_types = 'minmax';

-- 100 rows a day from 2020-01-01 to 2020-03-31, one part a month.
INSERT INTO t_stats_exact_parts SELECT toDate('2020-01-01') + intDiv(number, 100), number FROM numbers(9100);

-- The filter is true on every row of the February and March parts. The statistics answer the query, and no part is read.
SELECT min(v), max(v), count() FROM t_stats_exact_parts WHERE d >= '2020-02-01';
SELECT min(v), max(v), count() FROM t_stats_exact_parts WHERE d >= '2020-02-01' SETTINGS use_statistics_for_min_max_aggregation = 0;
SELECT countIf(explain LIKE '%_statistics_min_max_projection%'), countIf(explain LIKE '%ReadFromMergeTree%') FROM (EXPLAIN SELECT min(v), max(v), count() FROM t_stats_exact_parts WHERE d >= '2020-02-01');

-- The filter is true on every row of the March part only. The February part is read.
SELECT min(v), max(v), count() FROM t_stats_exact_parts WHERE d >= '2020-02-15';
SELECT min(v), max(v), count() FROM t_stats_exact_parts WHERE d >= '2020-02-15' SETTINGS use_statistics_for_min_max_aggregation = 0;
SELECT countIf(explain LIKE '%_statistics_min_max_projection%'), countIf(explain LIKE '%ReadFromMergeTree%') FROM (EXPLAIN SELECT min(v), max(v), count() FROM t_stats_exact_parts WHERE d >= '2020-02-15');

-- A row policy is a filter too. This one is true on every row of the March part only.
DROP ROW POLICY IF EXISTS p_05412 ON t_stats_exact_parts;
CREATE ROW POLICY p_05412 ON t_stats_exact_parts USING d >= '2020-02-15' TO ALL;
SELECT min(v), max(v), count() FROM t_stats_exact_parts;
SELECT min(v), max(v), count() FROM t_stats_exact_parts SETTINGS use_statistics_for_min_max_aggregation = 0;
SELECT countIf(explain LIKE '%_statistics_min_max_projection%'), countIf(explain LIKE '%ReadFromMergeTree%') FROM (EXPLAIN SELECT min(v), max(v), count() FROM t_stats_exact_parts);
DROP ROW POLICY p_05412 ON t_stats_exact_parts;

DROP TABLE t_stats_exact_parts;
