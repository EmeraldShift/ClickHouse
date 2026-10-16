-- A condition on key columns that a part does not load in its primary index must not make the generic exclusion search
-- split ranges down to single marks. The selected marks and the results must be as without a step limit.

DROP TABLE IF EXISTS t_exclusion_search_unloaded_key_columns;

-- `c1` changes every 4 marks, so with the ratio 0.2 the part loads only `c1` in its primary index.
CREATE TABLE t_exclusion_search_unloaded_key_columns (c1 UInt32, c2 UInt8, c3 String)
ENGINE = MergeTree ORDER BY (c1, c2, c3)
SETTINGS index_granularity = 1, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0.2;

INSERT INTO t_exclusion_search_unloaded_key_columns
SELECT intDiv(number, 4), number % 4 >= 2, toString(number % 97) FROM numbers(4000)
SETTINGS max_insert_threads = 1;

-- The same, but the partition key bounds `c2` to `[0, 1]` in one part and to `[2, 3]` in the other.
DROP TABLE IF EXISTS t_exclusion_search_partition_bound;
CREATE TABLE t_exclusion_search_partition_bound (c1 UInt32, c2 UInt8, c3 String)
ENGINE = MergeTree ORDER BY (c1, c2, c3) PARTITION BY c2 < 2
SETTINGS index_granularity = 1, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0.2;

INSERT INTO t_exclusion_search_partition_bound
SELECT intDiv(number, 4), number % 4, toString(number % 97) FROM numbers(4000)
SETTINGS max_insert_threads = 1;

SET use_lightweight_primary_key_index_analysis = 1;
SET merge_tree_coarse_index_granularity = 8;
-- The query condition cache would pre-split the ranges for repeated predicates.
SET use_query_condition_cache = 0;

-- { echoOn }

-- `sum` disables the exact-count optimization.
SELECT count(), sum(c1) FROM t_exclusion_search_unloaded_key_columns WHERE c2
SETTINGS merge_tree_generic_exclusion_search_max_steps = 500, log_comment = '05406_unloaded only_unloaded budget';
SELECT count(), sum(c1) FROM t_exclusion_search_unloaded_key_columns WHERE c2
SETTINGS merge_tree_generic_exclusion_search_max_steps = 0, log_comment = '05406_unloaded only_unloaded unlimited';

SELECT count(), sum(c1) FROM t_exclusion_search_unloaded_key_columns WHERE c1 BETWEEN 100 AND 800 AND c2
SETTINGS merge_tree_generic_exclusion_search_max_steps = 500, log_comment = '05406_unloaded and_unloaded budget';
SELECT count(), sum(c1) FROM t_exclusion_search_unloaded_key_columns WHERE c1 BETWEEN 100 AND 800 AND c2
SETTINGS merge_tree_generic_exclusion_search_max_steps = 0, log_comment = '05406_unloaded and_unloaded unlimited';

SELECT count(), sum(c1) FROM t_exclusion_search_unloaded_key_columns WHERE c1 < 300 OR c2
SETTINGS merge_tree_generic_exclusion_search_max_steps = 500, log_comment = '05406_unloaded or_unloaded budget';
SELECT count(), sum(c1) FROM t_exclusion_search_unloaded_key_columns WHERE c1 < 300 OR c2
SETTINGS merge_tree_generic_exclusion_search_max_steps = 0, log_comment = '05406_unloaded or_unloaded unlimited';

SELECT count(), sum(c1) FROM t_exclusion_search_unloaded_key_columns WHERE NOT (c1 >= 300 AND c2)
SETTINGS merge_tree_generic_exclusion_search_max_steps = 500, log_comment = '05406_unloaded not_and_unloaded budget';
SELECT count(), sum(c1) FROM t_exclusion_search_unloaded_key_columns WHERE NOT (c1 >= 300 AND c2)
SETTINGS merge_tree_generic_exclusion_search_max_steps = 0, log_comment = '05406_unloaded not_and_unloaded unlimited';

-- A plain `count()` collects exact ranges for the exact-count optimization. An atom on `c2` is assumed false to prove
-- ranges exact, otherwise every range would count as matching.
SELECT count() FROM t_exclusion_search_unloaded_key_columns WHERE c1 < 300 OR c2
SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1, merge_tree_generic_exclusion_search_max_steps = 500, log_comment = '05406_unloaded exact_or_unloaded budget';
SELECT count() FROM t_exclusion_search_unloaded_key_columns WHERE c1 < 300 OR c2
SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1, merge_tree_generic_exclusion_search_max_steps = 0, log_comment = '05406_unloaded exact_or_unloaded unlimited';

SELECT count() FROM t_exclusion_search_unloaded_key_columns WHERE NOT (c1 >= 300 AND c2)
SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1, merge_tree_generic_exclusion_search_max_steps = 500, log_comment = '05406_unloaded exact_not_and_unloaded budget';
SELECT count() FROM t_exclusion_search_unloaded_key_columns WHERE NOT (c1 >= 300 AND c2)
SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1, merge_tree_generic_exclusion_search_max_steps = 0, log_comment = '05406_unloaded exact_not_and_unloaded unlimited';

SELECT count() FROM t_exclusion_search_unloaded_key_columns WHERE c2
SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1, merge_tree_generic_exclusion_search_max_steps = 500, log_comment = '05406_unloaded exact_only_unloaded budget';
SELECT count() FROM t_exclusion_search_unloaded_key_columns WHERE c2
SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1, merge_tree_generic_exclusion_search_max_steps = 0, log_comment = '05406_unloaded exact_only_unloaded unlimited';

SELECT count(), sum(c1) FROM t_exclusion_search_partition_bound WHERE c2 = 1
SETTINGS merge_tree_generic_exclusion_search_max_steps = 500, log_comment = '05406_unloaded partition_bound budget';
SELECT count(), sum(c1) FROM t_exclusion_search_partition_bound WHERE c2 = 1
SETTINGS merge_tree_generic_exclusion_search_max_steps = 0, log_comment = '05406_unloaded partition_bound unlimited';

SYSTEM FLUSH LOGS query_log;

SELECT
    splitByChar(' ', log_comment)[2] AS test_case,
    maxIf(ProfileEvents['IndexGenericExclusionSearchStepLimitReached'], splitByChar(' ', log_comment)[3] = 'budget') AS step_limit_reached_with_budget,
    maxIf(ProfileEvents['SelectedMarks'], splitByChar(' ', log_comment)[3] = 'budget')
        = maxIf(ProfileEvents['SelectedMarks'], splitByChar(' ', log_comment)[3] = 'unlimited') AS same_marks_as_unlimited
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05406_unloaded %'
GROUP BY test_case
ORDER BY test_case;

-- The results without the primary key.
SELECT count(), sum(c1) FROM t_exclusion_search_unloaded_key_columns WHERE c2 SETTINGS use_primary_key = 0;
SELECT count(), sum(c1) FROM t_exclusion_search_unloaded_key_columns WHERE c1 BETWEEN 100 AND 800 AND c2 SETTINGS use_primary_key = 0;
SELECT count(), sum(c1) FROM t_exclusion_search_unloaded_key_columns WHERE c1 < 300 OR c2 SETTINGS use_primary_key = 0;
SELECT count(), sum(c1) FROM t_exclusion_search_unloaded_key_columns WHERE NOT (c1 >= 300 AND c2) SETTINGS use_primary_key = 0;
SELECT count(), sum(c1) FROM t_exclusion_search_partition_bound WHERE c2 = 1 SETTINGS use_primary_key = 0;

-- { echoOff }

DROP TABLE t_exclusion_search_unloaded_key_columns;
DROP TABLE t_exclusion_search_partition_bound;
