-- A condition on key columns that a part doesn't load in its primary index must not make the generic exclusion search
-- split ranges down to single marks. With a budget of 500 steps, no query below reaches the step limit.

DROP TABLE IF EXISTS t_exclusion_search_unloaded_key_columns;

-- `c1` changes every 4 marks. With the ratio 0.2 the part loads only `c1` in its primary index.
CREATE TABLE t_exclusion_search_unloaded_key_columns (c1 UInt32, c2 UInt8, c3 String)
ENGINE = MergeTree ORDER BY (c1, c2, c3)
SETTINGS index_granularity = 1, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0.2;

INSERT INTO t_exclusion_search_unloaded_key_columns
SELECT intDiv(number, 4), number % 4 >= 2, toString(number % 97) FROM numbers(4000)
SETTINGS max_insert_threads = 1;

SET use_lightweight_primary_key_index_analysis = 1;
SET merge_tree_coarse_index_granularity = 8;
SET merge_tree_generic_exclusion_search_max_steps = 500;
-- The query condition cache would pre-split the ranges for repeated predicates.
SET use_query_condition_cache = 0;

-- Each `c1` has 2 rows with `c2`: `count()` is 2000 and `sum(c1)` is 2 * (0 + ... + 999) = 999000. `sum` disables the
-- exact-count optimization.
SELECT count(), sum(c1) FROM t_exclusion_search_unloaded_key_columns WHERE c2 SETTINGS log_comment = '05479 sparse';

-- `use_lightweight_primary_key_index_analysis = 0` asks for the full representation of the key. A part that doesn't load
-- every key column of the condition uses the sparse one.
SELECT count(), sum(c1) FROM t_exclusion_search_unloaded_key_columns WHERE c2
SETTINGS use_lightweight_primary_key_index_analysis = 0, log_comment = '05479 full';

-- A plain `count()` collects exact ranges. The 1200 rows where `c1 < 300` are exact, and the search splits only the ranges
-- where `c1` crosses 300. Half of the other 2800 rows have `c2`.
SELECT count() FROM t_exclusion_search_unloaded_key_columns WHERE c1 < 300 OR c2
SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1, log_comment = '05479 exact';

SYSTEM FLUSH LOGS query_log;

SELECT splitByChar(' ', log_comment)[2] AS test_case, ProfileEvents['IndexGenericExclusionSearchStepLimitReached'] AS step_limit_reached
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05479 %'
ORDER BY test_case;

DROP TABLE t_exclusion_search_unloaded_key_columns;
