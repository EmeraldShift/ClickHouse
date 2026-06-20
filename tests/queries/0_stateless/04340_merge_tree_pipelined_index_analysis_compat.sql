DROP TABLE IF EXISTS t_pipelined_compat;
DROP TABLE IF EXISTS t_pipelined_text;
DROP TABLE IF EXISTS t_pipelined_final;

CREATE TABLE t_pipelined_compat
(
    k UInt64,
    v UInt64,
    payload String,
    INDEX ix_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;

INSERT INTO t_pipelined_compat SELECT number, number % 10, toString(number) FROM numbers(1000);

-- QCC should not be poisoned by running a limited pipelined query first.
SELECT k FROM t_pipelined_compat WHERE v = 3 LIMIT 1 FORMAT Null SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'ix_v', use_skip_indexes_on_data_read = 0, merge_tree_pipelined_index_analysis = 1, use_query_condition_cache = 1, max_threads = 2, merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 0;
SELECT count() FROM t_pipelined_compat WHERE v = 3 SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'ix_v', use_skip_indexes_on_data_read = 0, merge_tree_pipelined_index_analysis = 1, use_query_condition_cache = 1, max_threads = 2, merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 0;
SELECT count() FROM t_pipelined_compat WHERE v = 3 SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'ix_v', use_skip_indexes_on_data_read = 0, merge_tree_pipelined_index_analysis = 1, use_query_condition_cache = 0, max_threads = 2, merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 0;

-- Row-limit checks still happen on the pipelined path.
SELECT sum(length(payload)) FROM t_pipelined_compat WHERE v = 3 SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'ix_v', use_skip_indexes_on_data_read = 0, merge_tree_pipelined_index_analysis = 1, max_rows_to_read = 10, read_overflow_mode = 'throw', max_threads = 2, merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 0; -- { serverError TOO_MANY_ROWS }

CREATE TABLE t_pipelined_text
(
    k UInt64,
    s String,
    INDEX ix_s s TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1000000
)
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;

INSERT INTO t_pipelined_text SELECT number, if(number % 10 = 0, 'needle', 'hay') FROM numbers(1000);

-- Direct text read should not depend on use_skip_indexes_on_data_read.
SET enable_full_text_index = 1;
SELECT count() FROM t_pipelined_text WHERE hasToken(s, 'needle') SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'ix_s', query_plan_direct_read_from_text_index = 1, use_skip_indexes_on_data_read = 0, merge_tree_pipelined_index_analysis = 1, max_threads = 2, merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 0;

-- Prefetch is still the existing prefetch path when requested.
EXPLAIN PIPELINE SELECT sum(k) FROM t_pipelined_compat WHERE v = 3 SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'ix_v', use_skip_indexes_on_data_read = 1, merge_tree_pipelined_index_analysis = 1, allow_prefetched_read_pool_for_local_filesystem = 1, local_filesystem_read_method = 'pread_threadpool', max_threads = 2, merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 0;

-- In-order reads stay on the existing in-order path.
EXPLAIN PIPELINE SELECT k FROM t_pipelined_compat WHERE k < 10 ORDER BY k LIMIT 3 SETTINGS merge_tree_pipelined_index_analysis = 1, max_threads = 2;

CREATE TABLE t_pipelined_final
(
    k UInt64,
    version UInt64,
    v UInt64,
    payload String,
    INDEX ix_v v TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree(version)
ORDER BY k
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;

INSERT INTO t_pipelined_final VALUES (1, 1, 7, 'old'), (1, 2, 7, 'new'), (2, 1, 3, 'keep'), (3, 1, 4, 'skip');
SELECT groupArray((k, payload)) FROM t_pipelined_final FINAL WHERE v IN (3, 7) SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'ix_v', use_skip_indexes_on_data_read = 0, merge_tree_pipelined_index_analysis = 1, max_threads = 2, merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 0;
EXPLAIN PIPELINE SELECT groupArray((k, payload)) FROM t_pipelined_final FINAL WHERE v IN (3, 7) SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'ix_v', use_skip_indexes_on_data_read = 0, merge_tree_pipelined_index_analysis = 1, max_threads = 2, merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 0;

DROP TABLE t_pipelined_final;
DROP TABLE t_pipelined_text;
DROP TABLE t_pipelined_compat;
