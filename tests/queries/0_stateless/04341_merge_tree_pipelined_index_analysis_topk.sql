DROP TABLE IF EXISTS t_pipelined_topk;

CREATE TABLE t_pipelined_topk
(
    k UInt64,
    v UInt64,
    INDEX ix_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;

INSERT INTO t_pipelined_topk SELECT number, if(number < 10, number, 1000 + number) FROM numbers(1000);

-- Static minmax TopK narrowing plus dynamic threshold state should work in the pipelined pool.
SELECT groupArray(k) FROM (SELECT k FROM t_pipelined_topk ORDER BY v LIMIT 5 SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'ix_v', use_skip_indexes_on_data_read = 1, merge_tree_pipelined_index_analysis = 1, use_skip_indexes_for_top_k = 1, use_top_k_dynamic_filtering = 1, max_threads = 2, merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 0);
EXPLAIN PIPELINE SELECT k FROM t_pipelined_topk ORDER BY v LIMIT 5 SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'ix_v', use_skip_indexes_on_data_read = 1, merge_tree_pipelined_index_analysis = 1, use_skip_indexes_for_top_k = 1, use_top_k_dynamic_filtering = 1, max_threads = 2, merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 0;

-- Dynamic-only TopK: disable analysis-time TopK index selection but keep the dynamic PREWHERE threshold filter.
SELECT groupArray(k) FROM (SELECT k FROM t_pipelined_topk ORDER BY v LIMIT 5 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, merge_tree_pipelined_index_analysis = 1, use_skip_indexes_for_top_k = 0, use_top_k_dynamic_filtering = 1, max_threads = 2, merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 0);
EXPLAIN PIPELINE SELECT k FROM t_pipelined_topk ORDER BY v LIMIT 5 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, merge_tree_pipelined_index_analysis = 1, use_skip_indexes_for_top_k = 0, use_top_k_dynamic_filtering = 1, max_threads = 2, merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 0;

DROP TABLE t_pipelined_topk;
