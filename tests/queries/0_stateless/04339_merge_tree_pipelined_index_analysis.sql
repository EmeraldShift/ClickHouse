DROP TABLE IF EXISTS t_pipelined_index_analysis;

CREATE TABLE t_pipelined_index_analysis
(
    k UInt64,
    v UInt64,
    payload String,
    INDEX ix_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;

INSERT INTO t_pipelined_index_analysis SELECT number, number % 10, toString(number) FROM numbers(1000);

SELECT sum(k) FROM t_pipelined_index_analysis WHERE v = 3 SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'ix_v', use_skip_indexes_on_data_read = 0, merge_tree_pipelined_index_analysis = 0;
SELECT sum(k) FROM t_pipelined_index_analysis WHERE v = 3 SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'ix_v', use_skip_indexes_on_data_read = 0, merge_tree_pipelined_index_analysis = 1;

EXPLAIN PIPELINE SELECT sum(k) FROM t_pipelined_index_analysis WHERE v = 3 SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'ix_v', use_skip_indexes_on_data_read = 0, merge_tree_pipelined_index_analysis = 1, max_threads = 2, merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 0;

DROP TABLE t_pipelined_index_analysis;
