-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: the result counts 10-row granules.

-- With `read_overflow_mode_leaf = 'break'`, the shard returns 50 rows, even though the estimate of the rows to read is
-- above the leaf limit and `read_overflow_mode` is 'throw'.

DROP TABLE IF EXISTS t_leaf_break;
CREATE TABLE t_leaf_break (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 10;
INSERT INTO t_leaf_break SELECT number FROM numbers(100);

SELECT length(groupArray(x)) FROM remote('127.0.0.1', currentDatabase(), t_leaf_break)
SETTINGS max_block_size = 10, max_threads = 1, max_rows_to_read_leaf = 50, read_overflow_mode_leaf = 'break', read_overflow_mode = 'throw';

DROP TABLE t_leaf_break;
