-- Tags: no-parallel-replicas, no-random-merge-tree-settings
-- no-parallel-replicas: `max_rows_to_read` checks that a count reads no rows of its exact ranges. Reading from
--   remote replicas doesn't show that.
-- no-random-merge-tree-settings: the row limits depend on the granules.

-- An atom of a key that hides NULLs, or a relaxed atom, must not make `count()` count a row that `WHERE` rejects. Nor
-- may it hide the exact ranges of the rest of the condition.

SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SET use_query_condition_cache = 0;

-- `assumeNotNull` stores a NULL `x` in the key as an arbitrary value.
DROP TABLE IF EXISTS t_hidden_null;
CREATE TABLE t_hidden_null (k UInt32, x Nullable(UInt32)) ENGINE = MergeTree ORDER BY (k, assumeNotNull(x))
SETTINGS index_granularity = 4;
INSERT INTO t_hidden_null SELECT intDiv(number, 100), if(number % 3 = 0, NULL, number % 7) FROM numbers(1000);

SELECT count() FROM t_hidden_null WHERE x != 5;
SELECT count() FROM t_hidden_null WHERE x NOT IN (1, 5);
-- `k >= 5` is true on every row of the second half. The count reads only the first half.
SELECT count() FROM t_hidden_null WHERE k >= 5 OR x != 5 SETTINGS max_rows_to_read = 600;
-- The key can't check `x IS NULL`. The condition excludes no range, but the second half is still exact.
SELECT count() FROM t_hidden_null WHERE k >= 5 OR x IS NULL SETTINGS max_rows_to_read = 600;

-- The binary search finds the range of a condition on one key column.
DROP TABLE IF EXISTS t_relaxed;
CREATE TABLE t_relaxed (s String) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 4;
INSERT INTO t_relaxed SELECT toString(number) FROM numbers(1000);

SELECT count() FROM t_relaxed WHERE s LIKE '1%0';
SELECT count() FROM t_relaxed WHERE s LIKE '1%' SETTINGS max_rows_to_read = 20;

DROP TABLE t_hidden_null;
DROP TABLE t_relaxed;
