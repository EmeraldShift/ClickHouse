-- Tags: no-parallel-replicas, no-random-merge-tree-settings
-- no-parallel-replicas: `max_rows_to_read` checks that a count reads no rows of the ranges it proves exact, which
--   reading from remote replicas does not show.
-- no-random-merge-tree-settings: the row limits depend on the granules.

-- An atom of a key that hides NULLs, a relaxed atom, or an offset-only filter must not make `count()` count a row that
-- `WHERE` rejects. Nor may it stop the rest of the condition from proving ranges exact.

SET optimize_use_projections = 1, optimize_use_implicit_projections = 1, optimize_trivial_count_query = 1;
SET use_query_condition_cache = 0;

-- `assumeNotNull` stores a NULL `x` as an arbitrary value, so the key cannot tell a NULL from a value.
DROP TABLE IF EXISTS t_hidden_null;
CREATE TABLE t_hidden_null (k UInt32, x Nullable(UInt32)) ENGINE = MergeTree ORDER BY (k, assumeNotNull(x))
SETTINGS index_granularity = 4;
INSERT INTO t_hidden_null SELECT intDiv(number, 100), if(number % 3 = 0, NULL, number % 7) FROM numbers(1000);

SELECT 'a key that hides NULLs';
SELECT count() FROM t_hidden_null WHERE x != 5;
SELECT count() FROM t_hidden_null WHERE x != 5 SETTINGS optimize_use_implicit_projections = 0;
SELECT count() FROM t_hidden_null WHERE NOT (x = 5);
SELECT count() FROM t_hidden_null WHERE NOT (x = 5) SETTINGS optimize_use_implicit_projections = 0;
SELECT count() FROM t_hidden_null WHERE x NOT IN (1, 5);
SELECT count() FROM t_hidden_null WHERE x NOT IN (1, 5) SETTINGS optimize_use_implicit_projections = 0;
SELECT count() FROM t_hidden_null WHERE x IS NULL;
SELECT count() FROM t_hidden_null WHERE x IS NULL SETTINGS optimize_use_implicit_projections = 0;
SELECT count() FROM t_hidden_null WHERE x IS NOT NULL;
SELECT count() FROM t_hidden_null WHERE x IS NOT NULL SETTINGS optimize_use_implicit_projections = 0;

-- `k >= 5` is true on every row of the second half, so the count reads only the first half.
SELECT count() FROM t_hidden_null WHERE k >= 5 OR x != 5 SETTINGS max_rows_to_read = 600;
SELECT count() FROM t_hidden_null WHERE k >= 5 OR x != 5 SETTINGS optimize_use_implicit_projections = 0;

-- The binary search finds the range of a condition on one key column.
DROP TABLE IF EXISTS t_relaxed;
CREATE TABLE t_relaxed (s String) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 4;
INSERT INTO t_relaxed SELECT toString(number) FROM numbers(1000);

SELECT 'a relaxed atom';
SELECT count() FROM t_relaxed WHERE s LIKE '1%0';
SELECT count() FROM t_relaxed WHERE s LIKE '1%0' SETTINGS optimize_use_implicit_projections = 0;
SELECT count() FROM t_relaxed WHERE s LIKE '1%' SETTINGS max_rows_to_read = 20;
SELECT count() FROM t_relaxed WHERE s LIKE '1%' SETTINGS optimize_use_implicit_projections = 0;

SELECT 'an offset-only filter';
SELECT count() FROM t_relaxed WHERE _part_offset >= 7;
SELECT count() FROM t_relaxed WHERE _part_offset >= 7 SETTINGS optimize_use_implicit_projections = 0;

-- The trivial count by a partition predicate, which only the old analyzer uses, counts a partition if the predicate
-- is true on every row of it.
SET enable_analyzer = 0;

DROP TABLE IF EXISTS t_partition_count;
CREATE TABLE t_partition_count (p String, v UInt32) ENGINE = MergeTree PARTITION BY p ORDER BY v;
INSERT INTO t_partition_count SELECT 'xyz', number FROM numbers(10);
INSERT INTO t_partition_count SELECT 'qq', number FROM numbers(10);

DROP TABLE IF EXISTS t_partition_count_single;
CREATE TABLE t_partition_count_single (p String, v UInt32) ENGINE = MergeTree PARTITION BY p ORDER BY v;
INSERT INTO t_partition_count_single SELECT 'xyz', number FROM numbers(10);

SELECT 'a trivial count by a partition predicate';
SELECT count() FROM t_partition_count WHERE p NOT LIKE 'ab%c'
SETTINGS optimize_use_implicit_projections = 0, max_rows_to_read = 1;
-- No partition can be skipped, but `p = 'xyz'` is true on every row of the only partition.
SELECT count() FROM t_partition_count_single WHERE p = 'xyz' OR sipHash64(p) = 0
SETTINGS optimize_use_implicit_projections = 0, max_rows_to_read = 1;
-- The predicate is undecided on the partition `qq`.
SELECT count() FROM t_partition_count WHERE p = 'xyz' OR sipHash64(p) = 0
SETTINGS optimize_use_implicit_projections = 0, max_rows_to_read = 1; -- { serverError TOO_MANY_ROWS }

-- `NOT LIKE 'ab%c'` is undecided on the partition `abzc`.
INSERT INTO t_partition_count SELECT 'abzc', number FROM numbers(10);
SELECT count() FROM t_partition_count WHERE p NOT LIKE 'ab%c'
SETTINGS optimize_use_implicit_projections = 0, max_rows_to_read = 1; -- { serverError TOO_MANY_ROWS }
SELECT count() FROM t_partition_count WHERE p NOT LIKE 'ab%c' SETTINGS optimize_use_implicit_projections = 0;

DROP TABLE t_hidden_null;
DROP TABLE t_relaxed;
DROP TABLE t_partition_count;
DROP TABLE t_partition_count_single;
