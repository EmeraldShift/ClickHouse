-- Tags: no-random-merge-tree-settings
-- Random MergeTree settings change the granules that the row limits depend on.

-- A NULL makes only the atoms that read it NULL. Where it can't change the result of the whole condition, the primary
-- key still finds the ranges where the condition is true on every row, and `count()` doesn't read them.

SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SET enable_parallel_replicas = 0, use_query_condition_cache = 0;
SET merge_tree_min_rows_for_seek = 0, merge_tree_min_bytes_for_seek = 0;

-- Half of the rows have a NULL `x`. The key puts them last for each `k`.
DROP TABLE IF EXISTS t_null_key;
CREATE TABLE t_null_key (k UInt32, x Nullable(UInt32)) ENGINE = MergeTree ORDER BY (k, x)
SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi', allow_nullable_key = 1;
INSERT INTO t_null_key SELECT intDiv(number, 100), if(number % 2 = 0, NULL, number % 10) FROM numbers(1000);

SELECT count() FROM t_null_key WHERE x != 5;
-- `k >= 5` is true on every row in the second half, also where `x` is NULL. The count doesn't read these rows.
SELECT count() FROM t_null_key WHERE k >= 5 OR x < 3 SETTINGS max_rows_to_read = 200;

-- `ts > X` makes a group of two atoms, one for `toDate(ts)` and one for `ts`. Both are NULL where `ts` is NULL.
DROP TABLE IF EXISTS t_atom_group;
CREATE TABLE t_atom_group (k UInt32, ts Nullable(DateTime('UTC'))) ENGINE = MergeTree ORDER BY (k, toDate(ts), ts)
SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi', allow_nullable_key = 1;
INSERT INTO t_atom_group SELECT intDiv(number, 100), if(number % 2 = 0, NULL, toDateTime('2020-01-01 00:00:00', 'UTC') + number * 3600)
FROM numbers(1000);

SELECT count() FROM t_atom_group WHERE ts > toDateTime('2020-01-02 01:00:00', 'UTC');

-- With a float in `t`, the `NOT` stays above the `t < (0., 1)` atom. That atom is NULL on the row `(0., NULL)`. The
-- row lies inside its granule's range.
DROP TABLE IF EXISTS t_nested_null;
CREATE TABLE t_nested_null (t Tuple(Float64, Nullable(UInt32))) ENGINE = MergeTree ORDER BY t
SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi', allow_nullable_key = 1;
INSERT INTO t_nested_null SELECT (intDiv(number, 4) * 2 + (number % 4 = 3), multiIf(number % 4 < 2, 5, number % 4 = 2, NULL, 0))
FROM numbers(100);

SELECT count() FROM t_nested_null WHERE NOT (t < (0., 1));
-- Rows where `t < (20., 0)` is NULL lie above `(20., 0)`, outside the atom's range. Without a `NOT` above the atom,
-- the filter skips them as if the atom were false. The count doesn't read the granules below `(20., 0)`.
SELECT count() FROM t_nested_null WHERE t < (20., 0) SETTINGS max_rows_to_read = 8;
-- `t > (20., 0)` is NULL on the row `(20., NULL)` inside its range. The primary key doesn't search it as one continuous
-- range.
SELECT count() FROM t_nested_null WHERE t > (20., 0);

-- Half of the rows have a NULL `x`, and the key puts them last.
DROP TABLE IF EXISTS t_null_granules;
CREATE TABLE t_null_granules (x Nullable(Float64)) ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi', allow_nullable_key = 1;
INSERT INTO t_null_granules SELECT if(number % 2 = 0, NULL, number) FROM numbers(100);

-- With a float `x`, the `NOT` stays above the `x < 5` atom. The atom is NULL on the NULL rows, and so is the `NOT`.
SELECT count() FROM t_null_granules WHERE NOT (x < 5);
-- `x <=> 5` is false on a NULL row, so the `NOT` is true there.
SELECT count() FROM t_null_granules WHERE NOT (x <=> 5);
-- `assumeNotNull(x)` returns a value on a NULL row.
SELECT (SELECT count() FROM t_null_granules WHERE assumeNotNull(x) > 40)
    = (SELECT count() FROM t_null_granules WHERE assumeNotNull(x) > 40 SETTINGS use_primary_key = 0);
-- With `transform_null_in`, `x IN (NULL)` is true on a NULL row. The count doesn't read the all-NULL granules.
SELECT count() FROM t_null_granules WHERE x IN (NULL) SETTINGS transform_null_in = 1, max_rows_to_read = 8;

DROP TABLE t_null_key;
DROP TABLE t_atom_group;
DROP TABLE t_nested_null;
DROP TABLE t_null_granules;
