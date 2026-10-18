-- Tags: no-parallel-replicas, no-random-merge-tree-settings, no-random-settings
-- no-parallel-replicas: `max_rows_to_read` checks that a count reads no rows of its exact ranges. Reading from
--   remote replicas doesn't show that.
-- no-random-merge-tree-settings, no-random-settings: the row limits depend on the granules.

-- A minmax index makes a granule an exact range if the filter is true on every row of the granule. `count()` counts
-- the rows of an exact range without reading them. A NULL or a NaN in a granule must keep the granule from being exact.

SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SET use_query_condition_cache = 0, use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;

DROP TABLE IF EXISTS t_skip_index_exact;
CREATE TABLE t_skip_index_exact
(
    x UInt32,
    y UInt32,
    n Nullable(UInt32),
    f Float64,
    INDEX ix x TYPE minmax GRANULARITY 1,
    INDEX iy y TYPE minmax GRANULARITY 3,
    INDEX i_n n TYPE minmax GRANULARITY 1,
    INDEX i_f f TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;

INSERT INTO t_skip_index_exact
SELECT
    intDiv(number, 10),
    intDiv(999 - number, 10),
    if(number BETWEEN 500 AND 509, NULL, intDiv(number, 10)),
    if(number BETWEEN 500 AND 509, nan, intDiv(number, 10))
FROM numbers(1000);

-- The granule of rows 48 to 51 holds `x = 4` and `x = 5`, so it is the only granule that the count reads. The
-- row limits leave room for a few rows more.
SELECT count() FROM t_skip_index_exact WHERE x >= 5 SETTINGS max_rows_to_read = 10;
SELECT count() FROM t_skip_index_exact WHERE x >= 5 SETTINGS optimize_use_implicit_projections = 0;
SELECT count() FROM t_skip_index_exact WHERE NOT (x < 5) SETTINGS max_rows_to_read = 10;
SELECT count() FROM t_skip_index_exact WHERE x >= 5 AND x < 50 SETTINGS max_rows_to_read = 10;
SELECT count() FROM t_skip_index_exact WHERE x >= 5 AND x < 50 SETTINGS optimize_use_implicit_projections = 0;
SELECT count() FROM t_skip_index_exact WHERE x < 5 OR x >= 50 SETTINGS max_rows_to_read = 10;
SELECT count() FROM t_skip_index_exact WHERE x < 5 OR x >= 50 SETTINGS optimize_use_implicit_projections = 0;

-- `ix` keeps every granule for `x >= 5 OR y = 3`. It still finds the granules where `x >= 5` is true on every row, so the
-- count reads only the 13 granules of rows 0 to 51.
SELECT count() FROM t_skip_index_exact WHERE x >= 5 OR y = 3 SETTINGS use_skip_indexes_for_disjunctions = 0, max_rows_to_read = 60;
SELECT count() FROM t_skip_index_exact WHERE x >= 5 OR y = 3 SETTINGS optimize_use_implicit_projections = 0;

-- A granule of `iy` covers 12 rows, and the granule of rows 948 to 959 is not exact.
SELECT count() FROM t_skip_index_exact WHERE y >= 5 SETTINGS max_rows_to_read = 20;

-- The granules of rows 500 to 507 hold only NULLs, so the count skips them. The granule of rows 508 to 511 holds NULLs
-- and values, so it is not exact.
SELECT count() FROM t_skip_index_exact WHERE n >= 5 SETTINGS max_rows_to_read = 20;
SELECT count() FROM t_skip_index_exact WHERE n >= 5 SETTINGS optimize_use_implicit_projections = 0;
SELECT count() FROM t_skip_index_exact WHERE n IS NOT NULL SETTINGS max_rows_to_read = 10;
SELECT count() FROM t_skip_index_exact WHERE n IS NOT NULL SETTINGS optimize_use_implicit_projections = 0;
SELECT count() FROM t_skip_index_exact WHERE NOT (n < 5);
SELECT count() FROM t_skip_index_exact WHERE NOT (n < 5) SETTINGS optimize_use_implicit_projections = 0;

-- The bounds of a granule skip NaN. A NaN row is false for `f >= 5`, so no granule of `f` is exact for it, and true for
-- `NOT (f < 5)`.
SELECT count() FROM t_skip_index_exact WHERE f >= 5;
SELECT count() FROM t_skip_index_exact WHERE f >= 5 SETTINGS optimize_use_implicit_projections = 0;
SELECT count() FROM t_skip_index_exact WHERE NOT (f < 5);
SELECT count() FROM t_skip_index_exact WHERE NOT (f < 5) SETTINGS optimize_use_implicit_projections = 0;

-- Each index checks only its own column, so neither index makes a granule exact for a condition on both columns.
SELECT count() FROM t_skip_index_exact WHERE x >= 5 AND y >= 5;
SELECT count() FROM t_skip_index_exact WHERE x >= 5 AND y >= 5 SETTINGS optimize_use_implicit_projections = 0;

DROP TABLE t_skip_index_exact;

-- The bounds of a `Tuple` leave out a NULL inside it. The first granule holds `(2, 1)` and `(2, NULL)`, and
-- `(2, NULL) < (2, 3)` is NULL.
DROP TABLE IF EXISTS t_skip_index_tuple;
CREATE TABLE t_skip_index_tuple (t Tuple(Int32, Nullable(Int32)), INDEX it t TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;
INSERT INTO t_skip_index_tuple VALUES ((2, 1)), ((2, NULL)), ((2, 1)), ((2, 2));
SELECT count() FROM t_skip_index_tuple WHERE t < (2, 3);
SELECT count() FROM t_skip_index_tuple WHERE t < (2, 3) SETTINGS optimize_use_implicit_projections = 0;
DROP TABLE t_skip_index_tuple;

-- The bounds of a `Map` leave out a NULL inside its keys too.
DROP TABLE IF EXISTS t_skip_index_map;
CREATE TABLE t_skip_index_map (m Map(Tuple(Nullable(Int32), Int32), UInt8), INDEX im m TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;
INSERT INTO t_skip_index_map VALUES (map((1, 0), 0)), (map((NULL, 0), 0)), (map((1, 0), 0)), (map((1, 0), 0));
SELECT count() FROM t_skip_index_map WHERE m < map((2, 0), 0);
SELECT count() FROM t_skip_index_map WHERE m < map((2, 0), 0) SETTINGS optimize_use_implicit_projections = 0;
SELECT count() FROM t_skip_index_map WHERE m < map((2, 0), 0) SETTINGS use_skip_indexes = 0;
DROP TABLE t_skip_index_map;

-- The minmax index makes the middle granule exact. The set index keeps the granules on both sides and merges them
-- into one range across the exact granule.
DROP TABLE IF EXISTS t_skip_index_seek;
CREATE TABLE t_skip_index_seek
(
    x UInt8,
    INDEX ix x TYPE minmax GRANULARITY 1,
    INDEX iset x TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_skip_index_seek VALUES (0), (1), (0), (1), (1), (1), (1), (1), (0), (1), (0), (1);
SELECT count() FROM t_skip_index_seek WHERE x = 1 SETTINGS merge_tree_min_rows_for_seek = 1000;
DROP TABLE t_skip_index_seek;
