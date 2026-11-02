-- Tags: no-parallel-replicas, no-random-merge-tree-settings
-- no-parallel-replicas: `max_rows_to_read` checks that a count reads no rows of its exact ranges. Reading from
--   remote replicas doesn't show that.
-- no-random-merge-tree-settings: the row limits depend on the granules.

-- `fromModifiedJulianDay` is injective and monotonic. A comparison of `x` through the key `fromModifiedJulianDay(x)` is
-- exact, and `count()` reads only the granules at the ends of its ranges. A constant that the cast to `Int32`
-- changes can't use the key, and the count must still be right.

SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_injective;
CREATE TABLE t_injective (x Int32) ENGINE = MergeTree ORDER BY fromModifiedJulianDay(x) SETTINGS index_granularity = 4;
INSERT INTO t_injective SELECT 58000 + number FROM numbers(1000);

SELECT count() FROM t_injective WHERE x > 58300 SETTINGS max_rows_to_read = 8;
SELECT count() FROM t_injective WHERE x NOT IN (58300, 58301, 58302) SETTINGS max_rows_to_read = 16;
SELECT count() FROM t_injective WHERE x >= 58300.5;
SELECT count() FROM t_injective WHERE x < 3000000000;

DROP TABLE t_injective;
