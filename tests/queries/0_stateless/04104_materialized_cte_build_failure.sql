-- Materialized CTEs must surface builder failures to their readers rather
-- than silently returning empty storage. The error reaches the reader
-- through `shared_future::get()` (blocking barrier) or through the
-- Status::Async reader path when the build's promise is `set_exception`'d.

SET enable_materialized_cte = 1;

DROP TABLE IF EXISTS t_04104;
CREATE TABLE t_04104 (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_04104 SELECT number FROM numbers(100);

-- `throwIf` goes in a WHERE predicate so the optimizer cannot elide it;
-- it will fire during the materialization pipeline as soon as it sees a
-- qualifying row.

-- Case 1: CTE body throws mid-execution; single reader.
WITH failing AS MATERIALIZED (
    SELECT x FROM t_04104 WHERE throwIf(x >= 50, 'boom') = 0
)
SELECT count() FROM failing;  -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- Case 2: same failure, multiple readers. Every reader should see
-- the same error, not a hang and not inconsistent results across the
-- UNION branches.
WITH failing AS MATERIALIZED (
    SELECT x FROM t_04104 WHERE throwIf(x >= 50, 'boom') = 0
)
SELECT count() FROM (
    SELECT x FROM failing WHERE x < 10
    UNION ALL
    SELECT x FROM failing WHERE x >= 10
);  -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- Case 3: failure inside a CTE read by a subsequent CTE. The outer CTE's
-- builder pipeline triggers the inner CTE's barrier, which rethrows the
-- inner failure.
WITH
    failing AS MATERIALIZED (
        SELECT x FROM t_04104 WHERE throwIf(x >= 50, 'boom') = 0
    ),
    consumer AS MATERIALIZED (
        SELECT count() AS c FROM failing
    )
SELECT c FROM consumer;  -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

DROP TABLE t_04104;
