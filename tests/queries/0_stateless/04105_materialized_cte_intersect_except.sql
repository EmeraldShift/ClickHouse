-- Materialized CTE referenced from multiple branches of INTERSECT / EXCEPT.
-- The multi-branch-reading-same-CTE pattern hits the Status::Async reader
-- fan-out: every branch barriers on the same future, every reader gets its
-- own duped readiness fd, exactly one build runs.

SET enable_materialized_cte = 1;

DROP TABLE IF EXISTS t_04105;
CREATE TABLE t_04105 (c Int32) ENGINE = MergeTree ORDER BY c;
INSERT INTO t_04105 SELECT number FROM numbers(20);

-- INTERSECT: both sides read the same CTE; only values present in both
-- sides of the intersect survive.
WITH ct AS MATERIALIZED (SELECT c FROM t_04105 WHERE c < 15)
SELECT count() FROM (
    SELECT c FROM ct WHERE c < 10
    INTERSECT
    SELECT c FROM ct WHERE c >= 5
);

-- EXCEPT: LHS reads CTE, RHS reads CTE, difference computed.
WITH ct AS MATERIALIZED (SELECT c FROM t_04105 WHERE c < 15)
SELECT count() FROM (
    SELECT c FROM ct
    EXCEPT
    SELECT c FROM ct WHERE c < 10
);

-- Chained: UNION of (A INTERSECT B) branches, all over the same CTE.
WITH ct AS MATERIALIZED (SELECT c FROM t_04105)
SELECT count() FROM (
    (SELECT c FROM ct WHERE c < 15 INTERSECT SELECT c FROM ct WHERE c >= 5)
    UNION ALL
    (SELECT c FROM ct WHERE c < 8 INTERSECT SELECT c FROM ct WHERE c >= 3)
);

DROP TABLE t_04105;
