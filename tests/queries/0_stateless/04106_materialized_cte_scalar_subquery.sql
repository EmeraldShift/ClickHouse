-- Test that a scalar subquery referencing a materialized CTE works. Scalar
-- subqueries run through a fresh sub-`Planner` with `forceMaterializeCTE()`
-- set (see `evaluateScalarSubqueryIfNeeded`). That sub-planner has its own
-- `PlannerContext` and registers the same `MaterializedCTE` identity into a
-- per-query `PreparedMaterializedCTEs` registry. The registry keys by
-- pointer, so the "one future per CTE identity" invariant must still hold
-- across the sub-planner. A regression here would either double-build the
-- CTE or leave a reader blocked on a future whose build never fires.

SET enable_materialized_cte = 1;

-- Case 1: scalar subquery reading directly from a materialized CTE.
WITH ct AS MATERIALIZED (SELECT number AS n FROM numbers(10))
SELECT (SELECT sum(n) FROM ct) AS s;

-- Case 2: two scalar subqueries referencing the same materialized CTE.
-- Each scalar subquery gets its own sub-planner pass, so if the registry
-- were keyed per-`PlannerContext` this would build the CTE twice. The
-- invariant pins this to one future per CTE identity.
WITH ct AS MATERIALIZED (SELECT number AS n FROM numbers(10))
SELECT
    (SELECT sum(n) FROM ct) AS s,
    (SELECT count() FROM ct) AS c;

-- Case 3: scalar subquery inside a larger SELECT that also reads the CTE
-- directly. Exercises the main-plan reader and the scalar-subquery reader
-- sharing the same future.
WITH ct AS MATERIALIZED (SELECT number AS n FROM numbers(10))
SELECT count(), (SELECT sum(n) FROM ct) FROM ct;
