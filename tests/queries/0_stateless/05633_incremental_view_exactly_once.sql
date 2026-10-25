-- Tags: atomic-database, no-parallel, no-fasttest
-- Exactly-once for `REFRESH ... INCREMENTAL` views. A refresh records the bounds of its inputs before its first write and inserts
-- with deduplication tokens that follow from its cursor and those bounds. A refresh that fails after it appends its rows, before
-- its cursor persists, retries from the same cursor. The retry reads the same bounds and repeats the same INSERTs, which the
-- targets and the hidden tables drop. Rows that arrive between the failure and the retry go to the next refresh. The first
-- refresh empties the hidden tables before it fills them. Driven by the refresh_mv_incremental_fail_after_append failpoint
-- (global server state -> no-parallel).

CREATE TABLE src (k UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;

-- A view without hidden tables, a view whose residual keeps an integral, and two views whose subqueries keep stage tables
-- and whose residuals keep integrals. The stage of an aggregate has a residual too, and the stage of a filter doesn't. The
-- targets keep a deduplication log. refresh_retries = 0, so each refresh below runs once.
CREATE TABLE rows_tgt (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS non_replicated_deduplication_window = 100;
CREATE MATERIALIZED VIEW rows_mv REFRESH EVERY 10 YEAR SETTINGS refresh_retries = 0 INCREMENTAL TO rows_tgt EMPTY
    AS SELECT k FROM src;
CREATE MATERIALIZED VIEW counts_mv REFRESH EVERY 10 YEAR SETTINGS refresh_retries = 0 INCREMENTAL
    ENGINE = CollapsingMergeTree(sign) ORDER BY (g, c) SETTINGS non_replicated_deduplication_window = 100 EMPTY
    AS SELECT k % 3 AS g, count() AS c FROM src GROUP BY g HAVING c > 1 SETTINGS delta_query_sign_column = 'sign';
CREATE MATERIALIZED VIEW histogram_mv REFRESH EVERY 10 YEAR SETTINGS refresh_retries = 0 INCREMENTAL
    ENGINE = CollapsingMergeTree(sign) ORDER BY (n, users) SETTINGS non_replicated_deduplication_window = 100 EMPTY
    AS SELECT n, count() AS users FROM (SELECT k % 4 AS g, count() AS n FROM src GROUP BY g) GROUP BY n HAVING users > 0
    SETTINGS delta_query_sign_column = 'sign';
CREATE MATERIALIZED VIEW filtered_mv REFRESH EVERY 10 YEAR SETTINGS refresh_retries = 0 INCREMENTAL
    ENGINE = CollapsingMergeTree(sign) ORDER BY (g, c) SETTINGS non_replicated_deduplication_window = 100 EMPTY
    AS SELECT g, count() AS c FROM (SELECT k % 2 AS g FROM src WHERE k % 5 != 0) GROUP BY g HAVING c > 0
    SETTINGS delta_query_sign_column = 'sign';

-- Round 1: rows 0..4. The first refreshes of the views with hidden tables fail after they fill their hidden tables and targets.
-- Their retries empty the hidden tables first, so the integrals and the stage tables hold each row once. A residual makes
-- the target equal to itself over the integral, so the retries don't add rows to the targets either.
INSERT INTO src SELECT number FROM numbers(5);
SYSTEM REFRESH VIEW rows_mv; SYSTEM WAIT VIEW rows_mv;
SYSTEM ENABLE FAILPOINT refresh_mv_incremental_fail_after_append;
SYSTEM REFRESH VIEW counts_mv; SYSTEM WAIT VIEW counts_mv; -- { serverError REFRESH_FAILED }
SYSTEM REFRESH VIEW histogram_mv; SYSTEM WAIT VIEW histogram_mv; -- { serverError REFRESH_FAILED }
SYSTEM REFRESH VIEW filtered_mv; SYSTEM WAIT VIEW filtered_mv; -- { serverError REFRESH_FAILED }
SYSTEM DISABLE FAILPOINT refresh_mv_incremental_fail_after_append;
SYSTEM REFRESH VIEW counts_mv; SYSTEM WAIT VIEW counts_mv;
SYSTEM REFRESH VIEW histogram_mv; SYSTEM WAIT VIEW histogram_mv;
SYSTEM REFRESH VIEW filtered_mv; SYSTEM WAIT VIEW filtered_mv;
-- The first refreshes create the hidden tables.
SELECT 'hidden tables', countIf(name LIKE '.inner_integral.%'), countIf(name LIKE '.inner_stage.%')
FROM system.tables WHERE database = currentDatabase();

CREATE VIEW contents AS
    SELECT 'rows' AS view, toString((count(), uniqExact(k))) AS content FROM rows_tgt
    UNION ALL SELECT 'counts', toString(groupArray((g, c))) FROM (SELECT g, c FROM counts_mv GROUP BY g, c HAVING sum(sign) > 0 ORDER BY g)
    UNION ALL SELECT 'histogram', toString(groupArray((n, users))) FROM (SELECT n, users FROM histogram_mv GROUP BY n, users HAVING sum(sign) > 0 ORDER BY n)
    UNION ALL SELECT 'filtered', toString(groupArray((g, c))) FROM (SELECT g, c FROM filtered_mv GROUP BY g, c HAVING sum(sign) > 0 ORDER BY g);

SELECT 'round 1', * FROM contents ORDER BY ALL;

-- Round 2: rows 5..9. Each refresh fails after it appends its rows, then rows 10..14 arrive before the retries.
INSERT INTO src SELECT number FROM numbers(5, 5);
SYSTEM ENABLE FAILPOINT refresh_mv_incremental_fail_after_append;
SYSTEM REFRESH VIEW rows_mv; SYSTEM WAIT VIEW rows_mv; -- { serverError REFRESH_FAILED }
SYSTEM REFRESH VIEW counts_mv; SYSTEM WAIT VIEW counts_mv; -- { serverError REFRESH_FAILED }
SYSTEM REFRESH VIEW histogram_mv; SYSTEM WAIT VIEW histogram_mv; -- { serverError REFRESH_FAILED }
SYSTEM REFRESH VIEW filtered_mv; SYSTEM WAIT VIEW filtered_mv; -- { serverError REFRESH_FAILED }
SYSTEM DISABLE FAILPOINT refresh_mv_incremental_fail_after_append;
SELECT 'failed', * FROM contents ORDER BY ALL;
INSERT INTO src SELECT number FROM numbers(10, 5);

-- The retries read rows 0..9 again and insert nothing new.
SYSTEM REFRESH VIEW rows_mv; SYSTEM WAIT VIEW rows_mv;
SYSTEM REFRESH VIEW counts_mv; SYSTEM WAIT VIEW counts_mv;
SYSTEM REFRESH VIEW histogram_mv; SYSTEM WAIT VIEW histogram_mv;
SYSTEM REFRESH VIEW filtered_mv; SYSTEM WAIT VIEW filtered_mv;
SELECT 'retried', * FROM contents ORDER BY ALL;

-- The next refreshes add rows 10..14.
SYSTEM REFRESH VIEW rows_mv; SYSTEM WAIT VIEW rows_mv;
SYSTEM REFRESH VIEW counts_mv; SYSTEM WAIT VIEW counts_mv;
SYSTEM REFRESH VIEW histogram_mv; SYSTEM WAIT VIEW histogram_mv;
SYSTEM REFRESH VIEW filtered_mv; SYSTEM WAIT VIEW filtered_mv;
SELECT 'next', * FROM contents ORDER BY ALL;
SELECT 'expected counts', toString(groupArray((g, c))) FROM (SELECT k % 3 AS g, count() AS c FROM src GROUP BY g HAVING c > 1 ORDER BY g);
SELECT 'expected histogram', toString(groupArray((n, users)))
FROM (SELECT n, count() AS users FROM (SELECT k % 4 AS g, count() AS n FROM src GROUP BY g) GROUP BY n ORDER BY n);
SELECT 'expected filtered', toString(groupArray((g, c))) FROM (SELECT k % 2 AS g, count() AS c FROM src WHERE k % 5 != 0 GROUP BY g ORDER BY g);
