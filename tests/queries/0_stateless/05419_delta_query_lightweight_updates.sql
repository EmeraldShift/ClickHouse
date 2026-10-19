-- Tags: atomic-database
-- Delta queries with delta_query_sign_column read the lightweight updates and deletes of MergeTree tables. The delta
-- retracts each row that the updates between the cursors change, as of the old cursor, and inserts it as of the new one.
-- The targets keep every delta row, so the sum of the signs of a row is its multiplicity.

SET enable_lightweight_update = 1, lightweight_delete_mode = 'lightweight_update';

CREATE TABLE t (k UInt64, v Int64) ENGINE = MergeTree ORDER BY k
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
CREATE TABLE u (k UInt64, w Int64) ENGINE = MergeTree ORDER BY k
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
SYSTEM STOP MERGES t;
SYSTEM STOP MERGES u;

CREATE MATERIALIZED VIEW single REFRESH EVERY 10 YEAR APPEND INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT k, v FROM t SETTINGS delta_query_sign_column = 'sign';
CREATE MATERIALIZED VIEW joined REFRESH EVERY 10 YEAR APPEND INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT t.k AS k, t.v AS v, u.w AS w FROM t INNER JOIN u ON u.k = t.k SETTINGS delta_query_sign_column = 'sign';
-- u is the right side of the LEFT join, so a change of a row of u changes the rows of t with its key.
CREATE MATERIALIZED VIEW keyed REFRESH EVERY 10 YEAR APPEND INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT t.k AS k, t.v AS v, u.w AS w FROM t LEFT JOIN u ON u.k = t.k SETTINGS delta_query_sign_column = 'sign';
CREATE MATERIALIZED VIEW totals REFRESH EVERY 10 YEAR APPEND INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT t.k % 2 AS g, sum(t.v * u.w) AS s, count() AS n FROM t INNER JOIN u ON u.k = t.k GROUP BY g
    SETTINGS delta_query_sign_column = 'sign';

CREATE TABLE checks (round UInt8) ENGINE = Memory;
CREATE VIEW mismatches AS SELECT
    (SELECT count() FROM (
        (SELECT k, v, sum(sign) AS n FROM single GROUP BY k, v HAVING n != 0
            EXCEPT SELECT k, v, toInt64(count()) FROM t GROUP BY k, v)
        UNION ALL
        (SELECT k, v, toInt64(count()) FROM t GROUP BY k, v
            EXCEPT SELECT k, v, sum(sign) AS n FROM single GROUP BY k, v HAVING n != 0))) AS single,
    (SELECT count() FROM (
        (SELECT k, v, w, sum(sign) AS n FROM joined GROUP BY k, v, w HAVING n != 0
            EXCEPT SELECT t.k, t.v, u.w, toInt64(count()) FROM t INNER JOIN u ON u.k = t.k GROUP BY ALL)
        UNION ALL
        (SELECT t.k, t.v, u.w, toInt64(count()) FROM t INNER JOIN u ON u.k = t.k GROUP BY ALL
            EXCEPT SELECT k, v, w, sum(sign) AS n FROM joined GROUP BY k, v, w HAVING n != 0))) AS joined,
    (SELECT count() FROM (
        (SELECT k, v, w, sum(sign) AS n FROM keyed GROUP BY k, v, w HAVING n != 0
            EXCEPT SELECT t.k, t.v, u.w, toInt64(count()) FROM t LEFT JOIN u ON u.k = t.k GROUP BY ALL)
        UNION ALL
        (SELECT t.k, t.v, u.w, toInt64(count()) FROM t LEFT JOIN u ON u.k = t.k GROUP BY ALL
            EXCEPT SELECT k, v, w, sum(sign) AS n FROM keyed GROUP BY k, v, w HAVING n != 0))) AS keyed,
    (SELECT count() FROM (
        (SELECT g, sum(s) AS s, sum(n) AS n FROM totals GROUP BY g HAVING n != 0
            EXCEPT SELECT t.k % 2 AS g, sum(t.v * u.w), toInt64(count()) FROM t INNER JOIN u ON u.k = t.k GROUP BY g)
        UNION ALL
        (SELECT t.k % 2 AS g, sum(t.v * u.w), toInt64(count()) FROM t INNER JOIN u ON u.k = t.k GROUP BY g
            EXCEPT SELECT g, sum(s) AS s, sum(n) AS n FROM totals GROUP BY g HAVING n != 0))) AS totals;

-- Round 1: inserts only.
INSERT INTO t VALUES (1, 1), (2, 2), (3, 3);
INSERT INTO u VALUES (1, 10), (2, 20);
SYSTEM REFRESH VIEW single; SYSTEM WAIT VIEW single;
SYSTEM REFRESH VIEW joined; SYSTEM WAIT VIEW joined;
SYSTEM REFRESH VIEW keyed; SYSTEM WAIT VIEW keyed;
SYSTEM REFRESH VIEW totals; SYSTEM WAIT VIEW totals;
SELECT 'round 1', * FROM mismatches;

-- Round 2: updates of both tables, and an insert into u that matches an old row of t.
UPDATE t SET v = v + 100 WHERE k = 1;
UPDATE u SET w = 0 WHERE k = 2;
INSERT INTO t VALUES (4, 4);
INSERT INTO u VALUES (3, 30);
SYSTEM REFRESH VIEW single; SYSTEM WAIT VIEW single;
SYSTEM REFRESH VIEW joined; SYSTEM WAIT VIEW joined;
SYSTEM REFRESH VIEW keyed; SYSTEM WAIT VIEW keyed;
SYSTEM REFRESH VIEW totals; SYSTEM WAIT VIEW totals;
SELECT 'round 2', * FROM mismatches;
-- A filter by _block_number moves to PREWHERE, which must keep the column for the patches of later steps.
SELECT 'round 2 prewhere', k, w FROM u WHERE _block_number < 1000 ORDER BY k;
SELECT 'round 2 single delta', k, v, sum(sign) FROM single GROUP BY k, v HAVING sum(sign) != 1 OR k = 1 ORDER BY ALL;

-- Round 3: an update of a row inserted in the same round, and a lightweight delete of a matched row of u.
INSERT INTO t VALUES (5, 5);
UPDATE t SET v = -v WHERE k IN (2, 5);
DELETE FROM u WHERE k = 1;
SYSTEM REFRESH VIEW single; SYSTEM WAIT VIEW single;
SYSTEM REFRESH VIEW joined; SYSTEM WAIT VIEW joined;
SYSTEM REFRESH VIEW keyed; SYSTEM WAIT VIEW keyed;
SYSTEM REFRESH VIEW totals; SYSTEM WAIT VIEW totals;
SELECT 'round 3', * FROM mismatches;
SELECT 'round 3 keyed', k, v, w FROM keyed GROUP BY k, v, w HAVING sum(sign) = 1 ORDER BY ALL;

-- Round 4: two updates of one row, and an update of a row that an earlier round updated.
UPDATE t SET v = v + 1 WHERE k = 3;
UPDATE t SET v = v + 1 WHERE k = 3;
UPDATE t SET v = v * 2 WHERE k = 1;
SYSTEM REFRESH VIEW single; SYSTEM WAIT VIEW single;
SYSTEM REFRESH VIEW joined; SYSTEM WAIT VIEW joined;
SYSTEM REFRESH VIEW keyed; SYSTEM WAIT VIEW keyed;
SYSTEM REFRESH VIEW totals; SYSTEM WAIT VIEW totals;
SELECT 'round 4', * FROM mismatches;

-- Without delta_query_sign_column, a delta reads the inserts but not the updates.
CREATE TABLE cursors (round UInt8, cursor Map(String, Int64)) ENGINE = Memory;
INSERT INTO cursors SELECT 0, cursor FROM deltaQueryFrontier((SELECT k, v FROM t));
INSERT INTO t VALUES (6, 6);
INSERT INTO cursors SELECT 1, cursor FROM deltaQueryFrontier((SELECT k, v FROM t));
UPDATE t SET v = 7 WHERE k = 6;
INSERT INTO cursors SELECT 2, cursor FROM deltaQueryFrontier((SELECT k, v FROM t));
SELECT 'unsigned', * FROM deltaQuery((SELECT k, v FROM t), (SELECT cursor FROM cursors WHERE round = 0), (SELECT cursor FROM cursors WHERE round = 1));
SELECT * FROM deltaQuery((SELECT k, v FROM t), (SELECT cursor FROM cursors WHERE round = 1), (SELECT cursor FROM cursors WHERE round = 2)); -- { serverError NOT_IMPLEMENTED }
SELECT 'signed', * FROM deltaQuery((SELECT k, v FROM t SETTINGS delta_query_sign_column = 'sign'),
    (SELECT cursor FROM cursors WHERE round = 1), (SELECT cursor FROM cursors WHERE round = 2)) ORDER BY ALL;

-- A merge that writes the update into the rows below the cursor of round 1 loses the table as of that cursor. The table
-- as of the cursor of round 2 remains.
SYSTEM START MERGES t;
OPTIMIZE TABLE t FINAL;
SELECT * FROM deltaQuery((SELECT k, v FROM t SETTINGS delta_query_sign_column = 'sign'),
    (SELECT cursor FROM cursors WHERE round = 1), (SELECT cursor FROM cursors WHERE round = 2)); -- { serverError DELTA_QUERY_HISTORY_LOST }
INSERT INTO t VALUES (8, 8);
SELECT 'after merge', * FROM deltaQuery((SELECT k, v FROM t SETTINGS delta_query_sign_column = 'sign'),
    (SELECT cursor FROM cursors WHERE round = 2), (SELECT cursor FROM deltaQueryFrontier((SELECT k, v FROM t)))) ORDER BY ALL;

-- So does a mutation, also of a table without lightweight updates. The query from the empty cursor reads the mutated rows.
CREATE TABLE plain (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS enable_block_number_column = 1;
INSERT INTO plain VALUES (1), (2);
INSERT INTO cursors SELECT 3, cursor FROM deltaQueryFrontier((SELECT k FROM plain));
ALTER TABLE plain DELETE WHERE k = 1 SETTINGS mutations_sync = 2;
SELECT * FROM deltaQuery((SELECT k FROM plain), (SELECT cursor FROM cursors WHERE round = 3),
    (SELECT cursor FROM deltaQueryFrontier((SELECT k FROM plain)))); -- { serverError DELTA_QUERY_HISTORY_LOST }
SELECT 'mutated', * FROM deltaQuery((SELECT k FROM plain), map(), (SELECT cursor FROM deltaQueryFrontier((SELECT k FROM plain))));
