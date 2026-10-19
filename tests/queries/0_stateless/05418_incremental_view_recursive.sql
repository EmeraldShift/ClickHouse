-- Tags: atomic-database
-- An incremental view over WITH RECURSIVE keeps the CTE in its target. Each refresh joins the new rows of the inputs with
-- the paths in the target, and repeats the step over the paths it found. Retracting an edge retracts the paths through it.

CREATE TABLE roots (node UInt64) ENGINE = MergeTree ORDER BY node SETTINGS enable_block_number_column = 1;
CREATE TABLE edges (src UInt64, dst UInt64, sign Int8) ENGINE = MergeTree ORDER BY src SETTINGS enable_block_number_column = 1;
CREATE TABLE paths (node UInt64, depth UInt32, sign Int8) ENGINE = MergeTree ORDER BY node SETTINGS enable_block_number_column = 1;

CREATE MATERIALIZED VIEW paths_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO paths EMPTY AS
    WITH RECURSIVE r AS (
        SELECT node, toUInt32(0) AS depth FROM roots
        UNION ALL
        SELECT e.dst AS node, toUInt32(r.depth + 1) AS depth FROM r JOIN edges AS e ON e.src = r.node)
    SELECT * FROM r SETTINGS delta_query_sign_column = 'sign';

-- The same query over the current edges, which have no duplicates.
CREATE VIEW expected AS
    WITH RECURSIVE r AS (
        SELECT node, toUInt32(0) AS depth FROM roots
        UNION ALL
        SELECT e.dst AS node, toUInt32(r.depth + 1) AS depth FROM r
        JOIN (SELECT src, dst FROM edges GROUP BY src, dst HAVING sum(sign) > 0) AS e ON e.src = r.node)
    SELECT node, depth, toInt64(count()) AS n FROM r GROUP BY node, depth;

CREATE VIEW actual AS SELECT node, depth, sum(sign) AS n FROM paths GROUP BY node, depth HAVING n != 0;

-- Round 1: a chain. Round 2: edges that extend old paths, where path 1-3-4 needs both new edges. Round 3: a new root.
-- Round 4: retracting edge 2-3 removes the paths through it.
INSERT INTO roots VALUES (1);
INSERT INTO edges VALUES (1, 2, 1), (2, 3, 1);
SYSTEM REFRESH VIEW paths_mv; SYSTEM WAIT VIEW paths_mv;
SELECT 'round 1', * FROM actual ORDER BY ALL;
SELECT 'round 1 differs', * FROM (SELECT * FROM actual EXCEPT SELECT * FROM expected UNION ALL SELECT * FROM expected EXCEPT SELECT * FROM actual);

INSERT INTO edges VALUES (3, 4, 1), (1, 3, 1);
SYSTEM REFRESH VIEW paths_mv; SYSTEM WAIT VIEW paths_mv;
SELECT 'round 2', * FROM actual ORDER BY ALL;
SELECT 'round 2 differs', * FROM (SELECT * FROM actual EXCEPT SELECT * FROM expected UNION ALL SELECT * FROM expected EXCEPT SELECT * FROM actual);

INSERT INTO roots VALUES (2);
SYSTEM REFRESH VIEW paths_mv; SYSTEM WAIT VIEW paths_mv;
SELECT 'round 3', * FROM actual ORDER BY ALL;
SELECT 'round 3 differs', * FROM (SELECT * FROM actual EXCEPT SELECT * FROM expected UNION ALL SELECT * FROM expected EXCEPT SELECT * FROM actual);

INSERT INTO edges VALUES (2, 3, -1);
SYSTEM REFRESH VIEW paths_mv; SYSTEM WAIT VIEW paths_mv;
SELECT 'round 4', * FROM actual ORDER BY ALL;
SELECT 'round 4 differs', * FROM (SELECT * FROM actual EXCEPT SELECT * FROM expected UNION ALL SELECT * FROM expected EXCEPT SELECT * FROM actual);

-- deltaQuery needs the table that holds the CTE, and the query must return the CTE as it is.
SELECT * FROM deltaQuery((WITH RECURSIVE r AS (SELECT node FROM roots UNION ALL SELECT e.dst FROM r JOIN edges AS e ON e.src = r.node)
    SELECT * FROM r), map(), map()); -- { serverError BAD_ARGUMENTS }
SELECT * FROM deltaQuery((WITH RECURSIVE r AS (SELECT node FROM roots UNION ALL SELECT e.dst FROM r JOIN edges AS e ON e.src = r.node)
    SELECT node FROM r), map(), map()); -- { serverError NOT_IMPLEMENTED }
