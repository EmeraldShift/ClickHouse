-- Tags: atomic-database
-- Incremental views over chains of LEFT joins match their queries, with and without join_use_nulls. A reduction of the right
-- table of a LEFT join doesn't follow an equality in WHERE, which would remove the match of a left row and pad it instead.
-- A reduction by the right table of a LEFT join keeps the defaults that pad the left rows without a match. Only the terms of
-- its own changes reduce the left side of its ON by it.
-- The targets keep every delta row, so the sum of the signs of a row is its multiplicity.

CREATE TABLE a (k UInt64, w UInt64) ENGINE = MergeTree ORDER BY k SETTINGS enable_block_number_column = 1;
CREATE TABLE b (k UInt64, j UInt64) ENGINE = MergeTree ORDER BY k SETTINGS enable_block_number_column = 1;
CREATE TABLE c (j UInt64, name String) ENGINE = MergeTree ORDER BY j SETTINGS enable_block_number_column = 1;
CREATE TABLE d (w UInt64, v UInt64) ENGINE = MergeTree ORDER BY w SETTINGS enable_block_number_column = 1;
CREATE TABLE e (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS enable_block_number_column = 1;

CREATE MATERIALIZED VIEW chain REFRESH EVERY 10 YEAR APPEND INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT a.k AS k, b.j AS j, c.name AS name FROM a LEFT JOIN b ON b.k = a.k LEFT JOIN c ON c.j = b.j
    SETTINGS delta_query_sign_column = 'sign';
CREATE MATERIALIZED VIEW chain_nulls REFRESH EVERY 10 YEAR APPEND INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT a.k AS k, b.j AS j, c.name AS name FROM a LEFT JOIN b ON b.k = a.k LEFT JOIN c ON c.j = b.j
    SETTINGS delta_query_sign_column = 'sign', join_use_nulls = 1;
CREATE MATERIALIZED VIEW inner_chain REFRESH EVERY 10 YEAR APPEND INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT a.k AS k, b.j AS j, c.name AS name FROM a LEFT JOIN b ON b.k = a.k INNER JOIN c ON c.j = b.j
    SETTINGS delta_query_sign_column = 'sign';
CREATE MATERIALIZED VIEW cross REFRESH EVERY 10 YEAR APPEND INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT a.k AS k, c.name AS name, b.j AS j FROM a CROSS JOIN c LEFT JOIN b ON b.k = a.k AND b.j = c.j
    SETTINGS delta_query_sign_column = 'sign';
-- A new row of d with v = 0 must not match a row of a whose row of e has v = 5.
CREATE MATERIALIZED VIEW filtered REFRESH EVERY 10 YEAR APPEND INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT a.k AS k, d.v AS v FROM a INNER JOIN d ON d.w = a.w LEFT JOIN e ON e.k = a.k WHERE e.v = d.v
    SETTINGS delta_query_sign_column = 'sign';

-- Round 1: rows of a without matches. Round 2: b arrives for one of them. Round 3: c arrives, and a new row of a matches b.
INSERT INTO a VALUES (1, 1), (2, 1);
INSERT INTO e VALUES (1, 5);
SYSTEM REFRESH VIEW chain; SYSTEM WAIT VIEW chain;
SYSTEM REFRESH VIEW chain_nulls; SYSTEM WAIT VIEW chain_nulls;
SYSTEM REFRESH VIEW filtered; SYSTEM WAIT VIEW filtered;
SYSTEM REFRESH VIEW cross; SYSTEM WAIT VIEW cross;
SELECT 'round 1', 'chain', k, j, name FROM chain GROUP BY ALL HAVING sum(sign) = 1 ORDER BY ALL;
SELECT 'round 1', 'chain_nulls', k, j, name FROM chain_nulls GROUP BY ALL HAVING sum(sign) = 1 ORDER BY ALL;

INSERT INTO b VALUES (1, 10);
INSERT INTO d VALUES (1, 0);
SYSTEM REFRESH VIEW chain; SYSTEM WAIT VIEW chain;
SYSTEM REFRESH VIEW chain_nulls; SYSTEM WAIT VIEW chain_nulls;
SYSTEM REFRESH VIEW filtered; SYSTEM WAIT VIEW filtered;
SYSTEM REFRESH VIEW cross; SYSTEM WAIT VIEW cross;
SELECT 'round 2', 'chain', k, j, name FROM chain GROUP BY ALL HAVING sum(sign) = 1 ORDER BY ALL;
SELECT 'round 2', 'chain_nulls', k, j, name FROM chain_nulls GROUP BY ALL HAVING sum(sign) = 1 ORDER BY ALL;
SELECT 'round 2', 'filtered', k, v, sum(sign) FROM filtered GROUP BY ALL HAVING sum(sign) != 0 ORDER BY ALL;

INSERT INTO c VALUES (10, 'x');
INSERT INTO a VALUES (1, 2);
INSERT INTO d VALUES (1, 5);
SYSTEM REFRESH VIEW chain; SYSTEM WAIT VIEW chain;
SYSTEM REFRESH VIEW chain_nulls; SYSTEM WAIT VIEW chain_nulls;
SYSTEM REFRESH VIEW filtered; SYSTEM WAIT VIEW filtered;
SYSTEM REFRESH VIEW cross; SYSTEM WAIT VIEW cross;
SELECT 'round 3', 'chain', k, j, name, sum(sign) FROM chain GROUP BY ALL HAVING sum(sign) != 0 ORDER BY ALL;
SELECT 'round 3', 'chain_nulls', k, j, name, sum(sign) FROM chain_nulls GROUP BY ALL HAVING sum(sign) != 0 ORDER BY ALL;
SELECT 'round 3', 'filtered', k, v, sum(sign) FROM filtered GROUP BY ALL HAVING sum(sign) != 0 ORDER BY ALL;

-- Round 4: c gets a row that matches the default of b.j. Round 5: a new row of a without a match in b joins it, and a row
-- of a that joined it gets a match in b.
INSERT INTO c VALUES (0, 'zero');
SYSTEM REFRESH VIEW chain; SYSTEM WAIT VIEW chain;
SYSTEM REFRESH VIEW chain_nulls; SYSTEM WAIT VIEW chain_nulls;
SYSTEM REFRESH VIEW inner_chain; SYSTEM WAIT VIEW inner_chain;
SYSTEM REFRESH VIEW cross; SYSTEM WAIT VIEW cross;
SELECT 'round 4', 'chain', k, j, name, sum(sign) FROM chain GROUP BY ALL HAVING sum(sign) != 0 ORDER BY ALL;
SELECT 'round 4', 'chain_nulls', k, j, name, sum(sign) FROM chain_nulls GROUP BY ALL HAVING sum(sign) != 0 ORDER BY ALL;
SELECT 'round 4', 'inner_chain', k, j, name, sum(sign) FROM inner_chain GROUP BY ALL HAVING sum(sign) != 0 ORDER BY ALL;

INSERT INTO a VALUES (3, 3);
INSERT INTO b VALUES (2, 20);
SYSTEM REFRESH VIEW chain; SYSTEM WAIT VIEW chain;
SYSTEM REFRESH VIEW chain_nulls; SYSTEM WAIT VIEW chain_nulls;
SYSTEM REFRESH VIEW inner_chain; SYSTEM WAIT VIEW inner_chain;
SYSTEM REFRESH VIEW cross; SYSTEM WAIT VIEW cross;
SELECT 'round 5', 'chain', k, j, name, sum(sign) FROM chain GROUP BY ALL HAVING sum(sign) != 0 ORDER BY ALL;
SELECT 'round 5', 'chain_nulls', k, j, name, sum(sign) FROM chain_nulls GROUP BY ALL HAVING sum(sign) != 0 ORDER BY ALL;
SELECT 'round 5', 'inner_chain', k, j, name, sum(sign) FROM inner_chain GROUP BY ALL HAVING sum(sign) != 0 ORDER BY ALL;

-- The views equal their queries, with multiplicities.
SELECT 'chain mismatches', count() FROM (
    (SELECT k, j, name, sum(sign) AS n FROM chain GROUP BY ALL HAVING n != 0 EXCEPT
        SELECT a.k, b.j, c.name, toInt64(count()) FROM a LEFT JOIN b ON b.k = a.k LEFT JOIN c ON c.j = b.j GROUP BY ALL)
    UNION ALL
    (SELECT a.k, b.j, c.name, toInt64(count()) FROM a LEFT JOIN b ON b.k = a.k LEFT JOIN c ON c.j = b.j GROUP BY ALL EXCEPT
        SELECT k, j, name, sum(sign) AS n FROM chain GROUP BY ALL HAVING n != 0));

SELECT 'inner_chain mismatches', count() FROM (
    (SELECT k, j, name, sum(sign) AS n FROM inner_chain GROUP BY ALL HAVING n != 0 EXCEPT
        SELECT a.k, b.j, c.name, toInt64(count()) FROM a LEFT JOIN b ON b.k = a.k INNER JOIN c ON c.j = b.j GROUP BY ALL)
    UNION ALL
    (SELECT a.k, b.j, c.name, toInt64(count()) FROM a LEFT JOIN b ON b.k = a.k INNER JOIN c ON c.j = b.j GROUP BY ALL EXCEPT
        SELECT k, j, name, sum(sign) AS n FROM inner_chain GROUP BY ALL HAVING n != 0));

SELECT 'cross mismatches', count() FROM (
    (SELECT k, name, j, sum(sign) AS n FROM cross GROUP BY ALL HAVING n != 0 EXCEPT
        SELECT a.k, c.name, b.j, toInt64(count()) FROM a CROSS JOIN c LEFT JOIN b ON b.k = a.k AND b.j = c.j GROUP BY ALL)
    UNION ALL
    (SELECT a.k, c.name, b.j, toInt64(count()) FROM a CROSS JOIN c LEFT JOIN b ON b.k = a.k AND b.j = c.j GROUP BY ALL EXCEPT
        SELECT k, name, j, sum(sign) AS n FROM cross GROUP BY ALL HAVING n != 0));
