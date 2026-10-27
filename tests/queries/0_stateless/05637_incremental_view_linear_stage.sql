-- Tags: atomic-database
-- A stage whose aggregates are sums of numbers is linear when its view reads them only in sums of products with one
-- factor from the stage. Its stage table then gets rows of partial sums of each group, whose merges add them up. The other
-- views read their stages in ways that partial sums would change, or have a name that SummingMergeTree can't sum, so those
-- stages keep their residuals. A mutation of the
-- input makes each stage insert a correction, which subtracts from the partial sums, the counts too.

CREATE TABLE facts (grp UInt64, item UInt64, amt Int64) ENGINE = MergeTree ORDER BY grp
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;
CREATE TABLE rates (item UInt64, w Float64, sign Int8) ENGINE = MergeTree ORDER BY item
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE MATERIALIZED VIEW linear_mv REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT p.grp AS grp, sum(-p.q * m.w) AS total, sum(p.n * m.w) AS n
    FROM (SELECT grp, item, sum(amt) AS q, count() AS n FROM facts GROUP BY grp, item) AS p JOIN rates AS m ON p.item = m.item
    GROUP BY grp SETTINGS delta_query_sign_column = 'sign';
-- count counts rows, and a group can have several rows of partial sums.
CREATE MATERIALIZED VIEW counted_mv REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT p.grp AS grp, sum(p.q * m.w) AS total, count(p.q) AS n
    FROM (SELECT grp, item, sum(amt) AS q, count() AS n FROM facts GROUP BY grp, item) AS p JOIN rates AS m ON p.item = m.item
    GROUP BY grp SETTINGS delta_query_sign_column = 'sign';
-- A filter by a sum passes some partial sums of a group and not others.
CREATE MATERIALIZED VIEW filtered_mv REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT p.grp AS grp, sum(p.q * m.w) AS total, sum(p.n) AS n
    FROM (SELECT grp, item, sum(amt) AS q, count() AS n FROM facts GROUP BY grp, item) AS p JOIN rates AS m ON p.item = m.item
    WHERE p.q > 0 GROUP BY grp SETTINGS delta_query_sign_column = 'sign';
-- The square of a sum isn't the sum of the squares of its parts.
CREATE MATERIALIZED VIEW squared_mv REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT p.grp AS grp, sum(p.q * p.q * m.w) AS total, sum(p.n) AS n
    FROM (SELECT grp, item, sum(amt) AS q, count() AS n FROM facts GROUP BY grp, item) AS p JOIN rates AS m ON p.item = m.item
    GROUP BY grp SETTINGS delta_query_sign_column = 'sign';
-- The view reads `min` linearly, but the minimum of a group isn't the sum of the minimums of its parts.
CREATE MATERIALIZED VIEW min_mv REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT p.grp AS grp, sum(p.lo * m.w) AS total, sum(p.n) AS n
    FROM (SELECT grp, item, min(amt) AS lo, count() AS n FROM facts GROUP BY grp, item) AS p JOIN rates AS m ON p.item = m.item
    GROUP BY grp SETTINGS delta_query_sign_column = 'sign';
-- A residual counts the rows of each group to see whether the group exists.
CREATE MATERIALIZED VIEW having_mv REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT p.grp AS grp, sum(p.q * m.w) AS total
    FROM (SELECT grp, item, sum(amt) AS q, count() AS n FROM facts GROUP BY grp, item) AS p JOIN rates AS m ON p.item = m.item
    GROUP BY grp HAVING sum(p.q) < 2 SETTINGS delta_query_sign_column = 'sign';
-- An expression over the alias of a sum needs a residual too.
CREATE MATERIALIZED VIEW alias_mv REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT p.grp AS grp, sum(p.q * m.w) AS total, total + 1 AS bumped
    FROM (SELECT grp, item, sum(amt) AS q, count() AS n FROM facts GROUP BY grp, item) AS p JOIN rates AS m ON p.item = m.item
    GROUP BY grp SETTINGS delta_query_sign_column = 'sign';
-- SummingMergeTree reads a dotted name as a column of a Nested table.
CREATE MATERIALIZED VIEW dotted_mv REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT p.grp AS grp, sum(p.`q.x` * m.w) AS total, sum(p.n) AS n
    FROM (SELECT grp, item, sum(amt) AS `q.x`, count() AS n FROM facts GROUP BY grp, item) AS p JOIN rates AS m ON p.item = m.item
    GROUP BY grp SETTINGS delta_query_sign_column = 'sign';

CREATE VIEW contents AS
    SELECT 'linear', toString(groupArray((grp, total, n))) FROM (SELECT grp, sum(total) AS total, sum(n) AS n FROM linear_mv GROUP BY grp HAVING n != 0 ORDER BY grp)
    UNION ALL SELECT 'counted', toString(groupArray((grp, total, n))) FROM (SELECT grp, sum(total) AS total, sum(n) AS n FROM counted_mv GROUP BY grp HAVING n != 0 ORDER BY grp)
    UNION ALL SELECT 'filtered', toString(groupArray((grp, total, n))) FROM (SELECT grp, sum(total) AS total, sum(n) AS n FROM filtered_mv GROUP BY grp HAVING n != 0 ORDER BY grp)
    UNION ALL SELECT 'squared', toString(groupArray((grp, total, n))) FROM (SELECT grp, sum(total) AS total, sum(n) AS n FROM squared_mv GROUP BY grp HAVING n != 0 ORDER BY grp)
    UNION ALL SELECT 'min', toString(groupArray((grp, total, n))) FROM (SELECT grp, sum(total) AS total, sum(n) AS n FROM min_mv GROUP BY grp HAVING n != 0 ORDER BY grp)
    UNION ALL SELECT 'having', toString(groupArray((grp, total, s))) FROM (SELECT grp, total, sum(sign) AS s FROM having_mv GROUP BY grp, total HAVING s != 0 ORDER BY ALL)
    UNION ALL SELECT 'alias', toString(groupArray((grp, bumped, s))) FROM (SELECT grp, bumped, sum(sign) AS s FROM alias_mv GROUP BY grp, bumped HAVING s != 0 ORDER BY ALL)
    UNION ALL SELECT 'dotted', toString(groupArray((grp, total, n))) FROM (SELECT grp, sum(total) AS total, sum(n) AS n FROM dotted_mv GROUP BY grp HAVING n != 0 ORDER BY grp);
CREATE VIEW net_rates AS SELECT item, w FROM rates GROUP BY item, w HAVING sum(sign) > 0;
CREATE VIEW expected AS
    SELECT 'linear', toString(groupArray((grp, total, n))) FROM (SELECT p.grp AS grp, sum(-p.q * m.w) AS total, sum(p.n * m.w) AS n
        FROM (SELECT grp, item, sum(amt) AS q, count() AS n FROM facts GROUP BY grp, item) AS p JOIN net_rates AS m ON p.item = m.item GROUP BY grp ORDER BY grp)
    UNION ALL SELECT 'counted', toString(groupArray((grp, total, n))) FROM (SELECT p.grp AS grp, sum(p.q * m.w) AS total, count(p.q) AS n
        FROM (SELECT grp, item, sum(amt) AS q, count() AS n FROM facts GROUP BY grp, item) AS p JOIN net_rates AS m ON p.item = m.item GROUP BY grp ORDER BY grp)
    UNION ALL SELECT 'filtered', toString(groupArray((grp, total, n))) FROM (SELECT p.grp AS grp, sum(p.q * m.w) AS total, sum(p.n) AS n
        FROM (SELECT grp, item, sum(amt) AS q, count() AS n FROM facts GROUP BY grp, item) AS p JOIN net_rates AS m ON p.item = m.item WHERE p.q > 0 GROUP BY grp ORDER BY grp)
    UNION ALL SELECT 'squared', toString(groupArray((grp, total, n))) FROM (SELECT p.grp AS grp, sum(p.q * p.q * m.w) AS total, sum(p.n) AS n
        FROM (SELECT grp, item, sum(amt) AS q, count() AS n FROM facts GROUP BY grp, item) AS p JOIN net_rates AS m ON p.item = m.item GROUP BY grp ORDER BY grp)
    UNION ALL SELECT 'min', toString(groupArray((grp, total, n))) FROM (SELECT p.grp AS grp, sum(p.lo * m.w) AS total, sum(p.n) AS n
        FROM (SELECT grp, item, min(amt) AS lo, count() AS n FROM facts GROUP BY grp, item) AS p JOIN net_rates AS m ON p.item = m.item GROUP BY grp ORDER BY grp)
    UNION ALL SELECT 'having', toString(groupArray((grp, total, 1))) FROM (SELECT p.grp AS grp, sum(p.q * m.w) AS total
        FROM (SELECT grp, item, sum(amt) AS q, count() AS n FROM facts GROUP BY grp, item) AS p JOIN net_rates AS m ON p.item = m.item GROUP BY grp HAVING sum(p.q) < 2 ORDER BY grp)
    UNION ALL SELECT 'alias', toString(groupArray((grp, bumped, 1))) FROM (SELECT p.grp AS grp, sum(p.q * m.w) + 1 AS bumped
        FROM (SELECT grp, item, sum(amt) AS q, count() AS n FROM facts GROUP BY grp, item) AS p JOIN net_rates AS m ON p.item = m.item GROUP BY grp ORDER BY grp)
    UNION ALL SELECT 'dotted', toString(groupArray((grp, total, n))) FROM (SELECT p.grp AS grp, sum(p.q * m.w) AS total, sum(p.n) AS n
        FROM (SELECT grp, item, sum(amt) AS q, count() AS n FROM facts GROUP BY grp, item) AS p JOIN net_rates AS m ON p.item = m.item GROUP BY grp ORDER BY grp);

INSERT INTO facts VALUES (1, 10, 5), (1, 20, 3), (2, 10, -2);
INSERT INTO rates VALUES (10, 1.5, 1), (20, 2, 1);
SYSTEM REFRESH VIEW linear_mv; SYSTEM WAIT VIEW linear_mv;
SYSTEM REFRESH VIEW counted_mv; SYSTEM WAIT VIEW counted_mv;
SYSTEM REFRESH VIEW filtered_mv; SYSTEM WAIT VIEW filtered_mv;
SYSTEM REFRESH VIEW squared_mv; SYSTEM WAIT VIEW squared_mv;
SYSTEM REFRESH VIEW min_mv; SYSTEM WAIT VIEW min_mv;
SYSTEM REFRESH VIEW having_mv; SYSTEM WAIT VIEW having_mv;
SYSTEM REFRESH VIEW alias_mv; SYSTEM WAIT VIEW alias_mv;
SYSTEM REFRESH VIEW dotted_mv; SYSTEM WAIT VIEW dotted_mv;
SELECT 'r1', * FROM contents ORDER BY ALL;
SELECT 'r1 expected', * FROM expected ORDER BY ALL;

-- New facts of existing groups, which turn the sum of (1, 10) negative, a new group, and a new rate.
INSERT INTO facts VALUES (1, 10, 4), (2, 20, 7), (1, 10, -20);
INSERT INTO rates VALUES (10, 1.5, -1), (10, 2.5, 1);
SYSTEM REFRESH VIEW linear_mv; SYSTEM WAIT VIEW linear_mv;
SYSTEM REFRESH VIEW counted_mv; SYSTEM WAIT VIEW counted_mv;
SYSTEM REFRESH VIEW filtered_mv; SYSTEM WAIT VIEW filtered_mv;
SYSTEM REFRESH VIEW squared_mv; SYSTEM WAIT VIEW squared_mv;
SYSTEM REFRESH VIEW min_mv; SYSTEM WAIT VIEW min_mv;
SYSTEM REFRESH VIEW having_mv; SYSTEM WAIT VIEW having_mv;
SYSTEM REFRESH VIEW alias_mv; SYSTEM WAIT VIEW alias_mv;
SYSTEM REFRESH VIEW dotted_mv; SYSTEM WAIT VIEW dotted_mv;
SELECT 'r2', * FROM contents ORDER BY ALL;
SELECT 'r2 expected', * FROM expected ORDER BY ALL;

-- The mutation deletes a fact below the cursors, so the stages correct their tables.
ALTER TABLE facts DELETE WHERE amt = 4 SETTINGS mutations_sync = 2;
INSERT INTO facts VALUES (3, 10, 1);
SYSTEM REFRESH VIEW linear_mv; SYSTEM WAIT VIEW linear_mv;
SYSTEM REFRESH VIEW counted_mv; SYSTEM WAIT VIEW counted_mv;
SYSTEM REFRESH VIEW filtered_mv; SYSTEM WAIT VIEW filtered_mv;
SYSTEM REFRESH VIEW squared_mv; SYSTEM WAIT VIEW squared_mv;
SYSTEM REFRESH VIEW min_mv; SYSTEM WAIT VIEW min_mv;
SYSTEM REFRESH VIEW having_mv; SYSTEM WAIT VIEW having_mv;
SYSTEM REFRESH VIEW alias_mv; SYSTEM WAIT VIEW alias_mv;
SYSTEM REFRESH VIEW dotted_mv; SYSTEM WAIT VIEW dotted_mv;
SELECT 'corrected', * FROM contents ORDER BY ALL;
SELECT 'corrected expected', * FROM expected ORDER BY ALL;

-- The mutation deletes all facts of account 3.
ALTER TABLE facts DELETE WHERE grp = 3 SETTINGS mutations_sync = 2;
INSERT INTO facts VALUES (1, 20, 1), (1, 10, 2);
SYSTEM REFRESH VIEW linear_mv; SYSTEM WAIT VIEW linear_mv;
SYSTEM REFRESH VIEW counted_mv; SYSTEM WAIT VIEW counted_mv;
SYSTEM REFRESH VIEW filtered_mv; SYSTEM WAIT VIEW filtered_mv;
SYSTEM REFRESH VIEW squared_mv; SYSTEM WAIT VIEW squared_mv;
SYSTEM REFRESH VIEW min_mv; SYSTEM WAIT VIEW min_mv;
SYSTEM REFRESH VIEW having_mv; SYSTEM WAIT VIEW having_mv;
SYSTEM REFRESH VIEW alias_mv; SYSTEM WAIT VIEW alias_mv;
SYSTEM REFRESH VIEW dotted_mv; SYSTEM WAIT VIEW dotted_mv;
SELECT 'r4', * FROM contents ORDER BY ALL;
SELECT 'r4 expected', * FROM expected ORDER BY ALL;

-- Only the stage of linear_mv is linear. Its table sums the partial sums and holds the count as Int64.
SELECT 'stage', replaceRegexpOne(t.engine_full, ' SETTINGS.*', '') AS engine, arraySort(groupArray((c.name, c.type)))
FROM system.tables AS t JOIN system.columns AS c ON c.database = t.database AND c.table = t.name
WHERE t.database = currentDatabase() AND t.name LIKE '.inner_stage.%'
GROUP BY t.name, engine ORDER BY ALL;
