-- Tags: atomic-database
-- A mutation writes into the rows of the input below the cursors of incremental views. The deltas from those cursors then
-- lose history, and each view inserts the rows that make its target equal to its query instead. The next refresh is
-- incremental again.

SET enable_lightweight_update = 1;

CREATE TABLE t (k UInt64, v Int64) ENGINE = MergeTree ORDER BY k
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE MATERIALIZED VIEW single REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT k, v FROM t SETTINGS delta_query_sign_column = 'sign';
CREATE MATERIALIZED VIEW totals REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT k % 2 AS g, sum(v) AS s, count() AS n FROM t GROUP BY g SETTINGS delta_query_sign_column = 'sign';
-- HAVING needs a residual.
CREATE MATERIALIZED VIEW tops REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT k % 2 AS g, sum(v) AS m FROM t GROUP BY g HAVING m > 5 SETTINGS delta_query_sign_column = 'sign';

CREATE VIEW contents AS
    SELECT 'single', toString(groupArray((k, v, n))) FROM (SELECT k, v, sum(sign) AS n FROM single GROUP BY k, v HAVING n != 0 ORDER BY ALL)
    UNION ALL SELECT 'totals', toString(groupArray((g, s, n))) FROM (SELECT g, sum(s) AS s, sum(n) AS n FROM totals GROUP BY g HAVING n != 0 ORDER BY g)
    UNION ALL SELECT 'tops', toString(groupArray((g, m, n))) FROM (SELECT g, m, sum(sign) AS n FROM tops GROUP BY g, m HAVING n != 0 ORDER BY ALL);

INSERT INTO t VALUES (1, 1), (2, 2), (3, 3);
UPDATE t SET v = 10 WHERE k = 1;
SYSTEM REFRESH VIEW single; SYSTEM WAIT VIEW single;
SYSTEM REFRESH VIEW totals; SYSTEM WAIT VIEW totals;
SYSTEM REFRESH VIEW tops; SYSTEM WAIT VIEW tops;
SELECT 'before', * FROM contents ORDER BY ALL;

ALTER TABLE t UPDATE v = v * 100 WHERE k = 3 SETTINGS mutations_sync = 2;
INSERT INTO t VALUES (4, 4);
SYSTEM REFRESH VIEW single; SYSTEM WAIT VIEW single;
SYSTEM REFRESH VIEW totals; SYSTEM WAIT VIEW totals;
SYSTEM REFRESH VIEW tops; SYSTEM WAIT VIEW tops;
SELECT 'corrected', * FROM contents ORDER BY ALL;

UPDATE t SET v = 5 WHERE k = 2;
SYSTEM REFRESH VIEW single; SYSTEM WAIT VIEW single;
SYSTEM REFRESH VIEW totals; SYSTEM WAIT VIEW totals;
SYSTEM REFRESH VIEW tops; SYSTEM WAIT VIEW tops;
SELECT 'after', * FROM contents ORDER BY ALL;
