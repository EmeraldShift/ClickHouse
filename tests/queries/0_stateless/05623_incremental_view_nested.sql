-- Tags: atomic-database
-- Incremental views over subqueries in FROM match their queries. Each subquery is a stage, whose hidden table gets the
-- retraction and the new row of each group that changes. The targets declare counts as UInt64, and the deltas weigh counts
-- as Int64. A query with IN (subquery) runs as a SEMI join. Dropping a view drops its stage tables.

SET enable_lightweight_update = 1;
CREATE TABLE events (user UInt64, x Int64) ENGINE = MergeTree ORDER BY user
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
CREATE TABLE vip (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;

-- The number of users per count of events, and the counts of VIP users.
CREATE TABLE histogram (n UInt64, users Int64) ENGINE = SummingMergeTree ORDER BY n;
CREATE MATERIALIZED VIEW histogram_mv REFRESH EVERY 10 YEAR INCREMENTAL TO histogram EMPTY AS
    SELECT n, count() AS users FROM (SELECT user, count() AS n FROM events GROUP BY user) GROUP BY n
    SETTINGS delta_query_sign_column = 'sign';
CREATE MATERIALIZED VIEW vip_counts_mv REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT p.user AS user, p.n AS n FROM (SELECT user, count() AS n FROM events GROUP BY user) AS p JOIN vip AS v ON p.user = v.id
    SETTINGS delta_query_sign_column = 'sign';

-- The events of VIP users.
CREATE MATERIALIZED VIEW vip_mv REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT e.user AS user, e.x AS x FROM events AS e LEFT SEMI JOIN vip AS v ON e.user = v.id SETTINGS delta_query_sign_column = 'sign';

CREATE VIEW contents AS
    SELECT 'histogram', toString(groupArray((n, users))) FROM (SELECT n, sum(users) AS users FROM histogram GROUP BY n HAVING users != 0 ORDER BY n)
    UNION ALL SELECT 'vip', toString(groupArray((user, x, s))) FROM (SELECT user, x, sum(sign) AS s FROM vip_mv GROUP BY user, x HAVING s != 0 ORDER BY ALL)
    UNION ALL SELECT 'vip_counts', toString(groupArray((user, n, s))) FROM (SELECT user, n, sum(sign) AS s FROM vip_counts_mv GROUP BY user, n HAVING s != 0 ORDER BY ALL);
CREATE VIEW expected AS
    SELECT 'histogram', toString(groupArray((n, users))) FROM (SELECT n, count() AS users FROM (SELECT user, count() AS n FROM events GROUP BY user) GROUP BY n ORDER BY n)
    UNION ALL SELECT 'vip', toString(groupArray((user, x, 1))) FROM (SELECT user, x FROM events WHERE user IN (SELECT id FROM vip) ORDER BY ALL)
    UNION ALL SELECT 'vip_counts', toString(groupArray((user, n, s))) FROM (SELECT p.user AS user, p.n AS n, count() AS s
        FROM (SELECT user, count() AS n FROM events GROUP BY user) AS p JOIN vip AS v ON p.user = v.id GROUP BY ALL ORDER BY ALL);

INSERT INTO events VALUES (1, 1), (1, 2), (2, 3);
INSERT INTO vip VALUES (1), (1);
SYSTEM REFRESH VIEW histogram_mv; SYSTEM WAIT VIEW histogram_mv;
SYSTEM REFRESH VIEW vip_counts_mv; SYSTEM WAIT VIEW vip_counts_mv;
SYSTEM REFRESH VIEW vip_mv; SYSTEM WAIT VIEW vip_mv;
SELECT 'r1', * FROM contents ORDER BY ALL;
SELECT 'r1 expected', * FROM expected ORDER BY ALL;

INSERT INTO events VALUES (2, 4), (3, 5), (1, 6);
INSERT INTO vip VALUES (3);
DELETE FROM events WHERE user = 2 AND x = 3 SETTINGS lightweight_delete_mode = 'lightweight_update';
SYSTEM REFRESH VIEW histogram_mv; SYSTEM WAIT VIEW histogram_mv;
SYSTEM REFRESH VIEW vip_counts_mv; SYSTEM WAIT VIEW vip_counts_mv;
SYSTEM REFRESH VIEW vip_mv; SYSTEM WAIT VIEW vip_mv;
SELECT 'r2', * FROM contents ORDER BY ALL;
SELECT 'r2 expected', * FROM expected ORDER BY ALL;

SELECT 'stage tables', count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_stage.%';
DROP VIEW histogram_mv;
DROP VIEW vip_counts_mv;
SELECT 'hidden tables after drop', count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner%';

-- A view without delta_query_sign_column reads a stage whose rows never retract. The stage keeps duplicate rows.
CREATE TABLE plain (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;
CREATE MATERIALIZED VIEW plain_mv REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT k FROM (SELECT k FROM plain WHERE k > 0);
-- A CTE in FROM is a stage too.
CREATE MATERIALIZED VIEW cte_mv REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = SummingMergeTree ORDER BY n EMPTY AS
    WITH counts AS (SELECT k, count() AS c FROM plain GROUP BY k) SELECT c AS n, count() AS keys FROM counts GROUP BY n;
INSERT INTO plain VALUES (1), (1), (0);
SYSTEM REFRESH VIEW plain_mv; SYSTEM WAIT VIEW plain_mv;
SYSTEM REFRESH VIEW cte_mv; SYSTEM WAIT VIEW cte_mv;
INSERT INTO plain VALUES (1), (2);
SYSTEM REFRESH VIEW plain_mv; SYSTEM WAIT VIEW plain_mv;
SYSTEM REFRESH VIEW cte_mv; SYSTEM WAIT VIEW cte_mv;
SELECT 'plain', groupArray(k) FROM (SELECT k FROM plain_mv ORDER BY k);
SELECT 'cte', groupArray((n, keys)) FROM (SELECT n, sum(keys) AS keys FROM cte_mv GROUP BY n HAVING keys != 0 ORDER BY n);
SELECT 'cte expected', groupArray((n, keys)) FROM (SELECT c AS n, count() AS keys FROM (SELECT k, count() AS c FROM plain GROUP BY k) GROUP BY n ORDER BY n);
