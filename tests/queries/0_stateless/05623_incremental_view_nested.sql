-- Tags: atomic-database
-- A query with IN (subquery) runs as an incremental view over a SEMI join.

CREATE TABLE events (user UInt64, x Int64) ENGINE = MergeTree ORDER BY user SETTINGS enable_block_number_column = 1;
CREATE TABLE vip (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS enable_block_number_column = 1;

-- The events of VIP users.
CREATE MATERIALIZED VIEW vip_mv REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT e.user AS user, e.x AS x FROM events AS e LEFT SEMI JOIN vip AS v ON e.user = v.id SETTINGS delta_query_sign_column = 'sign';

CREATE VIEW contents AS
    SELECT 'vip', toString(groupArray((user, x, s))) FROM (SELECT user, x, sum(sign) AS s FROM vip_mv GROUP BY user, x HAVING s != 0 ORDER BY ALL);
CREATE VIEW expected AS
    SELECT 'vip', toString(groupArray((user, x, 1))) FROM (SELECT user, x FROM events WHERE user IN (SELECT id FROM vip) ORDER BY ALL);

INSERT INTO events VALUES (1, 1), (1, 2), (2, 3);
INSERT INTO vip VALUES (1), (1);
SYSTEM REFRESH VIEW vip_mv; SYSTEM WAIT VIEW vip_mv;
SELECT 'r1', * FROM contents ORDER BY ALL;
SELECT 'r1 expected', * FROM expected ORDER BY ALL;

INSERT INTO events VALUES (2, 4), (3, 5), (1, 6);
INSERT INTO vip VALUES (3);
SYSTEM REFRESH VIEW vip_mv; SYSTEM WAIT VIEW vip_mv;
SELECT 'r2', * FROM contents ORDER BY ALL;
SELECT 'r2 expected', * FROM expected ORDER BY ALL;
