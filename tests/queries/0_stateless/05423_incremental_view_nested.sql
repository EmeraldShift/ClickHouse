-- Tags: atomic-database
-- Queries that nest aggregates or read IN (subquery) run as chains of incremental views. The inner view of a nested
-- aggregate has HAVING, so its target gets the retraction and the new row of each group that changes. The outer view reads
-- that target as a signed input. The targets declare counts as UInt64, and the deltas weigh counts as Int64. A query with
-- IN (subquery) runs as a SEMI join.

SET enable_lightweight_update = 1;
CREATE TABLE events (user UInt64, x Int64) ENGINE = MergeTree ORDER BY user
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
CREATE TABLE vip (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS enable_block_number_column = 1;

-- The events per user, then the number of users per count.
CREATE TABLE per_user (user UInt64, n UInt64, sign Int8) ENGINE = MergeTree ORDER BY user SETTINGS enable_block_number_column = 1;
CREATE MATERIALIZED VIEW per_user_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO per_user EMPTY AS
    SELECT user, count() AS n FROM events GROUP BY user HAVING n > 0 SETTINGS delta_query_sign_column = 'sign';
CREATE TABLE histogram (n UInt64, users Int64) ENGINE = SummingMergeTree ORDER BY n;
CREATE MATERIALIZED VIEW histogram_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO histogram EMPTY AS
    SELECT n, count() AS users FROM per_user GROUP BY n SETTINGS delta_query_sign_column = 'sign';

-- The events of VIP users.
CREATE MATERIALIZED VIEW vip_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT e.user AS user, e.x AS x FROM events AS e LEFT SEMI JOIN vip AS v ON e.user = v.id SETTINGS delta_query_sign_column = 'sign';

CREATE VIEW contents AS
    SELECT 'histogram', toString(groupArray((n, users))) FROM (SELECT n, sum(users) AS users FROM histogram GROUP BY n HAVING users != 0 ORDER BY n)
    UNION ALL SELECT 'vip', toString(groupArray((user, x, s))) FROM (SELECT user, x, sum(sign) AS s FROM vip_mv GROUP BY user, x HAVING s != 0 ORDER BY ALL);
CREATE VIEW expected AS
    SELECT 'histogram', toString(groupArray((n, users))) FROM (SELECT n, count() AS users FROM (SELECT user, count() AS n FROM events GROUP BY user) GROUP BY n ORDER BY n)
    UNION ALL SELECT 'vip', toString(groupArray((user, x, 1))) FROM (SELECT user, x FROM events WHERE user IN (SELECT id FROM vip) ORDER BY ALL);

INSERT INTO events VALUES (1, 1), (1, 2), (2, 3);
INSERT INTO vip VALUES (1), (1);
SYSTEM REFRESH VIEW per_user_mv; SYSTEM WAIT VIEW per_user_mv;
SYSTEM REFRESH VIEW histogram_mv; SYSTEM WAIT VIEW histogram_mv;
SYSTEM REFRESH VIEW vip_mv; SYSTEM WAIT VIEW vip_mv;
SELECT 'r1', * FROM contents ORDER BY ALL;
SELECT 'r1 expected', * FROM expected ORDER BY ALL;

INSERT INTO events VALUES (2, 4), (3, 5), (1, 6);
INSERT INTO vip VALUES (3);
DELETE FROM events WHERE user = 2 AND x = 3 SETTINGS lightweight_delete_mode = 'lightweight_update';
SYSTEM REFRESH VIEW per_user_mv; SYSTEM WAIT VIEW per_user_mv;
SYSTEM REFRESH VIEW histogram_mv; SYSTEM WAIT VIEW histogram_mv;
SYSTEM REFRESH VIEW vip_mv; SYSTEM WAIT VIEW vip_mv;
SELECT 'r2', * FROM contents ORDER BY ALL;
SELECT 'r2 expected', * FROM expected ORDER BY ALL;
