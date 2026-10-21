-- Tags: atomic-database, distributed
-- Delta queries over Distributed tables run on each shard over its local tables. Both shards of the cluster are this
-- server, so each row of a local table is on both shards, and the results count it twice.

SET enable_lightweight_update = 1;

CREATE TABLE l (k UInt64, v Int64) ENGINE = MergeTree ORDER BY k
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
CREATE TABLE r (k UInt64, w Int64) ENGINE = MergeTree ORDER BY k
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
CREATE TABLE dl AS l ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), l);
CREATE TABLE dr AS r ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), r);

INSERT INTO l VALUES (1, 1), (2, 2);
INSERT INTO l VALUES (3, 3);
INSERT INTO r VALUES (1, 10), (2, 20), (3, 30);

-- The cursor maps the number of each shard to the cursor of the shard.
SELECT arraySort(arrayMap((key, value) -> concat(replaceRegexpOne(key, '[0-9a-f-]{36}', 'uuid'), ' ', toString(value)),
    mapKeys(cursor), mapValues(cursor)))
FROM deltaQueryFrontier((SELECT dl.k, v, w FROM dl JOIN dr ON dl.k = dr.k));

-- The tables can have lightweight updates, so the rows of the deltas have a sign.
SELECT 'join', * FROM deltaQuery((SELECT dl.k AS k, v, w FROM dl JOIN dr ON dl.k = dr.k SETTINGS delta_query_sign_column = 'sign'), map(),
    (SELECT cursor FROM deltaQueryFrontier((SELECT dl.k AS k, v, w FROM dl JOIN dr ON dl.k = dr.k)))) ORDER BY ALL;

-- Each shard reads its own cursor. With shard 1 one block behind, only shard 1 returns that block.
SELECT 'shard 1 behind', * FROM deltaQuery((SELECT k, v FROM dl SETTINGS delta_query_sign_column = 'sign'),
    mapApply((key, value) -> (key, if(startsWith(key, '1.'), value - 1, value)), (SELECT cursor FROM deltaQueryFrontier((SELECT k, v FROM dl)))),
    (SELECT cursor FROM deltaQueryFrontier((SELECT k, v FROM dl)))) ORDER BY ALL;

-- The tables of different clusters can't be co-located.
CREATE TABLE ol AS l ENGINE = Distributed(test_shard_localhost, currentDatabase(), l);
SELECT * FROM deltaQuery((SELECT dl.k FROM dl JOIN ol ON dl.k = ol.k), map(), map()); -- { serverError NOT_IMPLEMENTED }

CREATE MATERIALIZED VIEW joined REFRESH EVERY 10 YEAR APPEND INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT dl.k AS k, sum(v * w) AS s, count() AS n FROM dl JOIN dr ON dl.k = dr.k GROUP BY k SETTINGS delta_query_sign_column = 'sign';
-- HAVING needs a residual.
CREATE MATERIALIZED VIEW tops REFRESH EVERY 10 YEAR APPEND INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT k % 2 AS g, sum(v) AS m FROM dl GROUP BY g HAVING m > 5 SETTINGS delta_query_sign_column = 'sign';
-- A stage over Distributed tables runs on the shards, and its rows go to the stage table on this server.
CREATE MATERIALIZED VIEW nested REFRESH EVERY 10 YEAR APPEND INCREMENTAL ENGINE = SummingMergeTree ORDER BY n EMPTY AS
    SELECT n, count() AS keys FROM (SELECT k, count() AS n FROM dl GROUP BY k) GROUP BY n;

CREATE VIEW contents AS
    SELECT 'joined', toString(groupArray((k, s, n))) FROM (SELECT k, sum(s) AS s, sum(n) AS n FROM joined GROUP BY k HAVING n != 0 ORDER BY k)
    UNION ALL SELECT 'tops', toString(groupArray((g, m, n))) FROM (SELECT g, m, sum(sign) AS n FROM tops GROUP BY g, m HAVING n != 0 ORDER BY ALL)
    UNION ALL SELECT 'nested', toString(groupArray((n, keys))) FROM (SELECT n, sum(keys) AS keys FROM nested GROUP BY n HAVING keys != 0 ORDER BY n);
CREATE VIEW expected AS
    SELECT 'joined', toString(groupArray((k, s, n))) FROM (SELECT dl.k AS k, sum(v * w) AS s, count() AS n FROM dl JOIN dr ON dl.k = dr.k GROUP BY k ORDER BY k)
    UNION ALL SELECT 'tops', toString(groupArray((g, m, 1))) FROM (SELECT k % 2 AS g, sum(v) AS m FROM dl GROUP BY g HAVING m > 5 ORDER BY g)
    UNION ALL SELECT 'nested', toString(groupArray((n, keys))) FROM (SELECT n, count() AS keys FROM (SELECT k, count() AS n FROM dl GROUP BY k) GROUP BY n ORDER BY n);

SYSTEM REFRESH VIEW joined; SYSTEM WAIT VIEW joined;
SYSTEM REFRESH VIEW tops; SYSTEM WAIT VIEW tops;
SYSTEM REFRESH VIEW nested; SYSTEM WAIT VIEW nested;
SELECT 'initial', * FROM contents ORDER BY ALL;
SELECT 'expected', * FROM expected ORDER BY ALL SETTINGS distributed_product_mode = 'local';

INSERT INTO l VALUES (4, 4);
INSERT INTO r VALUES (4, 40);
UPDATE l SET v = 5 WHERE k = 2;
SYSTEM REFRESH VIEW joined; SYSTEM WAIT VIEW joined;
SYSTEM REFRESH VIEW tops; SYSTEM WAIT VIEW tops;
SYSTEM REFRESH VIEW nested; SYSTEM WAIT VIEW nested;
SELECT 'incremental', * FROM contents ORDER BY ALL;
SELECT 'expected', * FROM expected ORDER BY ALL SETTINGS distributed_product_mode = 'local';

-- The mutation writes into the rows below the cursors. The shards find that the deltas lost history, which fails the
-- refresh. The next refresh recomputes each view.
ALTER TABLE l UPDATE v = v * 100 WHERE k = 3 SETTINGS mutations_sync = 2;
INSERT INTO l VALUES (5, 5);
SYSTEM REFRESH VIEW joined; SYSTEM WAIT VIEW joined; -- { serverError REFRESH_FAILED }
SYSTEM REFRESH VIEW tops; SYSTEM WAIT VIEW tops; -- { serverError REFRESH_FAILED }
SYSTEM REFRESH VIEW nested; SYSTEM WAIT VIEW nested; -- { serverError REFRESH_FAILED }
SYSTEM REFRESH VIEW joined; SYSTEM WAIT VIEW joined;
SYSTEM REFRESH VIEW tops; SYSTEM WAIT VIEW tops;
SYSTEM REFRESH VIEW nested; SYSTEM WAIT VIEW nested;
SELECT 'corrected', * FROM contents ORDER BY ALL;
SELECT 'expected', * FROM expected ORDER BY ALL SETTINGS distributed_product_mode = 'local';

UPDATE l SET v = 6 WHERE k = 1;
SYSTEM REFRESH VIEW joined; SYSTEM WAIT VIEW joined;
SYSTEM REFRESH VIEW tops; SYSTEM WAIT VIEW tops;
SYSTEM REFRESH VIEW nested; SYSTEM WAIT VIEW nested;
SELECT 'after', * FROM contents ORDER BY ALL;
SELECT 'expected', * FROM expected ORDER BY ALL SETTINGS distributed_product_mode = 'local';
