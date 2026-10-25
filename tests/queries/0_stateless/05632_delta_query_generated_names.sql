-- Tags: atomic-database
-- Deltas over columns with the names of the columns that deltas generate.

CREATE TABLE l (k UInt64, `__weight` Int64, `__count` Int64, `__index` Int64) ENGINE = MergeTree ORDER BY k
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;
CREATE TABLE r (k UInt64, `__delta_column_0` Int64) ENGINE = MergeTree ORDER BY k
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;
CREATE TABLE cursors (round UInt8, cursor Map(String, Int64)) ENGINE = MergeTree ORDER BY round;

INSERT INTO l VALUES (1, 10, 20, 30), (2, 11, 21, 31);
INSERT INTO r VALUES (1, 5);
INSERT INTO cursors SELECT 0, cursor FROM deltaQueryFrontier((SELECT 1 FROM l, r));
-- The new row of r retracts the padded row of k = 2 in the LEFT join.
INSERT INTO r VALUES (2, 6);
INSERT INTO cursors SELECT 1, cursor FROM deltaQueryFrontier((SELECT 1 FROM l, r));

SELECT * FROM deltaQuery((SELECT l.k AS k, l.__weight AS __weight, l.__count AS __count, l.__index AS __index,
    r.__delta_column_0 AS __delta_column_0 FROM l LEFT JOIN r ON l.k = r.k),
    (SELECT cursor FROM cursors WHERE round = 0), (SELECT cursor FROM cursors WHERE round = 1))
ORDER BY ALL SETTINGS delta_query_sign_column = 'sign';

-- The union of the terms of an aggregation names its columns apart from the aliases of the query.
SELECT * FROM deltaQuery((SELECT l.k AS __delta_column_1, sum(l.__index) AS s FROM l INNER JOIN r ON l.k = r.k
    GROUP BY __delta_column_1), (SELECT cursor FROM cursors WHERE round = 0), (SELECT cursor FROM cursors WHERE round = 1))
ORDER BY ALL;

