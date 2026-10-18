-- Tags: atomic-database
-- Names of the query that shadow table columns must not change what the delta reads.
--
-- In the first queries `a.k` names the subcolumn `k` of column `a`, and `x.k` names the field `k` of the alias `x`. Neither
-- names the column `k` of table `a`. The delta for the insert into `a` matches the query over the rows of `a` and `b`.

CREATE TABLE a (k UInt64, a Tuple(k UInt64)) ENGINE = MergeTree ORDER BY k SETTINGS enable_block_number_column = 1;
CREATE TABLE b (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS enable_block_number_column = 1;
CREATE TABLE cursors (query UInt8, cursor Map(String, Int64)) ENGINE = MergeTree ORDER BY query;

INSERT INTO b VALUES (1), (2);
INSERT INTO cursors SELECT 1, cursor FROM deltaQueryFrontier((SELECT a.k AS ak, b.k AS bk FROM a INNER JOIN b ON a.k = b.k));
INSERT INTO cursors SELECT 2, cursor FROM deltaQueryFrontier((
    WITH CAST(tuple(2), 'Tuple(k UInt64)') AS x SELECT x.k AS xk, b.k AS bk FROM a AS x INNER JOIN b ON x.k = b.k));
INSERT INTO a VALUES (1, tuple(2));

SELECT 'subcolumn', * FROM deltaQuery((SELECT a.k AS ak, b.k AS bk FROM a INNER JOIN b ON a.k = b.k),
    (SELECT cursor FROM cursors WHERE query = 1),
    (SELECT cursor FROM deltaQueryFrontier((SELECT a.k AS ak, b.k AS bk FROM a INNER JOIN b ON a.k = b.k))));

SELECT 'alias', * FROM deltaQuery((
        WITH CAST(tuple(2), 'Tuple(k UInt64)') AS x SELECT x.k AS xk, b.k AS bk FROM a AS x INNER JOIN b ON x.k = b.k),
    (SELECT cursor FROM cursors WHERE query = 2),
    (SELECT cursor FROM deltaQueryFrontier((
        WITH CAST(tuple(2), 'Tuple(k UInt64)') AS x SELECT x.k AS xk, b.k AS bk FROM a AS x INNER JOIN b ON x.k = b.k))));

-- In the last queries the column `c` and the alias `x` have fields named like the virtual columns `_block_number`,
-- `_partition_id` and `_part` that the delta reads from table `c`. The delta for the insert of 1 into `c` joins it with the
-- row 1 of `b`.
CREATE TABLE c (ck UInt64, c Tuple(_block_number UInt64, _partition_id String, _part String) DEFAULT (0, 'all', 'all_0_0_0'))
    ENGINE = MergeTree ORDER BY ck SETTINGS enable_block_number_column = 1;
INSERT INTO c (ck) VALUES (5);
INSERT INTO cursors SELECT 3, cursor FROM deltaQueryFrontier((SELECT ck, b.k AS bk FROM c INNER JOIN b ON ck = b.k));
INSERT INTO cursors SELECT 4, cursor FROM deltaQueryFrontier((
    WITH CAST((0, 'all', 'all_0_0_0'), 'Tuple(_block_number UInt64, _partition_id String, _part String)') AS x
    SELECT ck, b.k AS bk FROM c AS x INNER JOIN b ON ck = b.k));
INSERT INTO c (ck) VALUES (1);

SELECT 'virtual column', * FROM deltaQuery((SELECT ck, b.k AS bk FROM c INNER JOIN b ON ck = b.k),
    (SELECT cursor FROM cursors WHERE query = 3),
    (SELECT cursor FROM deltaQueryFrontier((SELECT ck, b.k AS bk FROM c INNER JOIN b ON ck = b.k))));

SELECT 'virtual alias', * FROM deltaQuery((
        WITH CAST((0, 'all', 'all_0_0_0'), 'Tuple(_block_number UInt64, _partition_id String, _part String)') AS x
        SELECT ck, b.k AS bk FROM c AS x INNER JOIN b ON ck = b.k),
    (SELECT cursor FROM cursors WHERE query = 4),
    (SELECT cursor FROM deltaQueryFrontier((
        WITH CAST((0, 'all', 'all_0_0_0'), 'Tuple(_block_number UInt64, _partition_id String, _part String)') AS x
        SELECT ck, b.k AS bk FROM c AS x INNER JOIN b ON ck = b.k))));
