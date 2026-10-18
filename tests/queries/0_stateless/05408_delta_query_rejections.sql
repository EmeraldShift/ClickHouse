-- Tags: atomic-database
-- `deltaQuery` and `deltaQueryFrontier` reject join queries whose delta is not the union of per-table terms, and tables whose
-- rows can change without new block numbers.

CREATE TABLE t (k UInt64, v Int64) ENGINE = MergeTree ORDER BY k SETTINGS enable_block_number_column = 1;
CREATE TABLE u (k UInt64, w Int64) ENGINE = MergeTree ORDER BY k SETTINGS enable_block_number_column = 1;
CREATE TABLE no_block_number (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS enable_block_number_column = 0;
CREATE TABLE replacing (k UInt64) ENGINE = ReplacingMergeTree ORDER BY k SETTINGS enable_block_number_column = 1;
CREATE TABLE mutated (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS enable_block_number_column = 1;
CREATE TABLE memory (k UInt64) ENGINE = Memory;
CREATE TABLE sampled (k UInt64) ENGINE = MergeTree ORDER BY k SAMPLE BY k SETTINGS enable_block_number_column = 1;
CREATE TABLE shadowing (k UInt64, _part String) ENGINE = MergeTree ORDER BY k SETTINGS enable_block_number_column = 1;
INSERT INTO mutated VALUES (1), (2);
ALTER TABLE mutated DELETE WHERE k = 1 SETTINGS mutations_sync = 2;

SELECT * FROM deltaQueryFrontier((SELECT k FROM t UNION ALL SELECT k FROM u)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT DISTINCT t.k FROM t JOIN u ON t.k = u.k)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT t.k, sum(v) FROM t JOIN u ON t.k = u.k GROUP BY t.k HAVING sum(v) > 0)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT t.k FROM t JOIN u ON t.k = u.k ORDER BY t.k)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT t.k FROM t JOIN u ON t.k = u.k LIMIT 1)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT t.k, sum(v) FROM t JOIN u ON t.k = u.k GROUP BY t.k WITH TOTALS)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT t.k, row_number() OVER () FROM t JOIN u ON t.k = u.k)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT avg(v) FROM t JOIN u ON t.k = u.k)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT uniq(v) FROM t JOIN u ON t.k = u.k)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT t.k, maxIf(v, v > 0) FROM t JOIN u ON t.k = u.k GROUP BY t.k)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT t.k, anyLast(v) FROM t JOIN u ON t.k = u.k GROUP BY t.k)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT t.k, sum(v) + 1 FROM t JOIN u ON t.k = u.k GROUP BY t.k)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT t.k, sum(v) AS s, s * 2 FROM t JOIN u ON t.k = u.k GROUP BY t.k)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((WITH sum(v) AS s SELECT t.k, -s FROM t JOIN u ON t.k = u.k GROUP BY t.k)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT k FROM t WHERE k IN (SELECT k FROM u))); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT k FROM (SELECT k FROM t))); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT number FROM numbers(1))); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT t.k FROM t LEFT JOIN u ON t.k = u.k)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT t.k FROM t ANY INNER JOIN u ON t.k = u.k)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT t.k FROM t INNER JOIN u ON t.k = u.k SETTINGS join_default_strictness = 'ANY')); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT k FROM t WHERE (k, v) IN u)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT k FROM t WHERE k = (SELECT max(k) FROM u))); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT k, (SELECT groupArray(w) FROM u) FROM t)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT k FROM sampled SAMPLE 1 / 2)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT k FROM memory)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT k FROM replacing)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT k FROM no_block_number)); -- { serverError BAD_ARGUMENTS }
SELECT * FROM deltaQueryFrontier((SELECT k FROM mutated)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT k FROM shadowing)); -- { serverError NOT_IMPLEMENTED }

-- Accepted result columns of joins.
SELECT * FROM deltaQueryFrontier((SELECT sum(v), count(), sumIf(v, v > 0), countIf(v > 0), avgState(v), uniqState(t.k) FROM t JOIN u ON t.k = u.k)) FORMAT Null;
SELECT * FROM deltaQueryFrontier((SELECT min(v), max(v), any(v), groupBitOr(t.k), groupBitAnd(t.k), groupBitXor(t.k) FROM t JOIN u ON t.k = u.k)) FORMAT Null;
SELECT * FROM deltaQueryFrontier((SELECT t.k + 1 AS j, sum(v * 2) AS s, s FROM t JOIN u ON t.k = u.k GROUP BY t.k)) FORMAT Null;

-- A query over one table runs over the new rows, so any query shape is accepted.
INSERT INTO t VALUES (1, 10), (2, -3);
INSERT INTO t VALUES (2, 5);
SELECT 'one table', * FROM deltaQuery(
    (SELECT k, min(v) AS m, avg(v) AS a FROM t GROUP BY k HAVING count() > 1 ORDER BY k LIMIT 1),
    map(),
    (SELECT cursor FROM deltaQueryFrontier((SELECT k FROM t))));

-- Cursor keys are paths of table UUID, partition ID and `block_number`.
SELECT mapKeys(cursor) = [(SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 't') || '.all.block_number']
FROM deltaQueryFrontier((SELECT k FROM t));

-- Cursor checks.
SELECT * FROM deltaQuery((SELECT k FROM t), map(), map('not-a-table.all.block_number', 1)); -- { serverError BAD_ARGUMENTS }
SELECT * FROM deltaQuery((SELECT k FROM t), map(), map((SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 't') || '.all.block_number', 100)); -- { serverError BAD_ARGUMENTS }
SELECT * FROM deltaQuery((SELECT k FROM t), (SELECT cursor FROM deltaQueryFrontier((SELECT k FROM t))), map()); -- { serverError BAD_ARGUMENTS }
SELECT * FROM deltaQuery((SELECT k FROM t), map(), map((SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 't') || '.all.block_offset', 0)); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM deltaQuery((SELECT k FROM t), map(), (SELECT cursor FROM deltaQueryFrontier((SELECT k FROM t))));

-- Incremental refreshable materialized views over one table stored the `STREAM` cursor of the table, without the table level.
SELECT 'stream cursor', count() FROM deltaQuery((SELECT k FROM t), map('all.block_number', 2, 'all.block_offset', -1),
    (SELECT cursor FROM deltaQueryFrontier((SELECT k FROM t))));

-- A dropped partition has no rows, so its bound in the old cursor does not matter.
CREATE TABLE days (day UInt8, k UInt64) ENGINE = MergeTree PARTITION BY day ORDER BY k SETTINGS enable_block_number_column = 1;
CREATE TABLE cursors (cursor Map(String, Int64)) ENGINE = Memory;
INSERT INTO days VALUES (1, 1), (2, 1);
INSERT INTO cursors SELECT cursor FROM deltaQueryFrontier((SELECT d.k FROM days AS d JOIN t ON d.k = t.k));
ALTER TABLE days DROP PARTITION 1;
INSERT INTO days VALUES (2, 2);
SELECT 'dropped partition', * FROM deltaQuery((SELECT d.day, d.k FROM days AS d JOIN t ON d.k = t.k), (SELECT cursor FROM cursors),
    (SELECT cursor FROM deltaQueryFrontier((SELECT d.k FROM days AS d JOIN t ON d.k = t.k))));
