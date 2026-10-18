-- `deltaQuery` and `deltaQueryFrontier` reject queries whose delta is not the union of per-table terms, and tables whose rows can
-- change without new block numbers.

CREATE TABLE t (k UInt64, v Int64) ENGINE = MergeTree ORDER BY k SETTINGS enable_block_number_column = 1;
CREATE TABLE u (k UInt64, w Int64) ENGINE = MergeTree ORDER BY k SETTINGS enable_block_number_column = 1;
CREATE TABLE no_block_number (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE replacing (k UInt64) ENGINE = ReplacingMergeTree ORDER BY k SETTINGS enable_block_number_column = 1;
CREATE TABLE mutated (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS enable_block_number_column = 1;
CREATE TABLE memory (k UInt64) ENGINE = Memory;
INSERT INTO mutated VALUES (1), (2);
ALTER TABLE mutated DELETE WHERE k = 1 SETTINGS mutations_sync = 2;

SELECT * FROM deltaQueryFrontier((SELECT k FROM t UNION ALL SELECT k FROM u)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT DISTINCT k FROM t)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT k, sum(v) FROM t GROUP BY k HAVING sum(v) > 0)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT k FROM t ORDER BY k)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT k FROM t LIMIT 1)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT k, sum(v) FROM t GROUP BY k WITH TOTALS)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT k, row_number() OVER () FROM t)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT avg(v) FROM t)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT uniq(v) FROM t)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT min(v) FROM t)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT k, maxIf(v, v > 0) FROM t GROUP BY k)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT k, sum(v) + 1 FROM t GROUP BY k)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT k, sum(v) AS s, s * 2 FROM t GROUP BY k)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((WITH sum(v) AS s SELECT k, -s FROM t GROUP BY k)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT k FROM t WHERE k IN (SELECT k FROM u))); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT k FROM (SELECT k FROM t))); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT number FROM numbers(1))); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT t.k FROM t LEFT JOIN u ON t.k = u.k)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT t.k FROM t ANY INNER JOIN u ON t.k = u.k)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT k FROM t FINAL)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT k FROM memory)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT k FROM replacing)); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM deltaQueryFrontier((SELECT k FROM no_block_number)); -- { serverError BAD_ARGUMENTS }
SELECT * FROM deltaQueryFrontier((SELECT k FROM mutated)); -- { serverError NOT_IMPLEMENTED }

-- Accepted result columns.
SELECT * FROM deltaQueryFrontier((SELECT sum(v), count(), sumIf(v, v > 0), countIf(v > 0), avgState(v), uniqState(k) FROM t)) FORMAT Null;
SELECT * FROM deltaQueryFrontier((SELECT k + 1 AS j, sum(v * 2) AS s, s FROM t GROUP BY k)) FORMAT Null;

-- Cursor keys quote table names, so `x.y` in the current database differs from table `y` in database `<current>.x`.
CREATE TABLE `x.y` (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS enable_block_number_column = 1;
SELECT replace(mapKeys(cursor)[1], currentDatabase(), 'db') FROM deltaQueryFrontier((SELECT k FROM `x.y`));

-- Cursor checks.
INSERT INTO t VALUES (1, 10);
SELECT * FROM deltaQuery((SELECT k FROM t), map(currentDatabase() || '.t', map()), map(currentDatabase() || '.t', map('all', 2))); -- { serverError BAD_ARGUMENTS }
SELECT * FROM deltaQuery((SELECT k FROM t), map(currentDatabase() || '.t', map('all', 1)), map(currentDatabase() || '.t', map())); -- { serverError BAD_ARGUMENTS }
SELECT * FROM deltaQuery((SELECT k FROM t), map(currentDatabase() || '.u', map()), map(currentDatabase() || '.t', map())); -- { serverError BAD_ARGUMENTS }
SELECT * FROM deltaQuery((SELECT k FROM t), map(currentDatabase() || '.t', map()), map(currentDatabase() || '.t', map('all', 1)));
