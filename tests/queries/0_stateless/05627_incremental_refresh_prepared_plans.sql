-- Tags: atomic-database
-- An incremental refresh reuses the plan of the delta of an earlier refresh with the same set of inputs with new rows. The
-- reused plan reads the bounds of the new refresh, so the targets stay equal to the queries after merges, new partitions
-- and ALTERs of the inputs.

CREATE TABLE t (k UInt64, v Int64) ENGINE = MergeTree PARTITION BY intDiv(k, 100) ORDER BY k
    SETTINGS enable_block_number_column = 1;
CREATE TABLE d (k UInt64, w Int64) ENGINE = MergeTree ORDER BY k
    SETTINGS enable_block_number_column = 1;

CREATE MATERIALIZED VIEW rows REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT t.k AS k, v, w FROM t JOIN d ON t.k = d.k;
CREATE MATERIALIZED VIEW totals REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT t.k % 3 AS g, sum(v * w) AS s, count() AS n FROM t JOIN d ON t.k = d.k GROUP BY g;

CREATE VIEW check AS
    SELECT 'rows' AS view, (SELECT groupArray((k, v, w)) FROM (SELECT * FROM rows ORDER BY ALL))
        = (SELECT groupArray((k, v, w)) FROM (SELECT t.k AS k, v, w FROM t JOIN d ON t.k = d.k ORDER BY ALL)) AS equal
    UNION ALL SELECT 'totals' AS view, (SELECT groupArray((g, s, n)) FROM (SELECT g, sum(s) AS s, sum(n) AS n FROM totals GROUP BY g ORDER BY g))
        = (SELECT groupArray((g, s, n)) FROM (SELECT t.k % 3 AS g, sum(v * w) AS s, count() AS n FROM t JOIN d ON t.k = d.k GROUP BY g ORDER BY g)) AS equal;

INSERT INTO d SELECT number, number FROM numbers(1000);
INSERT INTO t SELECT number, 1 FROM numbers(10);
SYSTEM REFRESH VIEW rows; SYSTEM WAIT VIEW rows;
SYSTEM REFRESH VIEW totals; SYSTEM WAIT VIEW totals;
SELECT 'both inputs', * FROM check ORDER BY ALL;

INSERT INTO t SELECT number, 2 FROM numbers(10, 10);
SYSTEM REFRESH VIEW rows; SYSTEM WAIT VIEW rows;
SYSTEM REFRESH VIEW totals; SYSTEM WAIT VIEW totals;
SELECT 'new rows of t', * FROM check ORDER BY ALL;

INSERT INTO t SELECT number, 3 FROM numbers(20, 10);
SYSTEM REFRESH VIEW rows; SYSTEM WAIT VIEW rows;
SYSTEM REFRESH VIEW totals; SYSTEM WAIT VIEW totals;
SELECT 'more rows of t', * FROM check ORDER BY ALL;

OPTIMIZE TABLE t FINAL;
INSERT INTO t SELECT number, 4 FROM numbers(30, 10);
SYSTEM REFRESH VIEW rows; SYSTEM WAIT VIEW rows;
SYSTEM REFRESH VIEW totals; SYSTEM WAIT VIEW totals;
SELECT 'after a merge', * FROM check ORDER BY ALL;

INSERT INTO t SELECT number, 5 FROM numbers(500, 10);
SYSTEM REFRESH VIEW rows; SYSTEM WAIT VIEW rows;
SYSTEM REFRESH VIEW totals; SYSTEM WAIT VIEW totals;
SELECT 'new partition', * FROM check ORDER BY ALL;

INSERT INTO t SELECT number, 6 FROM numbers(40, 10);
INSERT INTO d SELECT number, 7 FROM numbers(40, 5);
SYSTEM REFRESH VIEW rows; SYSTEM WAIT VIEW rows;
SYSTEM REFRESH VIEW totals; SYSTEM WAIT VIEW totals;
SELECT 'new rows of both', * FROM check ORDER BY ALL;

INSERT INTO t SELECT number, 10 FROM numbers(45, 10);
INSERT INTO d SELECT number, 11 FROM numbers(50, 5);
SYSTEM REFRESH VIEW rows; SYSTEM WAIT VIEW rows;
SYSTEM REFRESH VIEW totals; SYSTEM WAIT VIEW totals;
SELECT 'more rows of both', * FROM check ORDER BY ALL;

ALTER TABLE t ADD COLUMN x UInt8;
INSERT INTO t (k, v) SELECT number, 8 FROM numbers(600, 10);
SYSTEM REFRESH VIEW rows; SYSTEM WAIT VIEW rows;
SYSTEM REFRESH VIEW totals; SYSTEM WAIT VIEW totals;
SELECT 'after an ALTER', * FROM check ORDER BY ALL;

INSERT INTO t (k, v) SELECT number, 9 FROM numbers(610, 10);
SYSTEM REFRESH VIEW rows; SYSTEM WAIT VIEW rows;
SYSTEM REFRESH VIEW totals; SYSTEM WAIT VIEW totals;
SELECT 'after an ALTER again', * FROM check ORDER BY ALL;

-- The analysis folds now64() into a constant, so each refresh of this view analyzes its query again.
CREATE MATERIALIZED VIEW clock REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
    SELECT k, now64(6) AS ts FROM t;
SYSTEM REFRESH VIEW clock; SYSTEM WAIT VIEW clock;
INSERT INTO t (k, v) SELECT number, 12 FROM numbers(620, 10);
SYSTEM REFRESH VIEW clock; SYSTEM WAIT VIEW clock;
INSERT INTO t (k, v) SELECT number, 13 FROM numbers(630, 10);
SYSTEM REFRESH VIEW clock; SYSTEM WAIT VIEW clock;
SELECT 'times of now64()', uniqExact(ts) FROM clock;

-- Which refreshes ran a plan of an earlier one.
SYSTEM FLUSH LOGS query_log;
SELECT replaceOne(log_comment, currentDatabase() || '.', ''), groupArray(ProfileEvents['InsertSelectReusedPlans'])
FROM (SELECT log_comment, ProfileEvents FROM system.query_log
    WHERE event_date >= yesterday() AND type = 'QueryFinish' AND log_comment LIKE 'refresh of ' || currentDatabase() || '.%'
    ORDER BY event_time_microseconds)
GROUP BY log_comment ORDER BY log_comment;
