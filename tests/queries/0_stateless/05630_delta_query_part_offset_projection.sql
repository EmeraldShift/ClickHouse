-- Tags: atomic-database
-- The delta of a join reads an input sorted by another column through a projection index by the join key. The filter of
-- that read has an `indexHint`, which moves to PREWHERE with the other conjuncts.

SET min_table_rows_to_use_projection_index = 0;

CREATE TABLE big (id UInt64, k UInt64, v UInt64, PROJECTION p (SELECT _part_offset ORDER BY k)) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0, index_granularity = 1024;
CREATE TABLE small (id UInt64, k UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;
INSERT INTO big SELECT number, cityHash64(number) % 100000, number FROM numbers(200000);
INSERT INTO small SELECT number, cityHash64(number) % 100000 FROM numbers(5);

CREATE MATERIALIZED VIEW v REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = SummingMergeTree ORDER BY tuple() EMPTY AS
    SELECT count() AS n, sum(b.v) AS s FROM small AS a INNER JOIN big AS b ON a.k = b.k
    SETTINGS min_table_rows_to_use_projection_index = 0;
SYSTEM REFRESH VIEW v; SYSTEM WAIT VIEW v;

-- New rows of small, then also an unmerged part of big.
INSERT INTO small SELECT 100 + number, cityHash64(100 + number) % 100000 FROM numbers(5);
SYSTEM REFRESH VIEW v; SYSTEM WAIT VIEW v;
INSERT INTO big SELECT number, cityHash64(number) % 100000, number FROM numbers(200000, 100);
INSERT INTO small SELECT 200 + number, cityHash64(200 + number) % 100000 FROM numbers(5);
SYSTEM REFRESH VIEW v; SYSTEM WAIT VIEW v;

SELECT (SELECT (sum(n), sum(s)) FROM v) = (SELECT (count(), sum(b.v)) FROM small AS a INNER JOIN big AS b ON a.k = b.k) AS matches;

SYSTEM FLUSH LOGS query_log;
-- The refreshes after the first read a few granules of big.
SELECT arraySlice(groupArray(read_rows < 50000), 2) FROM (SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase()
    AND type = 'QueryFinish' AND query_kind = 'Insert' AND log_comment LIKE '%.v' ORDER BY event_time_microseconds);
