-- A shard that gets a serialized plan reads only the granules that the condition of indexHint selects.

DROP TABLE IF EXISTS t_index_hint;
CREATE TABLE t_index_hint (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 10;
INSERT INTO t_index_hint SELECT number FROM numbers(1000);

SELECT count(), sum(k) FROM remote('127.0.0.1', currentDatabase(), t_index_hint) WHERE indexHint(k < 5)
SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0, max_rows_to_read = 10;

SELECT count(), sum(k) FROM remote('127.0.0.1', currentDatabase(), t_index_hint) WHERE indexHint(k < 5)
SETTINGS serialize_query_plan = 0, prefer_localhost_replica = 0, max_rows_to_read = 10;

DROP TABLE t_index_hint;
