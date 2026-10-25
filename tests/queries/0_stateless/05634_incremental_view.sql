-- Tags: atomic-database
-- `REFRESH ... INCREMENTAL` view: each refresh appends only the rows committed to the source
-- since the previous refresh. The refresh reads the source between the persisted cursor and the committed frontier, and
-- persists the frontier.

DROP TABLE IF EXISTS incr_src;
DROP TABLE IF EXISTS incr_tgt;
DROP TABLE IF EXISTS incr_mv;

CREATE TABLE incr_src (k UInt64, v UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 0,
    add_minmax_index_for_block_number_column = 1;

CREATE TABLE incr_tgt (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;

-- REFRESH EVERY 10 YEAR + EMPTY: no automatic refresh; every refresh below is triggered manually.
CREATE MATERIALIZED VIEW incr_mv
    REFRESH EVERY 10 YEAR INCREMENTAL
    TO incr_tgt EMPTY
    AS SELECT k, v FROM incr_src;

-- Round 1: 5 rows committed; the first refresh (empty cursor) appends exactly those.
INSERT INTO incr_src SELECT number, number * 10 FROM numbers(5);
SYSTEM REFRESH VIEW incr_mv;
SYSTEM WAIT VIEW incr_mv;
SELECT 'round1', count(), sum(k), sum(v) FROM incr_tgt;

-- Round 2: 5 more rows committed; the refresh resumes from the stored cursor and appends only the new ones.
INSERT INTO incr_src SELECT number, number * 10 FROM numbers(5, 5);
SYSTEM REFRESH VIEW incr_mv;
SYSTEM WAIT VIEW incr_mv;
SELECT 'round2', count(), sum(k), sum(v) FROM incr_tgt;

-- Round 3: nothing new committed; the refresh appends nothing (cursor does not regress, no duplicates).
SYSTEM REFRESH VIEW incr_mv;
SYSTEM WAIT VIEW incr_mv;
SELECT 'round3', count(), sum(k), sum(v) FROM incr_tgt;

DROP TABLE incr_mv;
DROP TABLE incr_tgt;
DROP TABLE incr_src;
