-- Tags: atomic-database
-- Validation of incremental refreshable materialized view definitions at CREATE and ALTER:
-- MergeTree source tables with block numbers, joined by inner joins, and no MODIFY QUERY.

DROP TABLE IF EXISTS val_src;
DROP TABLE IF EXISTS val_src_noblock;
DROP TABLE IF EXISTS val_src_repl;
DROP TABLE IF EXISTS val_dim;
DROP TABLE IF EXISTS val_mem;
DROP TABLE IF EXISTS val_tgt;
DROP TABLE IF EXISTS val_mv;
DROP TABLE IF EXISTS val_mv_join;

CREATE TABLE val_src (k UInt64, v UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    add_minmax_index_for_block_number_column = 1,
    add_minmax_index_for_block_offset_column = 1,
    part_minmax_index_columns = 'with_block_number_offset';

CREATE TABLE val_dim (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS enable_block_number_column = 1;
CREATE TABLE val_mem (k UInt64, v UInt64) ENGINE = Memory;
CREATE TABLE val_tgt (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;

-- A single plain streaming source is accepted.
CREATE MATERIALIZED VIEW val_mv
    REFRESH EVERY 10 YEAR APPEND INCREMENTAL
    TO val_tgt EMPTY
    AS SELECT k, v FROM val_src;
SELECT 'valid_created', count() FROM system.tables WHERE database = currentDatabase() AND name = 'val_mv';

-- An inner join of sources with block numbers is accepted.
CREATE MATERIALIZED VIEW val_mv_join
    REFRESH EVERY 10 YEAR APPEND INCREMENTAL
    TO val_tgt EMPTY
    AS SELECT s.k, s.v FROM val_src s JOIN val_dim d ON s.k = d.k;
SELECT 'join_created', count() FROM system.tables WHERE database = currentDatabase() AND name = 'val_mv_join';

-- A LEFT JOIN is rejected: a new row of the right side would change rows the view already appended.
CREATE MATERIALIZED VIEW val_mv_left_join
    REFRESH EVERY 10 YEAR APPEND INCREMENTAL
    TO val_tgt EMPTY
    AS SELECT s.k, s.v FROM val_src s LEFT JOIN val_dim d ON s.k = d.k; -- { serverError NOT_IMPLEMENTED }

-- A subquery is rejected: the cursor covers only the tables of the FROM clause.
CREATE MATERIALIZED VIEW val_mv_subquery
    REFRESH EVERY 10 YEAR APPEND INCREMENTAL
    TO val_tgt EMPTY
    AS SELECT k, v FROM val_src WHERE k IN (SELECT k FROM val_dim); -- { serverError NOT_IMPLEMENTED }

-- A subquery source (not a plain table) is rejected.
CREATE MATERIALIZED VIEW val_mv_subquery_src
    REFRESH EVERY 10 YEAR APPEND INCREMENTAL
    TO val_tgt EMPTY
    AS SELECT k, v FROM (SELECT k, v FROM val_src); -- { serverError NOT_IMPLEMENTED }

-- A source engine other than MergeTree (Memory) is rejected.
CREATE MATERIALIZED VIEW val_mv_mem
    REFRESH EVERY 10 YEAR APPEND INCREMENTAL
    TO val_tgt EMPTY
    AS SELECT k, v FROM val_mem; -- { serverError NOT_IMPLEMENTED }

-- A source that does not persist _block_number is rejected: the cursor would drift across merges.
-- Set the settings explicitly to 0 so the test is deterministic under randomized MergeTree settings.
CREATE TABLE val_src_noblock (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 0;
CREATE MATERIALIZED VIEW val_mv_noblock
    REFRESH EVERY 10 YEAR APPEND INCREMENTAL
    TO val_tgt EMPTY
    AS SELECT k, v FROM val_src_noblock; -- { serverError BAD_ARGUMENTS }

-- A merging engine (ReplacingMergeTree) rewrites historical rows on merge, so it is rejected.
CREATE TABLE val_src_repl (k UInt64, v UInt64) ENGINE = ReplacingMergeTree ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
CREATE MATERIALIZED VIEW val_mv_repl
    REFRESH EVERY 10 YEAR APPEND INCREMENTAL
    TO val_tgt EMPTY
    AS SELECT k, v FROM val_src_repl; -- { serverError NOT_IMPLEMENTED }

-- MODIFY QUERY is not supported for an incremental MV: it would leave the cursor stale.
ALTER TABLE val_mv MODIFY QUERY SELECT k, v FROM val_src WHERE k > 0; -- { serverError NOT_IMPLEMENTED }

-- CREATE OR REPLACE is not supported for an incremental MV: the replacement would start from a fresh cursor.
CREATE OR REPLACE MATERIALIZED VIEW val_mv
    REFRESH EVERY 10 YEAR APPEND INCREMENTAL
    TO val_tgt EMPTY
    AS SELECT k, v FROM val_src; -- { serverError NOT_IMPLEMENTED }

DROP TABLE val_mv;
DROP TABLE val_mv_join;
DROP TABLE val_tgt;
DROP TABLE val_mem;
DROP TABLE val_dim;
DROP TABLE val_src_noblock;
DROP TABLE val_src_repl;
DROP TABLE val_src;
