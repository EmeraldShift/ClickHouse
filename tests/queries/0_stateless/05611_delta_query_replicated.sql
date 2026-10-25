-- Tags: zookeeper, atomic-database, no-replicated-database
-- The frontier of a ReplicatedMergeTree table stops below the blocks that the replica has not fetched, so a delta never
-- reads past a gap that a fetch fills later.

CREATE TABLE r1 (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05611_r', 'r1') ORDER BY k
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;
CREATE TABLE r2 (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05611_r', 'r2') ORDER BY k
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;
CREATE TABLE names (k UInt64, name String) ENGINE = MergeTree ORDER BY k SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;
CREATE TABLE joined (k UInt64, name String) ENGINE = MergeTree ORDER BY k;
CREATE MATERIALIZED VIEW joined_view REFRESH EVERY 10 YEAR INCREMENTAL TO joined EMPTY
    AS SELECT r2.k AS k, names.name AS name FROM r2 INNER JOIN names ON r2.k = names.k;

INSERT INTO names VALUES (1, 'one'), (2, 'two'), (3, 'three');
SYSTEM STOP FETCHES r2;
INSERT INTO r1 VALUES (1), (2);
INSERT INTO r2 VALUES (3);

-- The block of r2 follows the unfetched block of r1, so the frontier of r2 is still empty.
SELECT 'fetches stopped', cursor = map() FROM deltaQueryFrontier((SELECT k FROM r2));
SYSTEM REFRESH VIEW joined_view;
SYSTEM WAIT VIEW joined_view;
SELECT 'fetches stopped', * FROM joined ORDER BY k;

SYSTEM START FETCHES r2;
SYSTEM SYNC REPLICA r2;
SELECT 'synced', count() FROM deltaQuery((SELECT k FROM r2), map(), (SELECT cursor FROM deltaQueryFrontier((SELECT k FROM r2))));
SYSTEM REFRESH VIEW joined_view;
SYSTEM WAIT VIEW joined_view;
SELECT 'synced', * FROM joined ORDER BY k;
