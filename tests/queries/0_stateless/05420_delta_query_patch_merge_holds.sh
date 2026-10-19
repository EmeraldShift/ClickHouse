#!/usr/bin/env bash
# Tags: atomic-database
# An incremental view keeps merges from merging the patch parts of its input across its cursor. The merged patch would
# keep only the latest update of a row, so the next refresh could not read the row as of the cursor.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t (k UInt64, v Int64) ENGINE = MergeTree ORDER BY k
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
        -- Only the explicit OPTIMIZE below may merge the patch parts.
        max_bytes_to_merge_at_max_space_in_pool = 1;
    CREATE MATERIALIZED VIEW single REFRESH EVERY 10 YEAR APPEND INCREMENTAL ENGINE = MergeTree ORDER BY tuple() EMPTY AS
        SELECT k, v FROM t SETTINGS delta_query_sign_column = 'sign';

    SET enable_lightweight_update = 1;
    INSERT INTO t VALUES (1, 1), (2, 2);
    UPDATE t SET v = 10 WHERE k = 1;
    SYSTEM REFRESH VIEW single; SYSTEM WAIT VIEW single;
    UPDATE t SET v = 20 WHERE k = 1;
    UPDATE t SET v = 30 WHERE k = 2;
"

# OPTIMIZE FINAL skips the patch partitions when no partition is given, so name the partition. It is hashed from the
# structure of the patch, hence read it instead of spelling it out.
patch_partition_id=$($CLICKHOUSE_CLIENT --query "
    SELECT DISTINCT partition_id FROM system.parts
    WHERE database = currentDatabase() AND table = 't' AND active AND startsWith(partition_id, 'patch')")

# A merge of all the patch parts would span the cursor of the view, so OPTIMIZE FINAL leaves them.
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE t PARTITION ID '$patch_partition_id' FINAL SETTINGS optimize_throw_if_noop = 0"

$CLICKHOUSE_CLIENT --query "
    SELECT 'patch parts', count() FROM system.parts
    WHERE database = currentDatabase() AND table = 't' AND active AND startsWith(name, 'patch');
    SYSTEM REFRESH VIEW single; SYSTEM WAIT VIEW single;
    SELECT k, v, sum(sign) AS n FROM single GROUP BY k, v HAVING n != 0 ORDER BY k;
"
