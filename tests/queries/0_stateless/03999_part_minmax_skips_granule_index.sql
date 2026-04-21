-- Tags: no-parallel-replicas
-- no-parallel-replicas: reads `system.query_log`.

-- `use_skip_indexes_on_data_read` is a randomized setting; when it is on, the
-- mark-filtering loop (where the part-minmax shortcut lives) is bypassed for
-- indexes that support data-read evaluation, and our firing-count assertions
-- would become vacuous. Pin it for the whole test.
SET use_skip_indexes_on_data_read = 0;

DROP TABLE IF EXISTS t_part_minmax_skip;

-- Two minmax indices so that queries with a predicate on both columns populate
-- `useful_indices` with more than one entry, which is what activates the runtime
-- disjunction-merge path.
CREATE TABLE t_part_minmax_skip
(
    t DateTime,
    v UInt32,
    INDEX idx_t t TYPE minmax GRANULARITY 1,
    INDEX idx_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(t)
ORDER BY v
SETTINGS index_granularity = 256;

-- Spread 100k rows over ~69 days (Jan, Feb, Mar 2024) so the query can cover a full
-- interior partition.
INSERT INTO t_part_minmax_skip
SELECT toDateTime('2024-01-01 00:00:00') + number * 60, number
FROM numbers(100000);

-- Baseline: result correctness under the new setting must match the default path.
SELECT 'correctness, setting off', count() FROM t_part_minmax_skip
WHERE t >= '2024-02-01' AND t < '2024-03-01'
SETTINGS use_part_minmax_to_skip_granule_index = 0;

SELECT 'correctness, setting on', count() FROM t_part_minmax_skip
WHERE t >= '2024-02-01' AND t < '2024-03-01'
SETTINGS use_part_minmax_to_skip_granule_index = 1;

-- Positive case: a WHERE that fully contains interior monthly partitions fires the
-- shortcut. Time-only predicate, single useful index.
SELECT count() FROM t_part_minmax_skip
WHERE t >= '2024-01-15' AND t < '2024-03-15'
SETTINGS use_part_minmax_to_skip_granule_index = 1, log_comment = '03999_part_minmax_skip_fires';

-- Cross-index disjunction: `(t range) OR (non-partition column predicate)`. idx_t's
-- per-index RPN is `t_range OR UNKNOWN(v)`; for partitions where `t_range` is proved
-- true, `UNKNOWN`'s `(T, T)` in the OR keeps `cf = cf_t AND T = F`, so the shortcut
-- still fires. This composes with `use_skip_indexes_for_disjunctions`.
SELECT count() FROM t_part_minmax_skip
WHERE (t >= '2024-01-15' AND t < '2024-03-15') OR v < 10
SETTINGS use_part_minmax_to_skip_granule_index = 1,
         use_skip_indexes_for_disjunctions = 1,
         log_comment = '03999_part_minmax_skip_cross_disjunction';

-- Relaxed predicate: `match(...)` declines the shortcut. Relaxed RPN atoms force
-- `can_be_false = true` inside `KeyCondition::checkInHyperrectangle`, so the shortcut
-- correctly refuses.
SELECT count() FROM t_part_minmax_skip
WHERE t >= '2024-01-15' AND t < '2024-03-15' AND match(toString(v), '^\d+$')
SETTINGS use_part_minmax_to_skip_granule_index = 1, log_comment = '03999_part_minmax_skip_relaxed';

-- Mixed-column conjunction: `(t range) AND (v predicate)`. idx_t's per-index RPN is
-- `t_range AND UNKNOWN(v)`; `UNKNOWN`'s `cf = T` poisons the AND to `cf = T`, so the
-- shortcut structurally can never fire here regardless of settings. This is not a
-- shortcoming of the gate — it's the correct behavior: the full predicate is not
-- proved true on the part without knowing `v`.
SELECT count() FROM t_part_minmax_skip
WHERE t >= '2024-01-15' AND t < '2024-03-15' AND v > 0
SETTINGS use_part_minmax_to_skip_granule_index = 1,
         use_skip_indexes_for_disjunctions = 1,
         log_comment = '03999_part_minmax_skip_mixed_and';

-- Correctness under all four combinations of the two settings on the cross-disjunction
-- query. Rows returned must be identical regardless of either setting's value.
SELECT 'cross-setting correctness', length(groupUniqArray(c)) = 1
FROM
(
    SELECT count() AS c FROM t_part_minmax_skip
    WHERE (t >= '2024-01-15' AND t < '2024-03-15') OR v < 10
    SETTINGS use_part_minmax_to_skip_granule_index = 0, use_skip_indexes_for_disjunctions = 0
    UNION ALL
    SELECT count() AS c FROM t_part_minmax_skip
    WHERE (t >= '2024-01-15' AND t < '2024-03-15') OR v < 10
    SETTINGS use_part_minmax_to_skip_granule_index = 0, use_skip_indexes_for_disjunctions = 1
    UNION ALL
    SELECT count() AS c FROM t_part_minmax_skip
    WHERE (t >= '2024-01-15' AND t < '2024-03-15') OR v < 10
    SETTINGS use_part_minmax_to_skip_granule_index = 1, use_skip_indexes_for_disjunctions = 0
    UNION ALL
    SELECT count() AS c FROM t_part_minmax_skip
    WHERE (t >= '2024-01-15' AND t < '2024-03-15') OR v < 10
    SETTINGS use_part_minmax_to_skip_granule_index = 1, use_skip_indexes_for_disjunctions = 1
);

SYSTEM FLUSH LOGS query_log;

-- Positive case fired at least once.
SELECT 'positive fires >= 1',
    sum(ProfileEvents['PartMinMaxSkipsGranuleIndex']) >= 1
FROM system.query_log
WHERE log_comment = '03999_part_minmax_skip_fires' AND type = 'QueryFinish';

-- Cross-index disjunction fired despite the disjunction-merge being active.
SELECT 'cross-disjunction fires >= 1',
    sum(ProfileEvents['PartMinMaxSkipsGranuleIndex']) >= 1
FROM system.query_log
WHERE log_comment = '03999_part_minmax_skip_cross_disjunction' AND type = 'QueryFinish';

-- Relaxed case declined.
SELECT 'relaxed fires = 0',
    sum(ProfileEvents['PartMinMaxSkipsGranuleIndex'])
FROM system.query_log
WHERE log_comment = '03999_part_minmax_skip_relaxed' AND type = 'QueryFinish';

-- Mixed-column AND: shortcut structurally cannot fire.
SELECT 'mixed-and fires = 0',
    sum(ProfileEvents['PartMinMaxSkipsGranuleIndex'])
FROM system.query_log
WHERE log_comment = '03999_part_minmax_skip_mixed_and' AND type = 'QueryFinish';

DROP TABLE t_part_minmax_skip;
