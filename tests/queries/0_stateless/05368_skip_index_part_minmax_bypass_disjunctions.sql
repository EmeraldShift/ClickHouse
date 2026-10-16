-- Tags: no-parallel-replicas, no-random-settings, no-random-merge-tree-settings
-- no-parallel-replicas: EXPLAIN output differs.
-- no-random-*-settings: the test checks exact granule counts.

-- `b_index` covers only `b`, which the part-level minmax index also covers. On the single part `b` is in [0, 999],
-- so the condition of `b_index`, `(b < 1000 AND unknown) OR b = 2`, can be true on every granule.
-- With `use_skip_indexes_for_disjunctions`, the atom `b = 2` must still be evaluated on each granule.
-- The combined result keeps only the granules where `c = 505` or `b = 2` can be true.
-- With `b = 2000` instead, each atom on `b` is certainly true or certainly false on the part. The combined result
-- keeps only the granule where `c = 505` can be true.

SET explain_query_plan_default = 'legacy';
SET use_query_condition_cache = 0;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    b Int32,
    c Int32,
    INDEX b_index b TYPE minmax,
    INDEX c_index c TYPE minmax
)
ENGINE = MergeTree
ORDER BY tuple()
PARTITION BY intDiv(b, 1000)
SETTINGS index_granularity = 10, index_granularity_bytes = 0, add_minmax_index_for_numeric_columns = 0;

INSERT INTO tab SELECT number, number FROM numbers(1000);

SELECT explain FROM (
    EXPLAIN indexes = 1 SELECT count() FROM tab WHERE (b < 1000 AND c = 505) OR b = 2
    SETTINGS use_skip_indexes_for_disjunctions = 1
) WHERE explain LIKE '%Granules%' OR explain LIKE '%Name%';

SELECT count() FROM tab WHERE (b < 1000 AND c = 505) OR b = 2 SETTINGS use_skip_indexes_for_disjunctions = 1;
SELECT count() FROM tab WHERE (b < 1000 AND c = 505) OR b = 2 SETTINGS use_skip_indexes = 0;

SELECT explain FROM (
    EXPLAIN indexes = 1 SELECT count() FROM tab WHERE (b < 1000 AND c = 505) OR b = 2000
    SETTINGS use_skip_indexes_for_disjunctions = 1
) WHERE explain LIKE '%Granules%' OR explain LIKE '%Name%';

SELECT count() FROM tab WHERE (b < 1000 AND c = 505) OR b = 2000 SETTINGS use_skip_indexes_for_disjunctions = 1;
SELECT count() FROM tab WHERE (b < 1000 AND c = 505) OR b = 2000 SETTINGS use_skip_indexes = 0;

DROP TABLE tab;
