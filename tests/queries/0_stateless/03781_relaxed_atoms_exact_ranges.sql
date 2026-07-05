-- Tags: no-replicated-database, no-parallel-replicas, no-random-merge-tree-settings
-- no-replicated-database: EXPLAIN output differs for replicated database.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas.
-- no-random-merge-tree-settings: EXPLAIN output may differ.

DROP TABLE IF EXISTS relaxed_atoms_exact_ranges;

CREATE TABLE relaxed_atoms_exact_ranges
(
    s String
)
ENGINE = MergeTree
ORDER BY s
SETTINGS index_granularity = 1;

INSERT INTO relaxed_atoms_exact_ranges SELECT 'bar' FROM numbers(4096);

-- max_rows_to_read = 1 is load-bearing: this returns 4096, so it can
-- only pass if the count comes from exact ranges rather than row reads.
SELECT count()
FROM relaxed_atoms_exact_ranges
WHERE match(s, '^foo') OR s = 'bar'
SETTINGS
    max_rows_to_read = 1,
    optimize_trivial_count_query = 1,
    optimize_use_implicit_projections = 1,
    optimize_use_projections = 1,
    use_query_condition_cache = 0,
    merge_tree_coarse_index_granularity = 8;

-- max_rows_to_read = 1 is load-bearing: this returns 4096, so it can
-- only pass if exact multi-element IN can prove exact ranges.
SELECT count()
FROM relaxed_atoms_exact_ranges
WHERE s IN ('bar', 'baz')
SETTINGS
    max_rows_to_read = 1,
    optimize_trivial_count_query = 1,
    optimize_use_implicit_projections = 1,
    optimize_use_projections = 1,
    use_query_condition_cache = 0,
    merge_tree_coarse_index_granularity = 8;

DROP TABLE relaxed_atoms_exact_ranges;

DROP TABLE IF EXISTS relaxed_atoms_exact_gap;

CREATE TABLE relaxed_atoms_exact_gap
(
    s String
)
ENGINE = MergeTree
ORDER BY s
SETTINGS index_granularity = 1;

INSERT INTO relaxed_atoms_exact_gap VALUES ('bar'), ('bat'), ('baz');

-- Exact ranges must not be merged across non-matching gaps just because
-- merge_tree_min_rows_for_seek says a seek would be more expensive. With the
-- bug, the exact ranges for bar and baz were coalesced across bat and count()
-- returned 3 from metadata.
SELECT count()
FROM relaxed_atoms_exact_gap
WHERE s IN ('bar', 'baz')
SETTINGS
    optimize_trivial_count_query = 1,
    optimize_use_implicit_projections = 1,
    optimize_use_projections = 1,
    use_query_condition_cache = 0,
    merge_tree_min_rows_for_seek = 1,
    merge_tree_min_bytes_for_seek = 0;

DROP TABLE relaxed_atoms_exact_gap;

DROP TABLE IF EXISTS relaxed_atoms_partition_count;

CREATE TABLE relaxed_atoms_partition_count
(
    d Date,
    x UInt64
)
ENGINE = MergeTree
PARTITION BY d
ORDER BY tuple()
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO relaxed_atoms_partition_count VALUES
    ('2026-01-01', 1),
    ('2026-01-01', 2),
    ('2026-01-02', 3),
    ('2026-01-03', 4),
    ('2026-01-03', 5),
    ('2026-01-03', 6);

-- max_rows_to_read = 1 is load-bearing: this returns 5, so it can only
-- pass if the count comes from partition metadata rather than row reads.
SELECT count()
FROM relaxed_atoms_partition_count
WHERE d IN ('2026-01-01', '2026-01-03')
SETTINGS
    max_rows_to_read = 1,
    optimize_trivial_count_query = 1,
    optimize_use_implicit_projections = 1,
    optimize_use_projections = 1;

DROP TABLE relaxed_atoms_partition_count;
