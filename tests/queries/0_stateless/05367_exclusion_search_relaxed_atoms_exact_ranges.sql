-- The generic exclusion search collects exact ranges for a condition with relaxed atoms, where the rest of the condition
-- proves ranges exact. The exact count must match the count without the primary key.

DROP TABLE IF EXISTS t_relaxed_atoms_exact_ranges;

CREATE TABLE t_relaxed_atoms_exact_ranges (b UInt32, s String)
ENGINE = MergeTree ORDER BY (b, s)
SETTINGS index_granularity = 8;

INSERT INTO t_relaxed_atoms_exact_ranges SELECT number, toString(number) FROM numbers(30000) SETTINGS max_insert_threads = 1;

SET merge_tree_coarse_index_granularity = 8;
SET use_query_condition_cache = 0;
SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;

-- { echoOn }

-- A set of more than one element is a relaxed atom.
SELECT count() FROM t_relaxed_atoms_exact_ranges WHERE b < 3000 OR b IN (5000, 6000, 7000);
SELECT count() FROM t_relaxed_atoms_exact_ranges WHERE b < 3000 OR b IN (5000, 6000, 7000) SETTINGS use_primary_key = 0;

SELECT count() FROM t_relaxed_atoms_exact_ranges WHERE NOT (b >= 3000 AND b NOT IN (5000, 6000, 7000));
SELECT count() FROM t_relaxed_atoms_exact_ranges WHERE NOT (b >= 3000 AND b NOT IN (5000, 6000, 7000)) SETTINGS use_primary_key = 0;

SELECT count() FROM t_relaxed_atoms_exact_ranges WHERE b IN (5000, 6000, 7000);
SELECT count() FROM t_relaxed_atoms_exact_ranges WHERE b IN (5000, 6000, 7000) SETTINGS use_primary_key = 0;

-- { echoOff }

DROP TABLE t_relaxed_atoms_exact_ranges;
