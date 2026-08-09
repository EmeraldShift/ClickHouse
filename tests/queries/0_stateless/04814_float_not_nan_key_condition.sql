SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS float_not_nan_oracle;
DROP TABLE IF EXISTS float_not_nan_mt;
DROP TABLE IF EXISTS float32_not_nan_mt;
DROP TABLE IF EXISTS lc_float_not_nan_mt;
DROP TABLE IF EXISTS integer_not_control;

CREATE TABLE float_not_nan_oracle (f Float64) ENGINE = Memory;

CREATE TABLE float_not_nan_mt
(
    f Float64,
    INDEX idx_f f TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 2;

CREATE TABLE float32_not_nan_mt
(
    f Float32,
    INDEX idx_f f TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 2;

CREATE TABLE lc_float_not_nan_mt
(
    f LowCardinality(Float64),
    INDEX idx_f f TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 2;

CREATE TABLE integer_not_control
(
    x UInt64,
    INDEX idx_x x TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO float_not_nan_oracle VALUES (1), (nan);
INSERT INTO float_not_nan_mt VALUES (1), (nan);
INSERT INTO float32_not_nan_mt VALUES (1), (nan);
INSERT INTO lc_float_not_nan_mt VALUES (1), (nan);
INSERT INTO integer_not_control SELECT number + 1 FROM numbers(100);

SELECT 'Float64 NOT greater',
    (SELECT count() FROM float_not_nan_mt WHERE NOT (f > 0)
        SETTINGS force_data_skipping_indices = 'idx_f'),
    (SELECT count() FROM float_not_nan_oracle WHERE NOT (f > 0));

SELECT 'Float64 NOT greaterOrEquals',
    (SELECT count() FROM float_not_nan_mt WHERE NOT (f >= 0)
        SETTINGS force_data_skipping_indices = 'idx_f'),
    (SELECT count() FROM float_not_nan_oracle WHERE NOT (f >= 0));

SELECT 'Float64 NOT less',
    (SELECT count() FROM float_not_nan_mt WHERE NOT (f < 2)
        SETTINGS force_data_skipping_indices = 'idx_f'),
    (SELECT count() FROM float_not_nan_oracle WHERE NOT (f < 2));

SELECT 'Float64 NOT lessOrEquals',
    (SELECT count() FROM float_not_nan_mt WHERE NOT (f <= 2)
        SETTINGS force_data_skipping_indices = 'idx_f'),
    (SELECT count() FROM float_not_nan_oracle WHERE NOT (f <= 2));

SELECT 'Float32 NOT greater', count()
FROM float32_not_nan_mt
WHERE NOT (f > 0)
SETTINGS force_data_skipping_indices = 'idx_f';

SELECT 'LowCardinality Float64 NOT greater', count()
FROM lc_float_not_nan_mt
WHERE NOT (f > 0)
SETTINGS force_data_skipping_indices = 'idx_f';

SELECT 'integer inversion still prunes', count()
FROM integer_not_control
WHERE NOT (x > 0)
SETTINGS force_data_skipping_indices = 'idx_x', max_rows_to_read = 1;

DROP TABLE float_not_nan_oracle;
DROP TABLE float_not_nan_mt;
DROP TABLE float32_not_nan_mt;
DROP TABLE lc_float_not_nan_mt;
DROP TABLE integer_not_control;
