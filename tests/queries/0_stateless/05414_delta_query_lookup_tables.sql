-- Tags: atomic-database
-- With delta_query_lookup_tables, new rows of a lookup table join only with rows of the other inputs that arrive after
-- them. A fact whose dimension row arrives later joins in the view without the setting, and not in the view with it.

CREATE TABLE facts (id UInt64, dim_id UInt64, v Int64) ENGINE = MergeTree ORDER BY id SETTINGS enable_block_number_column = 1;
CREATE TABLE dims (id UInt64, name String) ENGINE = MergeTree ORDER BY id SETTINGS enable_block_number_column = 1;
CREATE TABLE joined (name String, v Int64) ENGINE = SummingMergeTree ORDER BY name;
CREATE TABLE looked_up (name String, v Int64) ENGINE = SummingMergeTree ORDER BY name;

CREATE MATERIALIZED VIEW joined_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO joined EMPTY AS
    SELECT d.name AS name, sum(f.v) AS v FROM facts AS f INNER JOIN dims AS d ON d.id = f.dim_id GROUP BY name;
CREATE MATERIALIZED VIEW looked_up_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO looked_up EMPTY AS
    SELECT d.name AS name, sum(f.v) AS v FROM facts AS f INNER JOIN dims AS d ON d.id = f.dim_id GROUP BY name
    SETTINGS delta_query_lookup_tables = 'dims';

-- A fact and its dimension in the same round join in both views. Fact 3 has no dimension yet.
INSERT INTO dims VALUES (1, 'a'), (2, 'b');
INSERT INTO facts VALUES (1, 1, 10), (2, 2, 5), (3, 3, 7);
SYSTEM REFRESH VIEW joined_mv; SYSTEM WAIT VIEW joined_mv;
SYSTEM REFRESH VIEW looked_up_mv; SYSTEM WAIT VIEW looked_up_mv;
SELECT 'round 1', 'joined', name, sum(v) FROM joined GROUP BY name ORDER BY name;
SELECT 'round 1', 'looked_up', name, sum(v) FROM looked_up GROUP BY name ORDER BY name;

-- The dimension of fact 3 arrives late, and a new fact joins an old dimension.
INSERT INTO dims VALUES (3, 'c');
INSERT INTO facts VALUES (4, 1, 1);
SYSTEM REFRESH VIEW joined_mv; SYSTEM WAIT VIEW joined_mv;
SYSTEM REFRESH VIEW looked_up_mv; SYSTEM WAIT VIEW looked_up_mv;
SELECT 'round 2', 'joined', name, sum(v) FROM joined GROUP BY name ORDER BY name;
SELECT 'round 2', 'looked_up', name, sum(v) FROM looked_up GROUP BY name ORDER BY name;

-- A new fact joins the late dimension in both views.
INSERT INTO facts VALUES (5, 3, 100);
SYSTEM REFRESH VIEW joined_mv; SYSTEM WAIT VIEW joined_mv;
SYSTEM REFRESH VIEW looked_up_mv; SYSTEM WAIT VIEW looked_up_mv;
SELECT 'round 3', 'joined', name, sum(v) FROM joined GROUP BY name ORDER BY name;
SELECT 'round 3', 'looked_up', name, sum(v) FROM looked_up GROUP BY name ORDER BY name;

SELECT * FROM deltaQuery((SELECT f.v FROM facts AS f INNER JOIN dims AS d ON d.id = f.dim_id SETTINGS delta_query_lookup_tables = 'other'), map(), map()); -- { serverError BAD_ARGUMENTS }
SELECT * FROM deltaQuery((SELECT v FROM facts SETTINGS delta_query_lookup_tables = 'facts'), map(), map()); -- { serverError BAD_ARGUMENTS }
