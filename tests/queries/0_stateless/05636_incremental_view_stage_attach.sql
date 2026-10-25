-- Tags: atomic-database
-- The cursor of an incremental view covers its stages too, and it survives DETACH and ATTACH. The refresh after ATTACH reads
-- only the rows inserted since the last refresh, both in the stage and in the outer query.

CREATE TABLE fills (id UInt64, acct UInt16, sym UInt32, qty Int64) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;
CREATE TABLE marks (sym UInt32, px Float64, sign Int8) ENGINE = MergeTree ORDER BY sym
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;
INSERT INTO marks SELECT number, number + 1, 1 FROM numbers(5);
INSERT INTO fills SELECT number, number % 3, number % 5, number FROM numbers(20);

CREATE MATERIALIZED VIEW mv REFRESH EVERY 10 YEAR INCREMENTAL ENGINE = MergeTree ORDER BY acct EMPTY AS
    SELECT p.acct AS acct, sum(p.q * m.px) AS risk, sum(p.n) AS n
    FROM (SELECT acct, sym, sum(qty) AS q, count() AS n FROM fills GROUP BY acct, sym) AS p
    INNER JOIN marks AS m ON p.sym = m.sym
    GROUP BY acct
    SETTINGS delta_query_sign_column = 'sign';

SYSTEM REFRESH VIEW mv;
SYSTEM WAIT VIEW mv;

INSERT INTO fills SELECT number, number % 3, number % 5, number FROM numbers(20, 7);
DETACH TABLE mv;
ATTACH TABLE mv;
SYSTEM REFRESH VIEW mv;
SYSTEM WAIT VIEW mv;

SELECT 'mv', acct, sum(risk), sum(n) FROM mv GROUP BY acct ORDER BY acct;
SELECT 'expected', p.acct AS acct, sum(p.q * m.px), sum(p.n)
    FROM (SELECT acct, sym, sum(qty) AS q, count() AS n FROM fills GROUP BY acct, sym) AS p
    INNER JOIN marks AS m ON p.sym = m.sym
    GROUP BY acct
    ORDER BY acct;

DROP TABLE mv;
