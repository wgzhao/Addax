-- Benchmark fixture for the PostgreSQL test server (wgzhao-pc).
--
-- Same data shape as the MariaDB fixture: 2,000,000 deterministic rows, ~5%
-- NULLs in the nullable columns, a 4-byte UTF-8 constant, and binary values.
-- The benchmark owns these two table names, so they are dropped on purpose.

DROP TABLE IF EXISTS perf_source;
CREATE TABLE perf_source
(
    id       BIGINT       NOT NULL PRIMARY KEY,
    c_str1   VARCHAR(32)  NOT NULL,
    c_str2   VARCHAR(32)  NOT NULL,
    c_str3   VARCHAR(32)  NOT NULL,
    c_str4   VARCHAR(64)  NULL,
    c_int    INTEGER      NOT NULL,
    c_long   BIGINT       NOT NULL,
    c_double DOUBLE PRECISION NULL,
    c_date   DATE         NOT NULL,
    c_bool   BOOLEAN      NOT NULL,
    c_ts     TIMESTAMP    NULL,
    c_bytes  BYTEA        NOT NULL
);

INSERT INTO perf_source
SELECT i                                                                          AS id,
       'str1-' || lpad((i % 1000000)::text, 10, '0')                               AS c_str1,
       'addax-perf'                                                               AS c_str2,
       '中文测试'                                                                   AS c_str3,
       CASE WHEN i % 20 = 0 THEN NULL
            ELSE 'str4-' || lpad(((i * 7919) % 1000000)::text, 10, '0') END         AS c_str4,
       (i % 1000000)::integer                                                     AS c_int,
       i                                                                          AS c_long,
       CASE WHEN i % 13 = 0 THEN NULL
            ELSE (i % 1000000)::double precision / 100.0 END                        AS c_double,
       DATE '2020-01-01' + (i % 2000)                                             AS c_date,
       (i % 2 = 1)                                                                AS c_bool,
       CASE WHEN i % 17 = 0 THEN NULL
            ELSE TIMESTAMP '2020-01-01 00:00:00' + ((i % 86400) * interval '1 second') END AS c_ts,
       decode(lpad(to_hex(i % 4294967296), 16, '0'), 'hex')                        AS c_bytes
FROM generate_series(1, 2000000) AS s(i);

DROP TABLE IF EXISTS perf_sink;
CREATE TABLE perf_sink (LIKE perf_source INCLUDING ALL);

ANALYZE perf_source;
