-- Benchmark fixture for the MariaDB (wgzhao-pc) test server.
--
-- These two table names are owned by the benchmark; dropping them is deliberate
-- so a schema change cannot silently mix old and new rows.
--
-- 2,000,000 deterministic rows. seq_1_to_2000000 is MariaDB's sequence engine
-- table; on a server without it, replace the FROM clause with a digits cross
-- join or generate the rows through streamreader -> mysqlwriter.

DROP TABLE IF EXISTS perf_source;
CREATE TABLE perf_source
(
    id       BIGINT       NOT NULL,
    c_str1   VARCHAR(32)  NOT NULL,
    c_str2   VARCHAR(32)  NOT NULL,
    c_str3   VARCHAR(32)  NOT NULL,
    c_str4   VARCHAR(64)  NULL,
    c_int    INT          NOT NULL,
    c_long   BIGINT       NOT NULL,
    c_double DOUBLE       NULL,
    c_date   DATE         NOT NULL,
    c_bool   TINYINT(1)   NOT NULL,
    c_ts     TIMESTAMP    NULL,
    c_bytes  VARBINARY(16) NOT NULL,
    PRIMARY KEY (id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;

INSERT INTO perf_source
SELECT seq                                                                          AS id,
       CONCAT('str1-', LPAD(seq % 1000000, 10, '0'))                                 AS c_str1,
       'addax-perf'                                                                 AS c_str2,
       '中文测试'                                                                     AS c_str3,
       CASE WHEN seq % 20 = 0 THEN NULL
            ELSE CONCAT('str4-', LPAD((seq * 7919) % 1000000, 10, '0')) END          AS c_str4,
       seq % 1000000                                                                AS c_int,
       seq                                                                          AS c_long,
       CASE WHEN seq % 13 = 0 THEN NULL ELSE (seq % 1000000) / 100.0 END             AS c_double,
       DATE_ADD('2020-01-01', INTERVAL (seq % 2000) DAY)                             AS c_date,
       seq % 2                                                                      AS c_bool,
       CASE WHEN seq % 17 = 0 THEN NULL
            ELSE TIMESTAMPADD(SECOND, seq % 86400, TIMESTAMP('2020-01-01 00:00:00')) END AS c_ts,
       UNHEX(LPAD(HEX(seq % 4294967296), 16, '0'))                                   AS c_bytes
FROM seq_1_to_2000000;

DROP TABLE IF EXISTS perf_sink;
CREATE TABLE perf_sink LIKE perf_source;

ANALYZE TABLE perf_source;
