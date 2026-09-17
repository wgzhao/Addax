-- MySQL fixture, shared by every MySQL-backed case.
--
-- Idempotent on purpose: db_reset runs this before every case, so a fixture edit
-- takes effect on the next run and a dirty container from a previous run does not
-- need a manual teardown.
--
-- Each table exists to prove something specific, not to be realistic:
--   e2e_src     source data; type and character-boundary coverage
--   e2e_dst     writer target, pre-seeded with stale rows -- id=1 collides with a
--               source primary key and id=99 does not, so a preSql that silently
--               does nothing is caught rather than passing
--   e2e_upsert  upsert target, pre-seeded so one assertion covers all three
--               outcomes: updated in place, inserted, and left alone
--   e2e_audit   written by postSql and read by the verifier; without it postSql
--               would be an unverified config line nobody ever notices breaking

DROP TABLE IF EXISTS e2e_src;
CREATE TABLE e2e_src
(
    id     INT           NOT NULL PRIMARY KEY,
    name   VARCHAR(64)   NOT NULL,
    qty    INT           NOT NULL,
    price  DECIMAL(10, 2) NOT NULL,
    active TINYINT       NOT NULL,
    note   VARCHAR(200)  NULL,
    d      DATE          NULL,
    t      TIME          NULL,
    dt     DATETIME      NULL,
    ts     TIMESTAMP     NULL
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;

DROP TABLE IF EXISTS e2e_dst;
CREATE TABLE e2e_dst
(
    id     INT           NOT NULL PRIMARY KEY,
    name   VARCHAR(64)   NOT NULL,
    qty    INT           NOT NULL,
    price  DECIMAL(10, 2) NOT NULL,
    active TINYINT       NOT NULL,
    note   VARCHAR(200)  NULL,
    d      DATE          NULL,
    t      TIME          NULL,
    dt     DATETIME      NULL,
    ts     TIMESTAMP     NULL
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;

DROP TABLE IF EXISTS e2e_upsert;
CREATE TABLE e2e_upsert
(
    id     INT           NOT NULL PRIMARY KEY,
    name   VARCHAR(64)   NOT NULL,
    qty    INT           NOT NULL,
    price  DECIMAL(10, 2) NOT NULL,
    active TINYINT       NOT NULL,
    note   VARCHAR(200)  NULL,
    d      DATE          NULL,
    t      TIME          NULL,
    dt     DATETIME      NULL,
    ts     TIMESTAMP     NULL
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;

DROP TABLE IF EXISTS e2e_audit;
CREATE TABLE e2e_audit
(
    phase VARCHAR(20)  NOT NULL,
    note  VARCHAR(200) NULL
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;
