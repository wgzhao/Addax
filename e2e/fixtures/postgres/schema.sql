-- PostgreSQL fixture. Mirrors fixtures/mysql/schema.sql column for column; the
-- types are the closest equivalent that still renders identically on output
-- (smallint rather than boolean, because MySQL has no boolean that casts to the
-- same text).
--
-- Idempotent: db_reset runs this before every case.

DROP TABLE IF EXISTS e2e_src;
CREATE TABLE e2e_src
(
    id     integer      NOT NULL PRIMARY KEY,
    name   varchar(64)  NOT NULL,
    qty    integer      NOT NULL,
    price  numeric(10, 2) NOT NULL,
    active smallint     NOT NULL,
    note   varchar(200) NULL
);

DROP TABLE IF EXISTS e2e_dst;
CREATE TABLE e2e_dst
(
    id     integer      NOT NULL PRIMARY KEY,
    name   varchar(64)  NOT NULL,
    qty    integer      NOT NULL,
    price  numeric(10, 2) NOT NULL,
    active smallint     NOT NULL,
    note   varchar(200) NULL
);

DROP TABLE IF EXISTS e2e_upsert;
CREATE TABLE e2e_upsert
(
    id     integer      NOT NULL PRIMARY KEY,
    name   varchar(64)  NOT NULL,
    qty    integer      NOT NULL,
    price  numeric(10, 2) NOT NULL,
    active smallint     NOT NULL,
    note   varchar(200) NULL
);

DROP TABLE IF EXISTS e2e_audit;
CREATE TABLE e2e_audit
(
    phase varchar(20)  NOT NULL,
    note  varchar(200) NULL
);
