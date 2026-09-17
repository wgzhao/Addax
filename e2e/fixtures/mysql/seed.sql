-- Seed data for the MySQL fixture. Must stay logically identical to
-- fixtures/postgresql/seed.sql -- the cross-database cases compare the two sides
-- against one golden file, so a divergence here shows up as a diff there.
--
-- The values are chosen to break naive implementations. Rows 2-6 carry the
-- character and numeric edges that readers and writers actually get wrong:
--   2  CJK
--   3  the field delimiter and a double quote inside a value
--   4  a backslash (escaped here because MySQL string literals process backslashes;
--      PostgreSQL does not, so the two files differ in syntax but not in content)
--      plus leading and trailing spaces
--   5  an empty string (which Oracle treats as NULL -- see e2e/README.md for the
--      round-two caveat) and a single quote inside a value
--   6  a 64-character string, the column maximum, and a negative decimal

-- The date/time columns carry the boundaries that actually break implementations:
--   1  an ordinary value
--   2  just after the epoch
--   3  NULL everywhere, so a null timestamp is exercised (not the same as a zero one)
--   4  the far end of the MySQL TIMESTAMP range (2038-01-19 03:14:07 UTC)
--   5  a leap day
--   6  a pre-epoch date, which TIMESTAMP cannot hold -- but DATE/DATETIME can
--
-- Values are identical in fixtures/postgres/seed.sql. The postgres side declares the
-- tz-aware column as timestamptz and spells its literals with an explicit offset.
INSERT INTO e2e_src (id, name, qty, price, active, note, d, t, dt, ts)
VALUES (1, 'plain', 10, 1.50, 1, 'first',
        '2024-03-15', '08:30:45', '2024-03-15 08:30:45', '2024-03-15 08:30:45'),
       (2, '张三', 20, 22.00, 0, NULL,
        '1970-01-02', '00:00:01', '1970-01-02 00:00:01', '1970-01-02 00:00:01'),
       (3, 'a,b"c', 30, 333.33, 1, '',
        NULL, NULL, NULL, NULL),
       (4, 'back\\slash', 40, 4444.44, 0, '   padded   ',
        '2038-01-18', '23:59:59', '2038-01-18 23:59:59', '2038-01-18 23:59:59'),
       (5, '', 50, 0.00, 1, 'O''Brien',
        '2024-02-29', '12:00:00', '2024-02-29 12:00:00', '2024-02-29 12:00:00'),
       (6, 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+-', 60, -99.99, 0, NULL,
        '1969-12-31', '23:59:59', '1969-12-31 23:59:59', NULL);

-- Rows that must not survive a writer's preSql. Both are visible in the golden
-- file if the hook does not run.
INSERT INTO e2e_dst (id, name, qty, price, active, note)
VALUES (1, 'stale', 999, 1.00, 0, 'must be gone'),
       (99, 'ghost', 999, 2.00, 0, 'must be gone');

-- id=1 and id=3 exist with different values, so an upsert has to update them in
-- place; id=7 is not in the source at all, so a delete-then-insert implementation
-- would lose it. All three outcomes come out of one assertion.
INSERT INTO e2e_upsert (id, name, qty, price, active, note)
VALUES (1, 'alpha-old', 111, 1.00, 1, 'old'),
       (3, 'charlie-old', 333, 3.00, 0, 'old'),
       (7, 'keeper', 777, 7.00, 1, 'must survive');
