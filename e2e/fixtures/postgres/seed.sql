-- Seed data for the PostgreSQL fixture. Logically identical to
-- fixtures/mysql/seed.sql -- see that file for why each row is here.
--
-- The only intentional difference is the backslash on row 4: MySQL string
-- literals process backslash escapes, PostgreSQL's do not (standard_conforming_
-- strings is on by default), so the two files spell the same value differently.
-- Both end up storing exactly: back\slash

INSERT INTO e2e_src (id, name, qty, price, active, note)
VALUES (1, 'plain', 10, 1.50, 1, 'first'),
       (2, '张三', 20, 22.00, 0, NULL),
       (3, 'a,b"c', 30, 333.33, 1, ''),
       (4, 'back\slash', 40, 4444.44, 0, '   padded   '),
       (5, '', 50, 0.00, 1, 'O''Brien'),
       (6, 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+-', 60, -99.99, 0, NULL);

INSERT INTO e2e_dst (id, name, qty, price, active, note)
VALUES (1, 'stale', 999, 1.00, 0, 'must be gone'),
       (99, 'ghost', 999, 2.00, 0, 'must be gone');

INSERT INTO e2e_upsert (id, name, qty, price, active, note)
VALUES (1, 'alpha-old', 111, 1.00, 1, 'old'),
       (3, 'charlie-old', 333, 3.00, 0, 'old'),
       (7, 'keeper', 777, 7.00, 1, 'must survive');
