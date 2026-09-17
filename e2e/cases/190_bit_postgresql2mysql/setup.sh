# The mirror image of 180: here pgjdbc reads the source, so the reader has to turn
# the printable form a bit column comes back in into whatever the MySQL writer
# binds. Same values, same widths, opposite direction.
db_exec postgres "
DROP TABLE IF EXISTS e2e_bit_src;
CREATE TABLE e2e_bit_src
(
    id     int NOT NULL PRIMARY KEY,
    b1     bit(1),
    b3     bit(3),
    b8     bit(8),
    b8_48  bit(8),
    b16    bit(16),
    b64    bit(64),
    b_null bit(3)
);
INSERT INTO e2e_bit_src VALUES
    (1, B'1', B'101', B'00000101', B'00110000', B'0000010011010010', B'1000000000000000000000000000000000000000000000000000000000000001', NULL),
    (2, B'0', B'000', B'00000000', B'00000000', B'0000000000000000', B'0000000000000000000000000000000000000000000000000000000000000000', NULL),
    (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL);"

db_exec mysql "
DROP TABLE IF EXISTS e2e_bit_dst;
CREATE TABLE e2e_bit_dst
(
    id     int NOT NULL PRIMARY KEY,
    b1     bit(1),
    b3     bit(3),
    b8     bit(8),
    b8_48  bit(8),
    b16    bit(16),
    b64    bit(64),
    b_null bit(3)
);"
