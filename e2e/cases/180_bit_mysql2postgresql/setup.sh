# The shared fixture has no bit column, and bit is the one type family the drivers
# disagree on: Connector/J hands a value over packed, 8 bits per byte, while pgjdbc
# hands over the printable 0/1 string. These tables pin down which form the reader
# produces and what the writer makes of it.
#
# The source carries a value for every failure mode of the old base 2 parsing:
# b8_48 holds 48, whose packed byte 0x30 is the ASCII character '0' that used to be
# read as the value 0, b16 and b64 are wider than one byte, and row 3 is all NULL.
db_exec mysql "
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
    (1, b'1', b'101', b'00000101', b'110000', b'0000010011010010', b'1000000000000000000000000000000000000000000000000000000000000001', NULL),
    (2, b'0', b'000', b'00000000', b'00000000', b'0000000000000000', b'0', NULL),
    (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL);"

db_exec postgres "
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
