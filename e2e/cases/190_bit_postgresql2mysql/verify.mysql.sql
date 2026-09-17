-- LPAD(BIN(x + 0), width, '0') is the only rendering MySQL offers that shows the
-- value and the declared width at once: BIN() drops the leading zeros a bit(16)
-- column needs, and HEX() of a bit column is the hex of the number, not of the
-- bytes the driver received. The widths are the ones the golden file asserts.
SELECT CONCAT_WS('|',
                 IFNULL(CAST(id AS CHAR), '<null>'),
                 IFNULL(LPAD(BIN(b1 + 0), 1, '0'), '<null>'),
                 IFNULL(LPAD(BIN(b3 + 0), 3, '0'), '<null>'),
                 IFNULL(LPAD(BIN(b8 + 0), 8, '0'), '<null>'),
                 IFNULL(LPAD(BIN(b8_48 + 0), 8, '0'), '<null>'),
                 IFNULL(LPAD(BIN(b16 + 0), 16, '0'), '<null>'),
                 IFNULL(LPAD(BIN(b64 + 0), 64, '0'), '<null>'),
                 IFNULL(LPAD(BIN(b_null + 0), 3, '0'), '<null>'))
FROM e2e_bit_dst
ORDER BY id;
