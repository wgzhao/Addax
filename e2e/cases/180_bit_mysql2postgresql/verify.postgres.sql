-- The destination is what matters: it holds both the value and the width, so the
-- printable form of each column proves the writer reproduced the source exactly.
-- A value that lost its width (bit(3) b'101' widened into bit(8) would read
-- '00000101') or a byte that was misread as a character (0x30 read as '0' would
-- store 0) both show up as a different string here.
SELECT CONCAT_WS('|',
                 CAST(id AS text),
                 COALESCE(b1::text, '<null>'),
                 COALESCE(b3::text, '<null>'),
                 COALESCE(b8::text, '<null>'),
                 COALESCE(b8_48::text, '<null>'),
                 COALESCE(b16::text, '<null>'),
                 COALESCE(b64::text, '<null>'),
                 COALESCE(b_null::text, '<null>'))
FROM e2e_bit_dst
ORDER BY id;
