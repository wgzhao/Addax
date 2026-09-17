-- Target side: what the writer is expected to have written. Same projection as
-- verify.mysql.sql, so both sides are compared against the same golden file.
SELECT concat_ws('|',
                 coalesce(id::text, '<null>'),
                 coalesce('[' || name || ']', '<null>'),
                 coalesce(qty::text, '<null>'),
                 coalesce(price::text, '<null>'),
                 coalesce(active::text, '<null>'),
                 coalesce('[' || note || ']', '<null>'))
FROM e2e_dst
ORDER BY id;
