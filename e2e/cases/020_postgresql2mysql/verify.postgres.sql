SELECT concat_ws('|',
                 coalesce(id::text, '<null>'),
                 coalesce('[' || name || ']', '<null>'),
                 coalesce(qty::text, '<null>'),
                 coalesce(price::text, '<null>'),
                 coalesce(active::text, '<null>'),
                 coalesce('[' || note || ']', '<null>'))
FROM e2e_src
ORDER BY id;
