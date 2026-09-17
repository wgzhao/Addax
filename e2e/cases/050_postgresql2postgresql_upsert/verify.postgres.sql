SELECT line
FROM (SELECT concat('dst|', concat_ws('|',
                 coalesce(id::text, '<null>'),
                 coalesce('[' || name || ']', '<null>'),
                 coalesce(qty::text, '<null>'),
                 coalesce(price::text, '<null>'),
                 coalesce(active::text, '<null>'),
                 coalesce('[' || note || ']', '<null>'))) AS line
      FROM e2e_upsert
      UNION ALL
      SELECT concat('audit|', phase, '|', coalesce(note, '<null>')) FROM e2e_audit) t
ORDER BY line;
