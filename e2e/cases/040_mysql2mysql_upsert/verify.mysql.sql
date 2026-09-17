SELECT CONCAT_WS('|',
                 IFNULL(CAST(id AS CHAR), '<null>'),
                 IFNULL(CONCAT('[', name, ']'), '<null>'),
                 IFNULL(CAST(qty AS CHAR), '<null>'),
                 IFNULL(CAST(price AS CHAR), '<null>'),
                 IFNULL(CAST(active AS CHAR), '<null>'),
                 IFNULL(CONCAT('[', note, ']'), '<null>'))
FROM e2e_upsert
ORDER BY id;
