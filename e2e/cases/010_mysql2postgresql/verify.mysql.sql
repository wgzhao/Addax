-- Source side: what the reader is expected to have read.
--
-- Every column is null-wrapped and cast to text so each dialect renders the same
-- bytes: a NULL reaching CONCAT_WS is dropped silently, which would shift every
-- later column left and turn a real bug into an unreadable diff.
--
-- The string columns are wrapped in brackets. Without it a value with trailing
-- spaces would end the line in invisible whitespace where any editor could strip
-- it, silently turning the golden file into a lie; and an empty string would be
-- indistinguishable from a dropped column.
SELECT CONCAT_WS('|',
                 IFNULL(CAST(id AS CHAR), '<null>'),
                 IFNULL(CONCAT('[', name, ']'), '<null>'),
                 IFNULL(CAST(qty AS CHAR), '<null>'),
                 IFNULL(CAST(price AS CHAR), '<null>'),
                 IFNULL(CAST(active AS CHAR), '<null>'),
                 IFNULL(CONCAT('[', note, ']'), '<null>'))
FROM e2e_src
ORDER BY id;
