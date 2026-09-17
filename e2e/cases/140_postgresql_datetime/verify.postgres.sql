-- The tz-aware column is rendered AT TIME ZONE 'UTC': the stored point in time is
-- what has to survive, and normalising to UTC makes the comparison independent of
-- the session's TimeZone (which is what the postgres driver renders timestamptz in).
SELECT concat_ws('|',
                 coalesce(id::text, '<null>'),
                 coalesce(to_char(d, 'YYYY-MM-DD'), '<null>'),
                 coalesce(to_char(t, 'HH24:MI:SS'), '<null>'),
                 coalesce(to_char(dt, 'YYYY-MM-DD HH24:MI:SS'), '<null>'),
                 coalesce(to_char(ts AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS'), '<null>'))
FROM e2e_dst
ORDER BY id;
