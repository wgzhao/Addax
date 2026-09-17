-- Every date/time column is rendered explicitly rather than cast to text: the point
-- is to compare the stored value, not whichever format the client happens to pick.
-- DATE_FORMAT/TIME_FORMAT render in the session timezone, which the pinned TZ=UTC
-- and the container's UTC system clock keep identical to the stored instant.
SELECT CONCAT_WS('|',
                 IFNULL(CAST(id AS CHAR), '<null>'),
                 IFNULL(DATE_FORMAT(d, '%Y-%m-%d'), '<null>'),
                 IFNULL(TIME_FORMAT(t, '%H:%i:%s'), '<null>'),
                 IFNULL(DATE_FORMAT(dt, '%Y-%m-%d %H:%i:%s'), '<null>'),
                 IFNULL(DATE_FORMAT(ts, '%Y-%m-%d %H:%i:%s'), '<null>'))
FROM e2e_dst
ORDER BY id;
