SRC="$E2E_CASE_WORK/src.db"
DST="$E2E_CASE_WORK/dst.db"
src_canon="$E2E_CASE_WORK/actual/src.txt"
dst_canon="$E2E_CASE_WORK/actual/dst.txt"

# Compare the whole table, projecting numbers through a fixed format so a storage
# class change alone is not a difference, but a truncated fraction is. The source
# side canonicalises its INTEGER epoch DATETIME to the TEXT form the writer must
# produce; the target side must already be TEXT.
sqlite3 -noheader -separator '|' "$SRC" <<'SQL' >"$src_canon"
SELECT
  id,
  COALESCE(CAST(c_int AS TEXT), '<null>'),
  COALESCE(CAST(c_big AS TEXT), '<null>'),
  COALESCE(printf('%.3f', c_real), '<null>'),
  CASE WHEN c_text IS NULL THEN '<null>' ELSE '[' || c_text || ']' END,
  COALESCE(hex(c_blob), '<null>'),
  COALESCE(CAST(c_bool AS TEXT), '<null>'),
  COALESCE(CAST(c_date AS TEXT), '<null>'),
  -- normalize the legacy seconds-only form to the millisecond form the writer emits
  CASE WHEN c_datetime IS NULL THEN '<null>'
       ELSE strftime('%Y-%m-%d %H:%M:%f', c_datetime) END,
  CASE WHEN c_epoch IS NULL THEN '<null>'
       ELSE strftime('%Y-%m-%d %H:%M:%S', c_epoch / 1000, 'unixepoch')
            || '.' || printf('%03d', c_epoch % 1000) END,
  CASE WHEN c_numeric IS NULL THEN '<null>' ELSE printf('%.3f', c_numeric) END
FROM src
ORDER BY id;
SQL

sqlite3 -noheader -separator '|' "$DST" <<'SQL' >"$dst_canon"
SELECT
  id,
  COALESCE(CAST(c_int AS TEXT), '<null>'),
  COALESCE(CAST(c_big AS TEXT), '<null>'),
  COALESCE(printf('%.3f', c_real), '<null>'),
  CASE WHEN c_text IS NULL THEN '<null>' ELSE '[' || c_text || ']' END,
  COALESCE(hex(c_blob), '<null>'),
  COALESCE(CAST(c_bool AS TEXT), '<null>'),
  COALESCE(CAST(c_date AS TEXT), '<null>'),
  COALESCE(CAST(c_datetime AS TEXT), '<null>'),
  COALESCE(c_epoch, '<null>'),
  CASE WHEN c_numeric IS NULL THEN '<null>' ELSE printf('%.3f', c_numeric) END
FROM dst
ORDER BY id;
SQL

assert_files_equal "$src_canon" "$dst_canon" "sqlite round trip"

# The canonical comparison above would also pass if every DATETIME was stored as
# INTEGER epoch on both sides; pin the representation the writer has to produce.
assert_eq "$(sqlite3 "$DST" "SELECT typeof(c_datetime) || ',' || typeof(c_epoch) FROM dst WHERE id = 1")" \
    "text,text" "DATETIME columns are written as TEXT"
assert_eq "$(sqlite3 "$DST" "SELECT c_datetime || '|' || c_epoch FROM dst WHERE id = 1")" \
    "2026-01-02 03:04:05.000|2026-01-01 19:04:01.002" \
    "legacy DATETIME text and integer epochs come out in the millisecond form"

# Fractional DECIMAL values must survive in every split, including the ranges whose
# first row happens to be integral.
assert_eq "$(sqlite3 "$DST" "SELECT count(*) FROM dst WHERE c_numeric IS NOT NULL AND c_numeric != CAST(c_numeric AS INTEGER)")" \
    "$(sqlite3 "$SRC" "SELECT count(*) FROM src WHERE c_numeric IS NOT NULL AND c_numeric != CAST(c_numeric AS INTEGER)")" \
    "fractional DECIMAL values survive the round trip"
