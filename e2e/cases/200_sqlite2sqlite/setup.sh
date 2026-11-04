# The sqlite3 CLI is scaffolding (fixture creation and verification), not part of
# Addax itself. It is preinstalled on the CI image; elsewhere its absence skips the
# case instead of failing it.
if ! command -v sqlite3 >/dev/null 2>&1; then
    log "sqlite3 CLI is not installed, skipping"
    exit 77
fi

rm -rf "$E2E_CASE_OUT"
mkdir -p "$E2E_CASE_OUT"

SRC="$E2E_CASE_WORK/src.db"
DST="$E2E_CASE_WORK/dst.db"
rm -f "$SRC" "$DST"

# 400 rows is deliberate: the reader splits the PK into ten ranges of 39 rows, so
# some ranges start on rows whose DECIMAL value is integral (i multiple of 8) and
# continue with fractional ones. The SQLite driver derives a column's JDBC type
# from the storage class of the row it is positioned on, which is exactly the
# condition that used to make the shared reader truncate those fractions.
#
# c_datetime exercises the TEXT representation, c_epoch an INTEGER epoch written by
# 3.54.1.0 itself, and c_numeric has a scale-less NUMERIC target so the writer's
# binding path is covered too.
sqlite3 "$SRC" <<'SQL'
CREATE TABLE src (
  id INTEGER PRIMARY KEY,
  c_int INTEGER,
  c_big BIGINT,
  c_real REAL,
  c_text TEXT,
  c_blob BLOB,
  c_bool BOOLEAN,
  c_date DATE,
  c_datetime DATETIME,
  c_epoch DATETIME,
  c_numeric DECIMAL(12,3)
);
WITH RECURSIVE seq(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM seq WHERE i < 400)
INSERT INTO src
SELECT i,
       CASE WHEN i % 97 = 0 THEN NULL ELSE i * 3 - 7 END,
       9007199254740993 + i,
       i * 0.25,
       CASE WHEN i % 89 = 0 THEN NULL ELSE 'row-' || i || '-é-世界-''quote''' END,
       CASE WHEN i % 101 = 0 THEN NULL ELSE X'0001FF4100' END,
       i % 2,
       '2026-01-02 03:04:05',
       -- the first row is the seconds-only TEXT an older driver wrote
       CASE WHEN i = 1 THEN '2026-01-02 03:04:05'
            ELSE printf('2026-01-02 03:04:%02d.%03d', i % 60, i % 1000) END,
       CASE WHEN i % 11 = 0 THEN NULL ELSE 1767294241001 + i END,
       CASE WHEN i % 53 = 0 THEN NULL ELSE i * 1.125 END
FROM seq;
SQL

# A scale-less NUMERIC target: the driver reports scale 0, which used to send the
# shared writer down its integer binding branch.
sqlite3 "$DST" <<'SQL'
CREATE TABLE dst (
  id INTEGER PRIMARY KEY,
  c_int INTEGER,
  c_big BIGINT,
  c_real REAL,
  c_text TEXT,
  c_blob BLOB,
  c_bool BOOLEAN,
  c_date DATE,
  c_datetime DATETIME,
  c_epoch DATETIME,
  c_numeric NUMERIC
);
SQL
