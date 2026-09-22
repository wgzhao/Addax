# Addax plugin E2E suite

Runs the *packaged distribution* -- real `bin/addax.sh`, real `plugin/` tree, real job
config files -- against throwaway databases, and asserts what came out. It exists
because a change to shared code (`CommonRdbmsWriter`, the `writeMode` templates in
`WriterUtil`, the type handling in `CommonRdbmsReader`) has no other way of showing its
blast radius: the build compiles, and nothing else checks that a plugin still moves data.

Because it drives the packaged layout, it also covers things a unit test cannot: plugin
resolution through `plugin.json`, the `JarLoader` classpath built from each plugin's
`libs/`, the `${placeholder}` substitution in job files, and `addax.sh`'s argument
handling and exit codes.

## Requirements

- JDK 17 on `PATH` (the launcher enforces it)
- A built distribution: `target/addax-<version>/`
- A container runtime: Docker, or Apple's `container` on macOS — only for the cases that
  read or write a database. A selection of file and object store cases runs without one.
- Python with `moto[server]` for the S3 case (`pip install 'moto[server]'`); that case
  asks to be skipped when the interpreter cannot import it, and `E2E_MOTO_PYTHON` points
  the case at a different interpreter (a virtualenv, usually). It starts the test double
  on `E2E_MOTO_PORT` (5111 by default) when nothing is listening there yet and leaves it
  running, so consecutive runs reuse it.

## Quick start

```bash
# 1. build the distribution (the same two invocations the release uses)
mvn -B -T 1C -DskipTests -Dmaven.javadoc.skip=true -Dmaven.source.skip=true -Dgpg.skip=true clean package
mvn -B -T 1  -DskipTests -Dmaven.javadoc.skip=true -Dmaven.source.skip=true -Dgpg.skip=true package -Pdistribution

# 2. start the databases
./e2e/start-db.sh --dbs mysql,postgres

# 3. run
./e2e/run.sh                          # everything
./e2e/run.sh --dbs mysql '060*'       # one case
./e2e/run.sh --list                   # what would run
./e2e/stop-db.sh                      # throw the databases away
```

`run.sh` finds the newest `target/addax-*/` by default; set `ADDAX_HOME` to test a
different one (for example an installed `/opt/addax`).

## Container runtimes

`E2E_RUNTIME` selects the runtime; by default Docker is preferred and Apple's
`container` is the fallback. Docker is what CI uses, so a machine that can run both
tests the same way CI does.

| | Docker | Apple `container` |
|---|---|---|
| Databases reachable at | `127.0.0.1:<published port>` | the container's own address |
| Port publishing | used | **not used** |
| Start the daemon | Docker Desktop / OrbStack / colima | `container system start` |

Apple's `container` (0.8.0) publishes a port that accepts a TCP connection and then
silently drops it: `nc -z` reports the port open, no protocol ever gets through, with
either `-p 13306:3306` or `-p 127.0.0.1:13306:3306`. Its containers are directly
routable on the vmnet bridge though, so on that runtime the suite skips publishing and
connects to the container address instead (`rt_apply_endpoints` in `lib/runtime.sh`).
An explicitly configured `MYSQL_HOST`/`POSTGRES_HOST` always wins over that
substitution.

Host ports are deliberately not 3306/5432: developer machines tend to have a real
database on those, and the suite must not care.

## Layout

```
e2e/
├── db.env                   connection parameters, container names, image tags
├── start-db.sh / stop-db.sh docker run + readiness polling
├── run.sh                   the runner
├── lib/
│   ├── common.sh            logging, case discovery, summary
│   ├── runtime.sh           docker <-> Apple container abstraction
│   ├── db.sh                db_reset / db_query / db_load
│   ├── assert.sh            assertions available to verify scripts
│   └── addax.sh             ADDAX_HOME resolution, -p building, job execution
├── fixtures/<db>/           schema.sql + seed.sql, shared by every case
└── cases/<NNN_name>/        one directory per case
```

## Adding a case

Adding a case means adding a directory under `cases/`. Nothing else needs editing --
`run.sh` discovers them.

| File | Required | Meaning |
|---|---|---|
| `case.env` | yes | `DBS="mysql postgres"` (which databases are needed), `SRC_DB`, `DST_DB` |
| `job.json` or `job.NN.json` | yes | one job, or several executed in `sort -V` order (round-trip cases need two) |
| `setup.sh` | no | runs after the fixture reset, before the first job. Sourced, so it sees `$E2E_CASE_OUT`, `$CASE_DIR`. Exiting 77 asks the runner to skip the case (an optional dependency is missing) instead of failing it |
| `verify.sh` | no* | sourced on top of `lib/*.sh`; non-zero means the case failed |
| `verify.<db>.sql` | no* | declarative: run against `<db>`, compared with a golden file |
| `expect.txt` / `expect.<db>.txt` | no | the golden file |
| `EXPECTED_FAIL="reason"` in `case.env` | no | marks a known defect (see below) |

\* exactly one of `verify.sh` / `verify.<db>.sql`.

`case.env` may also set `CASE_TIMEOUT` (seconds, applied only where `timeout(1)`
exists -- not on macOS by default).

A case runs when every database it lists in `DBS` is covered by `--dbs`, so
`./e2e/run.sh --dbs mysql` skips anything needing PostgreSQL.

## How cases prove themselves

- **Database targets** -- a query is run against the target and compared line by line
  with a golden file. Project every column through `CONCAT_WS`/`concat_ws` into one
  delimited column with an `ORDER BY`, and null-wrap every column: a NULL reaching
  `CONCAT_WS` is dropped silently, which shifts the remaining columns left and turns a
  real bug into an unreadable diff. Wrap string columns in brackets -- an empty string
  is then visibly `[]` rather than an ambiguous empty field, and a value with trailing
  spaces does not end a line in invisible whitespace that an editor can strip away.
- **File targets** -- the produced file is compared byte for byte. Where the writer
  mints its own file name (`hdfswriter` always appends a timestamp), match it with a
  glob and assert the count. `assert_dir_matches` concatenates the parts in name order.
- **Binary formats** -- parquet and ORC are read back with `hdfsreader` and landed as
  csv, which is then compared. That also covers the reader.

Golden files are deliberately small enough to review by eye in a pull request. That is
the only thing standing between this suite and goldens that quietly encode a bug.

## Date and time

The fixture carries `d`, `t`, `dt` and `ts` on every table, seeded with the values
that break implementations: just after the epoch, the far end of the MySQL TIMESTAMP
range (2038-01-19), a leap day, a pre-epoch date, and NULLs. The two dialects declare
them so the rows stay logically identical -- MySQL `TIMESTAMP` where PostgreSQL has
`timestamptz`, which is the one column whose JDBC type differs meaningfully between
the two.

Two different things are being asserted, and the distinction matters:

- **Naive columns** (`DATE`, `TIME`, `DATETIME` / `timestamp`) hold a wall clock. Their
  goldens assert the wall clock survived.
- **tz-aware columns** (`TIMESTAMP` / `timestamptz`) hold an instant. Their goldens
  render it `AT TIME ZONE 'UTC'`, so the comparison says nothing about which zone the
  process ran in -- and that is the point. `150_postgresql_datetime_tz` runs the exact
  same job as `140` with `CASE_TZ=Asia/Shanghai` against the **same golden**: if the
  pipeline ever let the JVM's zone leak into a stored instant, 140 would still pass and
  150 would fail by eight hours.

The JVM default timezone is otherwise pinned to `UTC` for every job (see `TZ=` in
`lib/addax.sh`), because it decides the wall clock for the whole RDBMS path: the reader
converts through `Calendar.getInstance()`, the drivers render in the connection's zone,
and `TimestampColumn.asString()` uses it directly. Without the pin, a laptop in
Asia/Shanghai and a CI runner in UTC produce different text from the same job, and a
golden could only ever be right on one of them.

Two quirks are recorded in the goldens rather than worked around:

- A writer-level `dateFormat` (txtfilewriter, `fileFormat: csv`/`text`) formats **every**
  date/time column through one pattern, including `TIME` -- whose value is
  millis-of-day, so it comes out anchored at the epoch:
  `08:30:45` renders as `1970-01-01 08:30:45` (`160_mysql_datetime_to_txt`).
- Without a `dateFormat`, the column's Java class decides the zone: `DATE`/`TIME` become
  a `DateColumn` and render through `common.column.timeZone` (PRC/`GMT+8` by default),
  while `TIMESTAMP` becomes a `TimestampColumn` and renders through the JVM zone. Two
  columns of the same row can therefore be rendered in different zones.

Also worth knowing when writing job files: `connection.jdbcUrl` is read with
`getString()`, so it must be a **plain string**. The list form (`["jdbc:..."]`) is
tolerated on the reader side -- cases 010 and 020 exercise it -- but on the writer side
it fails with `Cannot create JDBC driver ... for connect URL '["jdbc:..."]'`, which at
least names the mangled URL.

Every job sets an explicit `errorLimit`:

```json
"errorLimit": { "record": 0 }
```

Without it both `checkRecordLimit` and `checkPercentageLimit` return early -- they only
act when a limit is configured -- so a job whose records all fail to write still exits
0. With the limit set, the same failure stops the job with a message naming the
underlying database error, which is a far better diagnostic than a diff against a
golden file. The exit code alone is never the whole assertion: every case also checks
content.

## Updating a golden

```bash
./e2e/run.sh --dbs mysql '060*' --update-expect
git diff e2e/cases/060_mysql2txt_csv/expect.txt      # read every line
```

Run it to capture output you have *already* decided is correct -- never to make a
failing case green. The diff is a normal file diff, so a reviewer sees exactly what
was blessed.

## Known defects

`EXPECTED_FAIL="..."` in a `case.env` marks a case that documents a real, unfixed
defect. The case still runs, and its result is reported as `XFAIL` instead of `FAIL`.
If it ever passes, the run **fails** with `XPASS`: the defect is gone, and the marker
must be removed so the fix stays protected.

Currently marked:

- `110_mysql2txt_append` -- `txtfilewriter` with `writeMode: append` cannot work.
  `TxtFileWriter.prepare()` deliberately keeps files matching `fileName`, then
  `Task.startWrite()` calls `if (!newFile.createNewFile()) throw new IOException(...)`,
  which returns false exactly when the file exists. Relaxing that check is not enough
  either: `Files.newOutputStream(...)` truncates by default.

## The `-p` minefield

Job files get their connection parameters as `${placeholders}`, substituted from
System properties. To set one you pass:

```bash
addax.sh -p"-Dmysql_host=127.0.0.1" job.json
```

Three rules follow from how that is implemented, and all three are silent failures if
you get them wrong:

1. **Values may only contain `[A-Za-z0-9_.:/@%+,=-]`.** `addax.sh` expands the `-p`
   argument through `sh -c` *without quoting it*, so a `&`, a space, a quote or a `$`
   in a value is shell syntax there -- a JDBC URL with a query string would background
   the JVM and try to run the rest as a command. `run.sh` rejects anything else up
   front (`assert_param_safe`). Keep JDBC URLs query-string free; the drivers do not
   need one here.
2. **Placeholders are `${lower_snake_case}`, never dotted.** The substitution regex is
   `(\$)\{?(\w+)\}?`, so `${mysql.host}` silently becomes `${mysql` + `.host}`.
3. **An undefined variable is left literally in the job file**, with no error. Every
   value `run.sh` passes is checked to be non-empty, but a new placeholder needs adding
   to `build_addax_params` in `lib/addax.sh`.

## When a case fails

Everything lands under `$E2E_WORK_DIR` (`${TMPDIR:-/tmp}/addax-e2e` by default):

```
<case>/logs/<job>.console.log   the addax console output for that job
<case>/actual/                  what the verify queries returned, next to the golden
<case>/addax-params.txt         the resolved ADDAX_HOME and the full -p string
<case>/evidence/                collected on failure: job files, goldens, logs
```

The `-p` string matters because the `${placeholder}` substitution happens in memory
before parsing: there is no "resolved job file" anywhere on disk to look at.

In CI the whole work directory is uploaded as the `e2e-logs` artifact on failure.

## Coverage

| Case | Reader | Writer | Also covers |
|---|---|---|---|
| 010_mysql2postgresql | mysqlreader | postgresqlwriter | `splitPk`, 3 channels, `preSql` |
| 020_postgresql2mysql | postgresqlreader | mysqlwriter | `splitPk`, `preSql` |
| 040_mysql2mysql_upsert | mysqlreader | mysqlwriter | `writeMode: update(id)` -> ON DUPLICATE KEY UPDATE |
| 050_postgresql2postgresql_upsert | postgresqlreader | postgresqlwriter | `writeMode: update(id)` -> ON CONFLICT, `postSql` |
| 060_mysql2txt_csv | mysqlreader | txtfilewriter | csv quoting, header, `nullFormat` |
| 070_postgresql2hdfs_text | postgresqlreader | hdfswriter | TEXT on `file:///` |
| 080_mysql2hdfs_parquet | mysqlreader + hdfsreader | hdfswriter | parquet round-trip |
| 090_postgresql2hdfs_orc | postgresqlreader + hdfsreader | hdfswriter | ORC round-trip |
| 100_mysql2hdfs_overwrite | mysqlreader | hdfswriter | `overwrite` replaces the previous file |
| 110_mysql2txt_append | mysqlreader | txtfilewriter | append semantics (known defect) |
| 120_postgresql2txt_sql | postgresqlreader | txtfilewriter | `fileFormat: sql` |
| 130_mysql_datetime | mysqlreader | mysqlwriter | DATE, TIME, DATETIME, TIMESTAMP round-trip |
| 140_postgresql_datetime | postgresqlreader | postgresqlwriter | date, time, timestamp, timestamptz round-trip |
| 150_postgresql_datetime_tz | postgresqlreader | postgresqlwriter | same, run with a JVM zone 8 hours from UTC |
| 160_mysql_datetime_to_txt | mysqlreader | txtfilewriter | writer-level `dateFormat` rendering |
| 170_mysql_datetime_to_postgresql | mysqlreader | postgresqlwriter | the same values across dialects |
| 250_s3reader_objects | s3reader | txtfilewriter | object patterns (literal `.` and `+`), duplicate names, gzip detection, against moto |
| 260_hdfsreader_roundtrip | streamreader + hdfsreader | hdfswriter | parquet and ORC round-trip on `file:///`: decimal, date, timestamp, boolean, array, map, type aliases, `column: ["*"]` |

The fixture behind all of them is six rows carrying the values that readers and writers
actually get wrong: CJK, an embedded delimiter and double quote, a backslash, leading
and trailing spaces, an empty string, a single quote, a 64-character maximum-width
value, and negative and zero decimals.

## Not covered yet

- **Oracle.** Needs the `gvenzl/oracle-xe` image (~2 GB, 2-3 minute first boot); it is
  amd64-only, so on Apple Silicon it would run under emulation. The harness is built
  for it as an addition: a `DBS` value, a `start-db.sh` branch, a `fixtures/oracle/`
  pair, and a matrix leg in the workflow.
- **Date and time through the hdfs writers.** Every case that targets `hdfswriter`
  still uses only the original six columns, so the ORC and Parquet timestamp paths
  (`OrcWriter`'s `SimpleDateFormat`, Parquet's INT96 handling) and their `hdfsreader`
  counterparts are untested. This is the largest remaining gap: those paths pick their
  own zones, and `MyParquetReader` treats INT96 as UTC while `MyOrcReader` uses the JVM
  default.
- **The other mirror direction.** 170 covers MySQL -> PostgreSQL for date/time values;
  PostgreSQL -> MySQL (a `timestamptz` read as a string and parsed into a MySQL
  `TIMESTAMP`) is not covered.
- **Decimal columns through hdfswriter.** The hdfs cases declare `price` as `double`,
  so the goldens show `1.5` rather than `1.50`. A `decimal(10,2)` case would cover the
  scale-preserving path.
- **Large values.** The Oracle MERGE path binds at most 8191 characters / 32767 bytes
  for LOBs (`OracleWriter`), and nothing here goes near that.
