# Addax performance baseline

Runs the packaged distribution through an isolation matrix and records wall time,
CPU, RSS, Addax counters, GC logs and optional JFR. It does **not** change product
code: the first goal is to decide whether the per-row `Record`/`Column` model or an
I/O side is binding.

## Scenarios

| Case | Reader → Writer | What it isolates |
|---|---|---|
| `stream_sink` | streamreader → streamwriter (`print=true`, stdout discarded) | engine + record/column + writer string formatting |
| `stream_hdfs_text` | streamreader → hdfswriter TEXT | engine + text encoding + HDFS |
| `mysql_hdfs_text` | mysqlreader → hdfswriter TEXT | primary DB → HDFS path |
| `mysql_hdfs_parquet` | mysqlreader → hdfswriter PARQUET/SNAPPY | typed writer encode |
| `mysql_hdfs_orc` | mysqlreader → hdfswriter ORC/SNAPPY | typed writer encode |
| `mysql_mysql` | mysqlreader → mysqlwriter | DB → DB batch write |
| `mysql_local_orc` | mysqlreader → hdfswriter ORC on `file:///` | CPU-bound reader+ORC A/B on the DB host |
| `mysql_local_text` | mysqlreader → hdfswriter TEXT on `file:///` | CPU-bound reader+text A/B on the DB host |

`stream_sink` is not a true null sink: `streamwriter` must consume the records, so
`recordToString` and `System.out.print` are paid. They are constant across A/B runs and
JFR attributes them; the writer-free cost is visible by comparing with `stream_hdfs_text`.

The `*_local_*` jobs write through Hadoop's local filesystem (`defaultFS: file:///`)
and are excluded from the default matrix. Run them with `--case` on the host that also
runs the database: that removes both the DB network and the HDFS network, which is the
only way to see reader/writer CPU changes in wall time on a remote test cluster.

## Prerequisites

- JDK 17+ and a built distribution (`target/addax-6.2.1-SNAPSHOT`).
- `python3` for `data-phase.sh`; the runner itself only needs bash and awk.
- `MYSQL_PASSWORD` exported for the `mysql_*` cases; `bench.env` deliberately has no
  default. Host/user/table defaults are in `bench.env`.
- `ssh wgzhao-pc` access for the MariaDB fixture (the benchmark user is `hive` on HDFS).
- HDFS reachable through `/opt/app/addax/hdp-hdfs-site.xml`; output goes to
  `/tmp/addax_perf/<run-id>/`.
- The MariaDB `perf_source` / `perf_sink` tables (see below).

## Quick start

```bash
# Build once (two steps; the distribution profile alone does not assemble plugins
# that were never packaged)
mvn -B -T 1C -DskipTests -Dmaven.javadoc.skip=true -Dmaven.source.skip=true -Dgpg.skip=true clean package
mvn -B -T 1  -DskipTests -Dmaven.javadoc.skip=true -Dmaven.source.skip=true -Dgpg.skip=true package -Pdistribution

# Export the benchmark DB password once; bench.env has no default
export MYSQL_PASSWORD=...

# Create/seed the 2M-row fixture (drops only perf_source/perf_sink)
./e2e/perf/run-baseline.sh --seed --case stream_sink --quick

# Smoke one case
./e2e/perf/run-baseline.sh --quick --case stream_sink

# Full baseline: 1 warmup + 3 measured + 1 JFR profile per case, 4 channels
./e2e/perf/run-baseline.sh
```

Results land in `e2e/perf/results/<timestamp>/` (git-ignored):

- `baseline.csv` — one row per run.
- `baseline.md` — medians/min/max.
- `manifest.json` — git SHA, distribution, JVM, heap, channel, DB/HDFS targets.
- `logs/`, `jfr/`, `gc/` — raw evidence.

## CPU-bound A/B on the database host

Copy the distribution and `jobs/mysql_local_*.json` to the host that runs MariaDB,
then run with a local JDBC URL and a local output directory:

```bash
sh bin/addax.sh \
  -p"-Dmysql_jdbc_url=jdbc:mysql://127.0.0.1:3306/test -Dmysql_user=... \
      -Dmysql_password=... -Dperf_table=perf_source -Dperf_channel=4 \
      -Dlocal_out_dir=/tmp/addax_perf/out" \
  -j "-Xms4g -Xmx4g -DsocksProxyHost= -Dhttp.proxyHost= -Dhttps.proxyHost=" \
  jobs/mysql_local_orc.json
```

Keep two runtime copies (before/after jars) and alternate them run by run. Warm the
source table before each measured run (`SELECT ...` to `/dev/null`) so the InnoDB
buffer pool state is the same for both sides; otherwise the first run after a large
local read can look 30% slower for reasons unrelated to the code.

## Useful flags

```bash
--case 'mysql_hdfs_*'   # substring/glob; default all non-local jobs
--runs 3 --warmup 1     # measured / discarded repetitions
--profile 0             # skip the JFR run
--channel 4             # job.setting.speed.channel
--rows 2000000          # synthetic total; sliceRecordCount = rows / channel
--heap 4g               # -Xms/-Xmx
--home <dir>            # distribution (default: newest target/addax-*)
--seed                  # run the MariaDB DDL/seed first
```

`PERF_JVM_EXTRA` adds JVM flags (`perf`/`design` runs). `PERF_PROXY_ARGS` is empty by
default; only set it to the `-D...proxyHost=` overrides when the target is
`localhost` — on JDK 23/macOS those overrides break remote MySQL with
`NoRouteToHostException`.

## Interpreting

- External wall time also includes ~3s JVM/Addax startup and a 0-3s completion poll
  (`core.container.job.sleepInterval`; `core.json` sets 3000ms), so a run that moves
  data in 4s can report 9.5s. Use `./e2e/perf/data-phase.sh <results-dir>` for the
  first-reader-start to last-writer-finish interval when comparing code.
- `rows/s` = Addax `Number of rec` / external wall clock. `Average rps` is only a
  cross-check: Addax divides by whole transfer seconds and its byte counter is
  `Record.getByteSize()`, not physical HDFS bytes.
- Compare `stream_sink` (model + formatting), `mysql_*` (JDBC + model + writer), and
  `stream_hdfs_*` (model + writer). The differences attribute the cost.
- **Kill criterion**: if CPU samples in `com.wgzhao.addax.core.element` /
  `core.transport` plus the conversion methods are below ~15%, and GC pause is below
  ~10% of wall time, do not refactor the record model; fix the reader/writer/IO side.

JFR analysis (JDK 21+):

```bash
jfr summary run.jfr
jfr view allocation-by-class run.jfr
jfr view hot-methods run.jfr
jfr print --events jdk.ObjectAllocationSample --stack-depth 64 run.jfr > alloc.txt
```

## Caveats

- `addax.sh` always appends `-XX:+UseZGC` (the neighbouring G1 pause/IHOP flags are
  inert under ZGC). The launcher cannot switch collectors via `-j`: specifying a
  second collector makes the JVM fail with "Multiple garbage collectors selected".
  A GC A/B needs a modified launcher or a direct `java` invocation; the harness does
  not change the launcher.
- The runner does not edit `conf/core.json`; core.json currently has unlimited speed
  (`-1`) and channel capacity 512. Override via a distribution copy if a sweep needs
  other values.
- The DB→HDFS reader uses `splitPk=id` and `eachTableSplitSize=<channel>`; without it a
  single table can produce `ceil(channel/1)*5` splits and therefore a different HDFS
  file count.
- The fixture is deterministic (2M rows, ~5% NULLs, Chinese text, binary, decimals,
  timestamps). Re-seed only when the schema changes; do not re-seed between reps.
- The HDFS round-trip is not re-read by the harness yet; row counts are asserted from
  Addax. For a fidelity check, read the output back with `hdfsreader` (ORC/TEXT) →
  `txtfilewriter`, sort by the id column and compare. Do not compare ORC file md5s:
  ORC metadata contains a random writer id, so identical data still hashes differently.
- The `*_local_*` jobs replace the writer's `hdfsSitePath` with `defaultFS: file:///`.
  The HDFSWriter uses Hadoop's generic `FileSystem`, so local paths work, but the
  output is a normal local file tree, not HDFS.

The measured baseline, the environment, and the method for comparing other
tools are in [../../PERFORMANCE.md](../../PERFORMANCE.md).
