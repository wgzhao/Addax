# Addax performance baseline

Measured on 2026-10-25 with Addax 6.2.1-SNAPSHOT (commit `035a51f46`). This is a
reproducible baseline, not a vendor benchmark: it documents the test setup, the
numbers, and the parts of the pipeline that were binding, so later changes and
other tools can be compared on the same terms.

The harness that produced it lives in [`e2e/perf/`](e2e/perf/README.md).

## TL;DR

- On a host connected to MariaDB/HDFS over a ~5 MB/s link, DB → HDFS runs at
  **60–78k rows/s** with **0.9–1.2 CPU cores** used. The link is the limit.
- On the database host with a local DB and local filesystem, the same 2M-row
  fixture moves in a **3.3 s data phase for ORC (≈600k rows/s)** and
  **4.6 s for TEXT (≈430k rows/s)**, using roughly 8 of 12 cores during the
  data phase.
- The engine and record/column path alone (`streamreader` → `streamwriter`)
  reaches **209k rows/s** on the Mac at 2.1 cores.
- **4–8 channels** is the plateau; 16 channels is slower. One or two channels
  leave throughput on the table.
- GC is not a factor: ZGC paused for **0.138 ms in total** during a 29 s ORC
  profile.
- The per-row `DefaultRecord` + `Column` model was **not** the binding
  constraint in any measured scenario. Do not expect a record-model rewrite to
  move DB → HDFS throughput before the link/reader is addressed.

## Test setup

| Item | Value |
|---|---|
| Addax | 6.2.1-SNAPSHOT, git `035a51f46` |
| Client host | Apple M4, 16 GB RAM, macOS, JDK 23.0.2, ZGC, `-Xms4g -Xmx4g` |
| Database / CPU-bound host | Intel i5-10400 (6C/12T), 15 GB RAM, Linux, JDK 17.0.16, MariaDB |
| Driver | mysql-connector-j 8.2.0 |
| HDFS | `hdfs://yytz`, user `hive` |
| Channels | 4 unless stated otherwise |
| Rows | 2,000,000 |
| Row width | ≈96.6 bytes/row by Addax's `Record.getByteSize()` |

The fixture is deterministic and checked into the harness:

- [`e2e/perf/sql/mariadb_perf_source.sql`](e2e/perf/sql/mariadb_perf_source.sql)
  creates `perf_source` and an empty `perf_sink`.
- 12 columns: `id`, four VARCHARs, `c_int`, `c_long`, `c_double`, `c_date`,
  `c_bool`, `c_ts`, `c_varbinary`.
- About 5% NULLs, Chinese text, double values, dates and timestamps.

Two environments were measured:

1. **Mac baseline** — remote MariaDB and remote HDFS through the local system
   SOCKS proxy. This is the end-to-end path, but the proxy link is narrow.
2. **CPU-bound A/B host** — Addax and MariaDB on `wgzhao-pc`, writer on
   `file:///tmp` (Hadoop LocalFileSystem). This removes both networks and shows
   what the reader/writer code can do when it is not waiting on a socket.

The link evidence for environment 1: copying the 250 MB text dump over SSH took
51.1 s (≈5 MB/s), while MariaDB produced the same dump locally in 4.9 s
(≈408k rows/s). Addax moved the logical rows at ≈6.7 MB/s, i.e. at the link's
capacity.

## End-to-end results (Mac, network-bound)

Medians of 3 measured runs after 1 warmup. `Data phase` is the first reader
start to the last writer finish; `wall` also includes JVM/Addax startup and a
scheduler completion poll.

| Case | Reader → Writer | Rows/s (wall) | Rows/s (data phase) | Wall s | Data phase s | CPU % | Max RSS |
|---|---|---:|---:|---:|---:|---:|---:|
| `mysql_hdfs_orc` | mysql → HDFS ORC/SNAPPY | 68,306 | 83,160 | 29.28 | 24.05 | 94.2 | 2.9 GB |
| `mysql_hdfs_parquet` | mysql → HDFS PARQUET/SNAPPY | 68,213 | 76,482 | 29.32 | 26.15 | 105.0 | 3.4 GB |
| `mysql_hdfs_text` | mysql → HDFS TEXT | 60,514 | 68,775 | 33.05 | 29.08 | 106.2 | 3.7 GB |
| `mysql_mysql` | mysql → mysql (batch insert) | 42,571 | 46,904 | 46.98 | 42.64 | 65.9 | 3.9 GB |
| `mysql_sink` | mysql → stdout sink | 77,700 | 90,992 | 25.74 | 21.98 | 118.0 | 3.9 GB |
| `stream_hdfs_text` | stream → HDFS TEXT | 58,514 | n/a | 34.18 | n/a | 92.0 | 3.8 GB |
| `stream_sink` | stream → stdout sink | 209,424 | n/a | 9.55 | n/a | 208.9 | 3.9 GB |

What the rows isolate:

- `stream_sink` is the CPU-only path: engine, stream generator, record/column
  objects and stdout formatting. It is not a true null sink, so the number is a
  lower bound for engine throughput.
- `stream_hdfs_text` adds HDFS text encoding and the network.
- `mysql_*` adds JDBC decode and conversion. ORC and Parquet converge at
  ≈68k rows/s, while TEXT is slower because every field is rendered as text.
- `mysql_mysql` is writer-bound: batch inserts over the same link are slower
  than reads.
- `mysql_sink` removes the HDFS writer but still pays the JDBC reader and stdout
  formatting.

The wait counters in the Addax log tell the same story: for ORC,
`WaitReaderTime` was ≈72 s versus `WaitWriterTime` ≈0.2 s (writers wait for the
reader); for `mysql_mysql` the ratio is reversed (readers wait for the writer).

## CPU-bound results (DB host, local DB and local filesystem)

These runs remove the network. External wall time is misleading for a 2M-row
job because startup (~3.3 s) and the scheduler poll (0–3 s,
`core.container.job.sleepInterval=3000`) dominate it. The table reports the data
phase; rows/s is calculated from that phase.

| Output | Data phase (median of 3) | Rows/s (data phase) | Notes |
|---|---:|---:|---|
| ORC/SNAPPY, local FS | 3.32 s | ≈602k | reader and ORC writer both near the limit |
| TEXT, local FS | 4.61 s | ≈434k | TEXT writer is the limit |

The MariaDB buffer pool was warmed before every run and before/after jars were
interleaved; a single cold run can look 30% slower than the rest.

### Channel scaling (ORC, local DB/FS)

Medians of 3 runs per channel count:

| Channels | Data phase | Rows/s (data phase) |
|---:|---:|---:|
| 1 | 6.62 s | ≈302k |
| 2 | 4.37 s | ≈458k |
| 4 | 3.92 s* | ≈510k* |
| 8 | 3.72 s | ≈537k |
| 16 | 4.15 s | ≈482k |

\* The controlled interleaved ORC run at 4 channels was 3.32 s (≈602k rows/s);
the sweep's 4-channel point was noisier. The stable conclusion is that **4–8
channels is the plateau and 16 is worse**; start at 4 and test 4–8 on the target
host.

## Profiles and GC

JFR on the Mac ORC run (`settings=profile`, 4 GB heap, ZGC):

- **Allocations:** `String` 23.3%, `byte[]` 10.6%, `int[]` 10.0%, `ZoneInfo`
  7.3%, `BytesColumn` 7.2%, `Gregorian$Date` 4.8%, `BigDecimal` 4.5%.
  Column/record classes together were ≈17%.
- **Reader hotspots** include MySQL's text-protocol decode, `BigInteger` and
  `BigDecimal` parsing, and Calendar/ZoneInfo work. Connector/J 8.2 creates a
  `SqlDateValueFactory` for every `getDate(int)` call and clones a Calendar in
  every `getTimestamp(int, Calendar)` call; that is driver behavior, not a
  Column-class cost.
- **GC:** 15 ZGC pauses, 0.138 ms total, in a 29 s run.

The CPU samples were ≈73% reader threads and ≈27% writer threads on the network
path. The reader was waiting on the link, not saturating a core: total CPU was
under one core for the whole job.

## What the numbers mean

- **A row-model rewrite is not justified by these measurements.** Allocation and
  conversion costs are visible in profiles, but wall time is set by the link
  (Mac) or by the JDBC reader / TEXT writer (local FS). Packed records,
  pooling or columnar batches would add compatibility risk for no measured
  end-to-end gain.
- **Before optimizing code, check the topology.** The same binary moves 2M rows
  in a 3–4 s data phase when the DB is local and the sink is local, versus
  24–29 s over the 5 MB/s proxy link.
- **Format matters.** ORC/Parquet were faster than TEXT on the same rows. Use
  TEXT when the consumer needs it, not by default.
- **Channels:** 4–8 is the useful range on a 12-core host; more connections can
  reduce throughput.
- **Fixed overhead:** JVM/Addax startup is ~3 s and the scheduler completion
  poll adds 0–3 s. For short jobs, tune
  `core.container.job.sleepInterval`; for throughput comparisons, use the data
  phase from `e2e/perf/data-phase.sh`.

## Investigation notes (evaluated, not merged)

A set of conversion-path micro-optimizations was implemented and A/B-tested
during this investigation:

- avoiding unconditional `rawData.toString()` in the HDFS TEXT writer;
- `Long.parseLong` / `Double.parseDouble` fast paths with BigDecimal fallbacks;
- `rs.getLong` for TINYINT/SMALLINT/INT instead of `getString`;
- `getTimestamp(i)` without a per-call Calendar;
- building the record without a temporary `Column[]`.

They reduced CPU by roughly 5–20% and improved the TEXT data phase by ≈13% in
isolation, but the ORC data phase did not move, and end-to-end wall time on the
network-bound path was unchanged. They were **not merged**, because the gain was
narrow and inconsistent. Reading DATE through `getTimestamp` was also tried and
was ≈20–30% slower, so DATE keeps `getDate`.

If a future deployment is CPU-bound (fast LAN, local storage, many columns),
these are the first places to revisit; the record/column representation itself
is not.

## Reproducing and comparing other tools

Use [`e2e/perf/README.md`](e2e/perf/README.md). For a fair comparison:

1. Use the same fixture, schema and row count; keep row width in mind, because
   rows/s is not MB/s.
2. Use the same topology and the same heap. Record the link throughput next to
   rows/s.
3. Warm the source table and the target filesystem before each measured run.
4. Measure the **data phase** (first reader start to last writer finish), not
   only external wall time; startup and the scheduler poll are fixed overheads.
5. Alternate the two tools run by run, at least 3 measured runs each, and report
   medians.
6. Verify data fidelity, not just row counts: sorted TEXT output hash, or decoded
   ORC/Parquet rows. Do not compare ORC file hashes; ORC metadata contains a
   random writer id.

## Caveats

- Numbers are specific to this 2M-row fixture, MariaDB, Connector/J 8.2.0, ZGC,
  the two hosts and the 5 MB/s proxy link. They are a baseline, not a ceiling.
- The Mac results are transfer-bound. Do not read 60–80k rows/s as the code's
  maximum; the local-DB runs show 400–600k rows/s on the same fixture.
- The A/B host is shared. The channel sweep and the 4-channel interleaved run
  differ by ~18% on the same code; use medians and interleaved runs.
- `wall` includes ~3 s of startup and an 0–3 s scheduler poll, which is why a
  4 s data phase can appear as a 9.5 s wall.
- HDFS output paths are under `/tmp/addax_perf/`; the harness cleans its own
  output but the results directory is git-ignored and machine-specific.
