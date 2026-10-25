#!/usr/bin/env bash
#
# Reproducible Addax performance baseline.
#
# Runs a small isolation matrix through the packaged distribution and records
# wall time, CPU, RSS, Addax's own counters, GC logs and (optionally) JFR.
# It deliberately does not change product code: the point is to decide, from
# measurements, whether the Record/Column model or an I/O side is binding.
#
#   ./e2e/perf/run-baseline.sh --list
#   ./e2e/perf/run-baseline.sh --quick --case stream_sink
#   ./e2e/perf/run-baseline.sh --case 'mysql_hdfs_*' --channel 4 --runs 3
#   ./e2e/perf/run-baseline.sh --seed --case mysql_hdfs_text
#
# See e2e/perf/README.md for the scenarios and the interpretation.

set -euo pipefail

PERF_DIR="$(cd "$(dirname "$0")" && pwd -P)"
E2E_DIR="$(dirname "$PERF_DIR")"
# shellcheck source=../lib/common.sh
. "${E2E_DIR}/lib/common.sh"
# shellcheck source=../lib/addax.sh
. "${E2E_DIR}/lib/addax.sh"
# shellcheck source=bench.env
. "${PERF_DIR}/bench.env"

# bash 3.2 (macOS) keeps the array helpers out of scope; this script uses scalars
# and newline-separated lists instead.

CASE_FILTER=""
RUNS="$PERF_RUNS"
WARMUP="$PERF_WARMUP"
PROFILE="$PERF_PROFILE"
CHANNEL="$PERF_CHANNEL"
ROWS="$PERF_ROWS"
HEAP="$PERF_HEAP"
OUT_DIR=""
HOME_ARG=""
DO_SEED=0
DRY_RUN=0
LIST_ONLY=0

usage() {
    cat <<'EOF'
Usage: run-baseline.sh [options]

Options:
  --case <glob>       case name or glob (default: all jobs under jobs/)
  --runs <n>          measured repetitions (default: 3)
  --warmup <n>        discarded warmup repetitions (default: 1)
  --profile <0|1>     run one extra JFR-profiled repetition (default: 1)
  --channel <n>       job.setting.speed.channel (default: 4)
  --rows <n>          expected total rows for the synthetic reader (default: 2,000,000)
  --heap <size>       -Xms/-Xmx value (default: 4g)
  --home <dir>        Addax distribution (default: $ADDAX_HOME, newest target/addax-*, /opt/app/addax)
  --out <dir>         result directory (default: e2e/perf/results/<timestamp>)
  --seed              create/seed perf_source and perf_sink via ssh
  --quick             warmup=0, runs=1, profile=0
  --dry-run           print the commands without running them
  --list              list case names and exit
  -h, --help          this message

Environment: see bench.env. The runner never edits core.json; pass JVM flags
through --heap and the PERF_JVM_EXTRA environment variable.
EOF
}

while [ $# -gt 0 ]; do
    case "$1" in
        --case) CASE_FILTER="$2"; shift 2 ;;
        --runs) RUNS="$2"; shift 2 ;;
        --warmup) WARMUP="$2"; shift 2 ;;
        --profile) PROFILE="$2"; shift 2 ;;
        --channel) CHANNEL="$2"; shift 2 ;;
        --rows) ROWS="$2"; shift 2 ;;
        --heap) HEAP="$2"; shift 2 ;;
        --home) HOME_ARG="$2"; shift 2 ;;
        --out) OUT_DIR="$2"; shift 2 ;;
        --seed) DO_SEED=1; shift ;;
        --quick) WARMUP=0; RUNS=1; PROFILE=0; shift ;;
        --dry-run) DRY_RUN=1; shift ;;
        --list) LIST_ONLY=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) die "unknown option: $1 (try --help)" ;;
    esac
done

case_matches() { # name
    [ -n "$CASE_FILTER" ] || return 0
    # shellcheck disable=SC2254 # a glob is intentional
    case "$1" in $CASE_FILTER) return 0 ;; esac
    return 1
}

list_cases() {
    local f name
    for f in "$PERF_DIR"/jobs/*.json; do
        [ -e "$f" ] || continue
        name="$(basename "$f" .json)"
        # The *_local_* jobs write to the local filesystem and are meant to be run
        # on the host that also runs the database (CPU-bound A/B without the
        # network). They are not part of the default remote-HDFS matrix.
        if [ -z "$CASE_FILTER" ]; then
            case "$name" in *_local_*) continue ;; esac
        fi
        case_matches "$name" || continue
        printf '%s\n' "$name"
    done
}

if [ "$LIST_ONLY" -eq 1 ]; then
    list_cases
    exit 0
fi

[ "$RUNS" -ge 1 ] || die "--runs must be >= 1"
[ "$WARMUP" -ge 0 ] || die "--warmup must be >= 0"
[ "$CHANNEL" -ge 1 ] || die "--channel must be >= 1"
[ "$ROWS" -ge "$CHANNEL" ] || die "--rows must be >= --channel"

if [ -n "$HOME_ARG" ]; then
    ADDAX_HOME="$HOME_ARG"
elif [ -z "${ADDAX_HOME:-}" ]; then
    ADDAX_HOME="$(resolve_addax_home)" || die "no distribution found; set --home or ADDAX_HOME"
fi
ADDAX_HOME="$(cd "$ADDAX_HOME" && pwd -P)"
[ -x "$ADDAX_HOME/bin/addax.sh" ] || die "$ADDAX_HOME does not contain bin/addax.sh"
[ -f "$ADDAX_HOME/conf/core.json" ] || die "$ADDAX_HOME does not contain conf/core.json"

needs_mysql=0
for case_name in $(list_cases); do
    case "$case_name" in mysql_*) needs_mysql=1; break ;; esac
done
if [ "$DO_SEED" -eq 1 ]; then
    needs_mysql=1
fi
if [ "$needs_mysql" -eq 1 ] && [ -z "$MYSQL_PASSWORD" ]; then
    die "MYSQL_PASSWORD is required for the mysql_* cases (export it; bench.env has no default)"
fi

require_commands java ssh

if [ "$(uname -s)" = "Darwin" ]; then
    TIME_BIN=/usr/bin/time
    TIME_STYLE=bsd
else
    TIME_BIN=/usr/bin/time
    TIME_STYLE=gnu
fi
[ -x "$TIME_BIN" ] || die "$TIME_BIN is required for CPU/RSS measurement"

if [ -z "$OUT_DIR" ]; then
    OUT_DIR="${PERF_OUT_ROOT}/$(date +%Y%m%d-%H%M%S)"
fi
mkdir -p "$OUT_DIR/logs" "$OUT_DIR/jfr" "$OUT_DIR/gc" "$OUT_DIR/local_out"

RUN_ID="$(basename "$OUT_DIR")"
CSV="$OUT_DIR/baseline.csv"
[ -e "$CSV" ] || printf '%s\n' \
    'case,label,status,wall_s,user_s,sys_s,cpu_pct,max_rss_kb,rows,rows_per_s,addax_rps,bytes,failed,log,jfr,gc' >"$CSV"

SLICE_ROWS=$((ROWS / CHANNEL))
[ "$SLICE_ROWS" -ge 1 ] || SLICE_ROWS=1
MYSQL_JDBC_URL="jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DB}"

ADDAX_PARAMS=""
append_param() { # name, value
    assert_param_safe "$1" "$2"
    ADDAX_PARAMS="${ADDAX_PARAMS}${ADDAX_PARAMS:+ }-D$1=$2"
}
append_param console.enabled false
append_param mysql_host "$MYSQL_HOST"
append_param mysql_port "$MYSQL_PORT"
append_param mysql_user "$MYSQL_USER"
# stream-only runs do not need the DB; mysql_* runs are checked above
if [ -n "$MYSQL_PASSWORD" ]; then
    append_param mysql_password "$MYSQL_PASSWORD"
fi
append_param mysql_db "$MYSQL_DB"
append_param mysql_jdbc_url "$MYSQL_JDBC_URL"
append_param perf_table "$PERF_TABLE"
append_param perf_sink_table "$PERF_SINK_TABLE"
append_param perf_channel "$CHANNEL"
append_param perf_rows "$ROWS"
append_param perf_slice_rows "$SLICE_ROWS"
append_param perf_run_id "$RUN_ID"
append_param hdfs_default_fs "$HDFS_DEFAULT_FS"
append_param hdfs_site_path "$HDFS_SITE_PATH"
append_param hdfs_base "$HDFS_BASE"
append_param local_out_dir "$OUT_DIR/local_out"

# The benchmark targets are remote (wgzhao-pc and the HDFS cluster), and the
# macOS system proxy does not interfere with those JDBC/HDFS sockets. Clearing
# the proxy properties is only needed for localhost targets; doing it
# unconditionally breaks the MySQL driver on this JDK (NoRouteToHostException).
PROXY_ARGS="${PERF_PROXY_ARGS-}"
BASE_JVM="-Xms${HEAP} -Xmx${HEAP} -DHADOOP_USER_NAME=${HADOOP_USER_NAME} ${PROXY_ARGS}"
BASE_JVM="${BASE_JVM}${PERF_JVM_EXTRA:+ ${PERF_JVM_EXTRA}}"

mysql_exec() { # sql
    local i=0
    while [ "$i" -lt 5 ]; do
        if [ "$MYSQL_HOST" = "127.0.0.1" ] || [ "$MYSQL_HOST" = "localhost" ]; then
            mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" \
                -D "$MYSQL_DB" -e "$1" && return 0
        else
            # The test host occasionally drops a fresh SSH session ("Connection
            # closed by 127.0.0.1 port 12450"); retry rather than fail the run.
            ssh -o BatchMode=yes -o ConnectTimeout=10 "$MYSQL_HOST" \
                "mysql -h 127.0.0.1 -P $MYSQL_PORT -u $MYSQL_USER -p$MYSQL_PASSWORD -D $MYSQL_DB -e \"$1\"" && return 0
        fi
        i=$((i + 1))
        sleep 1
    done
    return 1
}

seed_database() {
    local sql="$PERF_DIR/sql/mariadb_perf_source.sql"
    local i=0
    [ -f "$sql" ] || die "missing $sql"
    log "seeding $MYSQL_HOST:$MYSQL_PORT/$MYSQL_DB from $(basename "$sql")"
    if [ "$DRY_RUN" -eq 1 ]; then
        log "  (dry-run) would pipe $sql into mysql"
        return 0
    fi
    while [ "$i" -lt 5 ]; do
        if [ "$MYSQL_HOST" = "127.0.0.1" ] || [ "$MYSQL_HOST" = "localhost" ]; then
            mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" \
                -D "$MYSQL_DB" <"$sql" && return 0
        else
            ssh -o BatchMode=yes -o ConnectTimeout=10 "$MYSQL_HOST" \
                "mysql -h 127.0.0.1 -P $MYSQL_PORT -u $MYSQL_USER -p$MYSQL_PASSWORD -D $MYSQL_DB" <"$sql" && return 0
        fi
        i=$((i + 1))
        sleep 1
    done
    return 1
}

prepare_case() { # case
    case "$1" in
        mysql_mysql)
            if [ "$DRY_RUN" -eq 0 ]; then
                mysql_exec "TRUNCATE TABLE $PERF_SINK_TABLE"
            fi
            ;;
    esac
}

parse_addax_stat() { # logfile, label
    local logfile="$1" label="$2"
    [ -f "$logfile" ] || return 1
    # The final report is a multi-line INFO message; grab the last occurrence of
    # each label so an earlier periodic report cannot win.
    sed -n "s/^.*${label}[[:space:]]*:[[:space:]]*//p" "$logfile" | tail -1
}

run_one() { # case, label, profile(0|1)
    local case_name="$1" label="$2" want_jfr="$3"
    local job="$PERF_DIR/jobs/${case_name}.json"
    local log_dir="$OUT_DIR/logs/${case_name}-${label}"
    local console="$OUT_DIR/logs/${case_name}-${label}.console.log"
    local time_log="$OUT_DIR/logs/${case_name}-${label}.time.log"
    local jfr="$OUT_DIR/jfr/${case_name}-${label}.jfr"
    local gc_log="$OUT_DIR/gc/${case_name}-${label}.gc.log"
    local jvm status=0

    [ -f "$job" ] || die "missing job $job"
    mkdir -p "$log_dir"
    jvm="$BASE_JVM -Xlog:gc*:file=${gc_log}:time,uptime:filecount=1"
    if [ "$want_jfr" -eq 1 ]; then
        jvm="$jvm -XX:StartFlightRecording=filename=${jfr},settings=profile,delay=${PERF_JFR_DELAY},dumponexit=true"
    fi

    prepare_case "$case_name"

    local cmd=("$TIME_BIN" -o "$time_log")
    if [ "$TIME_STYLE" = "bsd" ]; then
        cmd+=(-l)
    else
        cmd+=(-v)
    fi

    if [ "$DRY_RUN" -eq 1 ]; then
        log "  (dry-run) $case_name/$label: $ADDAX_HOME/bin/addax.sh -p\"$ADDAX_PARAMS\" -l $log_dir -L info -j \"$jvm\" $job"
        return 0
    fi

    log "  $case_name/$label"
    # stream_sink is the only case that prints records to stdout; keep that
    # output out of the console log so the log stays readable.
    case "$case_name" in
        *_sink) is_sink=1 ;;
        *) is_sink=0 ;;
    esac
    if [ "$is_sink" -eq 1 ]; then
        "${cmd[@]}" "$ADDAX_HOME/bin/addax.sh" -p"$ADDAX_PARAMS" -l "$log_dir" -L info \
            -j "$jvm" "$job" >/dev/null 2>"$console" || status=$?
    else
        "${cmd[@]}" "$ADDAX_HOME/bin/addax.sh" -p"$ADDAX_PARAMS" -l "$log_dir" -L info \
            -j "$jvm" "$job" >"$console" 2>&1 || status=$?
    fi

    local addax_log
    addax_log="$(find "$log_dir" -type f -name 'addax_*.log' -print 2>/dev/null | sort | tail -1)"
    if [ -z "$addax_log" ]; then
        addax_log="$(find "$ADDAX_HOME/log" -type f -name 'addax_*.log' -newer "$console" -print 2>/dev/null | sort | tail -1)"
    fi

    local wall user sys rss rows rps addax_rps bytes failed
    if [ "$TIME_STYLE" = "bsd" ]; then
        # BSD time puts all three on one line: "<real> real <user> user <sys> sys".
        # The maximum RSS is reported in bytes, unlike GNU time's kilobytes.
        wall="$(awk '/real/ {print $1; exit}' "$time_log" 2>/dev/null || true)"
        user="$(awk '/real/ {print $3; exit}' "$time_log" 2>/dev/null || true)"
        sys="$(awk '/real/ {print $5; exit}' "$time_log" 2>/dev/null || true)"
        rss="$(awk '/maximum resident set size/ {print int($1 / 1024); exit}' "$time_log" 2>/dev/null || true)"
    else
        wall="$(awk -F': ' '/Elapsed \(wall clock\)/ {print $2; exit}' "$time_log" 2>/dev/null || true)"
        user="$(awk -F': ' '/User time \(seconds\)/ {print $2; exit}' "$time_log" 2>/dev/null || true)"
        sys="$(awk -F': ' '/System time \(seconds\)/ {print $2; exit}' "$time_log" 2>/dev/null || true)"
        rss="$(awk -F': ' '/Maximum resident set size/ {print $2; exit}' "$time_log" 2>/dev/null || true)"
        # GNU time renders elapsed as h:mm:ss or m:ss; normalise to seconds.
        case "$wall" in
            *:*) wall="$(awk -F: '{ if (NF == 3) print $1*3600 + $2*60 + $3; else print $1*60 + $2 }' <<<"$wall")" ;;
        esac
    fi
    wall="${wall:-0}"
    user="${user:-0}"
    sys="${sys:-0}"
    rss="${rss:-0}"

    rows="$(parse_addax_stat "$addax_log" 'Number of rec' || true)"
    addax_rps="$(parse_addax_stat "$addax_log" 'Average   rps' || true)"
    bytes="$(parse_addax_stat "$addax_log" 'Total   bytes' || true)"
    failed="$(parse_addax_stat "$addax_log" 'Failed record' || true)"
    rows="$(printf '%s' "${rows:-0}" | tr -d ', ')"
    addax_rps="$(printf '%s' "${addax_rps:-0}" | tr -d ', ' | sed 's/rec\/s//')"
    bytes="$(printf '%s' "${bytes:-0}" | tr -d ', ')"
    failed="$(printf '%s' "${failed:-0}" | tr -d ', ')"

    rps="$(awk -v r="${rows:-0}" -v w="${wall:-0}" 'BEGIN { if (w > 0) printf "%.3f", r / w; else print "0" }')"
    local cpu
    cpu="$(awk -v u="$user" -v s="$sys" -v w="$wall" 'BEGIN { if (w > 0) printf "%.1f", (u + s) * 100 / w; else print "0" }')"

    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$case_name" "$label" "$status" "$wall" "$user" "$sys" "$cpu" "$rss" \
        "$rows" "$rps" "$addax_rps" "$bytes" "$failed" \
        "$addax_log" "$jfr" "$gc_log" >>"$CSV"

    printf '    status=%s wall=%ss rows=%s rows/s=%s cpu=%s%% rss=%sKB failed=%s\n' \
        "$status" "$wall" "$rows" "$rps" "$cpu" "$rss" "$failed"
    if [ "$status" -ne 0 ]; then
        printf '    last lines of %s:\n' "$console"
        tail -20 "$console" | sed 's/^/      /' >&2
        return 1
    fi
    return 0
}

median_of() { # case, column, filter-label-prefix
    local case_name="$1" col="$2" prefix="$3"
    awk -F, -v c="$case_name" -v p="$prefix" -v n="$col" \
        '$1 == c && $2 ~ p && $3 == 0 {print $n}' "$CSV" | sort -n | \
        awk '{a[NR] = $1} END {
            if (NR == 0) { print "NA"; exit }
            if (NR % 2) printf "%.3f", a[(NR + 1) / 2];
            else printf "%.3f", (a[NR / 2] + a[NR / 2 + 1]) / 2
        }'
}

min_of() { # case, column, prefix
    awk -F, -v c="$1" -v p="$3" -v n="$2" '$1 == c && $2 ~ p && $3 == 0 {print $n}' "$CSV" | sort -n | head -1
}

max_of() { # case, column, prefix
    awk -F, -v c="$1" -v p="$3" -v n="$2" '$1 == c && $2 ~ p && $3 == 0 {print $n}' "$CSV" | sort -n | tail -1
}

write_summary() {
    local md="$OUT_DIR/baseline.md"
    {
        printf '# Addax performance baseline\n\n'
        printf -- '- run: `%s`\n' "$RUN_ID"
        printf -- '- distribution: `%s`\n' "$ADDAX_HOME"
        printf -- '- git: `%s`\n' "$(git -C "$E2E_DIR/.." rev-parse --short HEAD 2>/dev/null || echo unknown)"
        printf -- '- channel: %s, warmup: %s, measured runs: %s, JFR profile run: %s\n' \
            "$CHANNEL" "$WARMUP" "$RUNS" "$PROFILE"
        printf -- '- heap: `%s`; JVM: `%s`\n\n' "$HEAP" "$(java -version 2>&1 | head -1)"
        printf '| Case | Runs | Median rows/s | Min | Max | Median wall s | Median CPU %% | Median RSS KB | Expected rows | Failed |\n'
        printf '|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n'
        local c
        for c in $(list_cases); do
            local runs_present
            runs_present="$(awk -F, -v c="$c" '$1 == c && $2 ~ /^run/ {n++} END {print n+0}' "$CSV")"
            printf '| `%s` | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n' \
                "$c" "$runs_present" \
                "$(median_of "$c" 10 '^run')" \
                "$(min_of "$c" 10 '^run')" \
                "$(max_of "$c" 10 '^run')" \
                "$(median_of "$c" 4 '^run')" \
                "$(median_of "$c" 7 '^run')" \
                "$(median_of "$c" 8 '^run')" \
                "$ROWS" \
                "$(awk -F, -v c="$c" '$1 == c && $2 ~ /^run/ {s += $13} END {print s+0}' "$CSV")"
        done
        printf '\nNotes:\n'
        printf -- '- `rows/s` is `Number of rec` divided by the external wall clock, not Addax `Average rps` (the latter divides by whole transfer seconds).\n'
        printf -- '- Addax `Total bytes` is `Record.getByteSize()`, not physical HDFS bytes; use HDFS output size for MB/s.\n'
        printf -- '- A warmup and the JFR-profiled run are excluded from the medians above.\n'
        printf -- '- A failed case still has a CSV row (`status != 0`) and its console log in `logs/`.\n'
    } >"$md"
    log "summary: $md"
}

write_manifest() {
    local core_json="$ADDAX_HOME/conf/core.json"
    cat >"$OUT_DIR/manifest.json" <<EOF
{
  "run_id": "$RUN_ID",
  "started_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "git_head": "$(git -C "$E2E_DIR/.." rev-parse HEAD 2>/dev/null || echo unknown)",
  "git_dirty": "$(git -C "$E2E_DIR/.." status --porcelain 2>/dev/null | wc -l | tr -d ' ')",
  "addax_home": "$ADDAX_HOME",
  "addax_version": "$(basename "$ADDAX_HOME")",
  "core_json_sha256": "$(shasum -a 256 "$core_json" 2>/dev/null | awk '{print $1}')",
  "java": "$(java -version 2>&1 | head -1 | tr '"' "'" )",
  "os": "$(uname -a | tr '"' "'" )",
  "channel": $CHANNEL,
  "rows_expected": $ROWS,
  "heap": "$HEAP",
  "jvm_extra": "${PERF_JVM_EXTRA:-}",
  "mysql": "${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DB}",
  "mysql_table": "$PERF_TABLE",
  "hdfs_default_fs": "$HDFS_DEFAULT_FS",
  "hdfs_site_path": "$HDFS_SITE_PATH",
  "hdfs_base": "$HDFS_BASE"
}
EOF
}

log "Addax baseline"
log "  distribution: $ADDAX_HOME"
log "  output:       $OUT_DIR"
log "  channel=$CHANNEL rows=$ROWS heap=$HEAP runs=$RUNS warmup=$WARMUP profile=$PROFILE"

if [ "$DO_SEED" -eq 1 ]; then
    seed_database
fi

write_manifest

failures=0
for case_name in $(list_cases); do
    log_step "$case_name"
    if [ "$WARMUP" -gt 0 ]; then
        run_one "$case_name" warmup 0 || failures=$((failures + 1))
    fi
    i=1
    while [ "$i" -le "$RUNS" ]; do
        run_one "$case_name" "run${i}" 0 || failures=$((failures + 1))
        i=$((i + 1))
    done
    if [ "$PROFILE" -eq 1 ]; then
        run_one "$case_name" profile 1 || failures=$((failures + 1))
    fi
done

write_summary
log "csv: $CSV"
if [ "$failures" -ne 0 ]; then
    warn "$failures run(s) failed; inspect logs/ before interpreting the medians"
    exit 1
fi
