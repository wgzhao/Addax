# shellcheck shell=bash
#
# Locating and driving the Addax distribution.
#
# Everything here runs the real thing: the packaged distribution under target/,
# the real bin/addax.sh launcher, and real job config files. That is the whole
# point -- plugin resolution, plugin.json lookup and the libs/ classloader path
# are only exercised by the packaged layout, never by a unit test.

# ADDAX_HOME resolution: explicit env var wins, otherwise the newest local build.
# The fallback exists for the laptop workflow; CI always sets it explicitly.
resolve_addax_home() {
    if [ -n "${ADDAX_HOME:-}" ]; then
        if [ ! -x "${ADDAX_HOME}/bin/addax.sh" ]; then
            die "ADDAX_HOME=$ADDAX_HOME does not contain an executable bin/addax.sh"
        fi
        printf '%s' "$ADDAX_HOME"
        return 0
    fi

    local d best=""
    for d in "$E2E_DIR"/../target/addax-*/; do
        [ -d "$d" ] || continue
        [ -x "${d}bin/addax.sh" ] || continue
        if [ -z "$best" ] || [ "${d%/}" -nt "$best" ]; then
            best="${d%/}"
        fi
    done

    [ -n "$best" ] || return 1
    warn "using local build: $best  (may be stale -- set ADDAX_HOME to point elsewhere)" >&2
    printf '%s' "$best"
}

# addax.sh expands -p through `sh -c` WITHOUT quoting the value, so anything with
# shell meaning in it would be interpreted: a '&' backgrounds the JVM, a ';' starts
# a new command, a space splits the argument. Reject rather than surprise.
#
# The allowlist is what a JDBC URL, host, port, user, password or path needs --
# nothing more. A value that fails this needs a different transport, not a looser
# check; see e2e/README.md.
assert_param_safe() { # name, value
    case "$2" in
        *[!A-Za-z0-9_.:/@%+,=-]*)
            die "unsafe character in -p value for '$1': [$2]
Values are passed to addax.sh as a single -p argument and expanded inside \`sh -c\`,
so only [A-Za-z0-9_.:/@%+,=-] is allowed. See e2e/README.md ('the -p minefield')."
            ;;
    esac
    [ -n "$2" ] || die "empty -p value for '$1' (an undefined \${placeholder} would be left literally in the job file)"
}

ADDAX_PARAMS=""

append_param() { # name, value  (name must be [A-Za-z0-9_]+ -- a dot terminates the match)
    assert_param_safe "$1" "$2"
    ADDAX_PARAMS="${ADDAX_PARAMS}${ADDAX_PARAMS:+ }-D$1=$2"
}

build_addax_params() {
    ADDAX_PARAMS=""
    append_param mysql_host "$MYSQL_HOST"
    append_param mysql_port "$MYSQL_PORT"
    append_param mysql_user "$MYSQL_USER"
    append_param mysql_password "$MYSQL_PASSWORD"
    append_param mysql_db "$MYSQL_DB"
    append_param mysql_jdbc_url "$(db_jdbc_url mysql)"

    append_param postgres_host "$POSTGRES_HOST"
    append_param postgres_port "$POSTGRES_PORT"
    append_param postgres_user "$POSTGRES_USER"
    append_param postgres_password "$POSTGRES_PASSWORD"
    append_param postgres_db "$POSTGRES_DB"
    append_param postgres_jdbc_url "$(db_jdbc_url postgres)"

    append_param e2e_out_dir "$E2E_CASE_OUT"

    # The S3 test double the object store cases run against. Its port is the same variable
    # the case setup starts it on, so the job and the server cannot drift apart.
    append_param s3_endpoint "http://127.0.0.1:${E2E_MOTO_PORT:-5111}"

    # The elasticsearch cluster the case reads from. There is no throwaway cluster in this
    # suite, so it is whatever E2E_ES_ENDPOINT points at (the same variable setup.sh checks
    # the case's reachability with, and the one the README documents).
    append_param es_endpoint "${E2E_ES_ENDPOINT:-http://127.0.0.1:9200}"
}

# Run one job file. The job path is a positional argument: `-job x.json` is NOT
# supported (getopt consumes it as `-j ob`, silently injecting a bogus JVM option).
# Exit codes from Engine.main: 0 success, 1 bad CLI, 2 job failure.
# Runs one SQL statement against a DuckDB database file.
#
# DuckDB is embedded, so there is no server for start-db.sh to bring up and no fixture
# directory to seed: a case that needs a table has to build the database file itself.
# The table also has to exist before the job starts -- CommonRdbmsWriter.Job.init reads
# its column metadata during pretreatment, while preSql only runs later, in the task.
#
# The statement is executed through the driver shipped in the distribution, so the setup
# never depends on a duckdb CLI being installed. The scratch source file is reused across
# calls within a case; JDK 17, which the suite already requires, compiles it on the fly.
addax_duckdb_sql() { # database_file, sql
    local db="$1" sql="$2" src
    src="${E2E_CASE_WORK:-${TMPDIR:-/tmp}}/DuckDbSql.java"
    if [ ! -f "$src" ]; then
        cat >"$src" <<'JAVA'
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class DuckDbSql
{
    public static void main(String[] args) throws Exception
    {
        try (Connection conn = DriverManager.getConnection(args[0]);
                Statement stmt = conn.createStatement()) {
            stmt.execute(args[1]);
        }
    }
}
JAVA
    fi
    java --class-path "${ADDAX_HOME}/plugin/writer/duckdbwriter/libs/*" "$src" \
        "jdbc:duckdb:${db}" "$sql" || die "failed to run against $db: $sql"
}

run_addax_job() { # job_file, log_dir
    local job="$1" logdir="$2"
    local base out rc=0
    base="$(basename "$job" .json)"
    out="$logdir/${base}.console.log"

    # CASE_JVM_ARGS is passed as one -j argument and expanded by addax.sh inside `sh -c`,
    # so a case must put its whole option string there (spaces and all). The elasticsearch
    # case uses it to switch off the JVM proxy: a machine behind a system proxy would
    # otherwise route the job's HTTP requests through it.
    #
    # Only when a case asks for it: -j replaces the launcher's own default JVM options.
    # The ${a[@]+...} form is what keeps an empty array from tripping `set -u`, which
    # bash 3.2 (macOS /bin/bash) treats as an unbound variable.
    local jvm_args=()
    [ -n "${CASE_JVM_ARGS:-}" ] && jvm_args=(-j "$CASE_JVM_ARGS")

    # TZ pins the JVM default timezone, which is what decides the wall clock for
    # every timestamp the RDBMS path handles (the reader converts through
    # Calendar.getInstance(), the drivers render in the connection's zone) and which
    # TimestampColumn.asString() uses when a file writer has no dateFormat. Without
    # it the same job produces different text on a laptop (Asia/Shanghai) and on a
    # CI runner (UTC), and the goldens would only be valid on one of them.
    #
    # A case can override it with CASE_TZ in case.env -- the timezone *aware* cases do
    # exactly that, because their whole point is that the instant survives a zone
    # change while the rendered wall clock does not.
    local tz="${CASE_TZ:-UTC}"

    log "  addax.sh ${base}  (TZ=${tz}${CASE_JVM_ARGS:+  JVM=${CASE_JVM_ARGS}})"
    if command -v timeout >/dev/null 2>&1; then
        TZ="$tz" timeout "${E2E_JOB_TIMEOUT:-600}" "$ADDAX_HOME/bin/addax.sh" \
            ${jvm_args[@]+"${jvm_args[@]}"} -p"$ADDAX_PARAMS" -l "$logdir" -L info "$job" >"$out" 2>&1 || rc=$?
    else
        TZ="$tz" "$ADDAX_HOME/bin/addax.sh" \
            ${jvm_args[@]+"${jvm_args[@]}"} -p"$ADDAX_PARAMS" -l "$logdir" -L info "$job" >"$out" 2>&1 || rc=$?
    fi

    if [ "$rc" -ne 0 ]; then
        printf 'ERROR: job %s failed with exit code %s. Last 40 lines of %s:\n' \
            "$base" "$rc" "$out" >&2
        tail -40 "$out" >&2
        return 1
    fi
    return 0
}

# A CI failure is only actionable if it says why. Collect everything a human would
# otherwise have to reproduce by hand.
collect_evidence() { # case_name, case_dir
    # One assignment per statement on purpose: bash expands every right-hand side
    # in a single `local` before assigning any of them, so `local a=$1 b=$a` reads
    # `a` from the calling scope -- and under `set -u` that is an unbound variable.
    local case_name="$1"
    local case_dir="$2"
    local work="$E2E_WORK_DIR/$case_name"
    local dest="$work/evidence"
    mkdir -p "$dest"

    # The case definition (job configs, fixture hooks, goldens) is copied alongside
    # the logs: a job failure is only reproducible if you know exactly what ran.
    cp "$case_dir"/case.env "$case_dir"/job*.json "$dest/" 2>/dev/null || true
    cp "$case_dir"/setup.sh "$case_dir"/verify.sh "$case_dir"/verify.*.sql \
        "$case_dir"/expect*.txt "$dest/" 2>/dev/null || true
    cp -R "$work/logs" "$dest/" 2>/dev/null || true
    cp -R "$work/actual" "$dest/" 2>/dev/null || true

    # addax writes a per-run log under ${ADDAX_HOME}/log; grab anything from the
    # last five minutes so a failure before -l took effect is still covered.
    mkdir -p "$dest/addax-log"
    find "${ADDAX_HOME:-/nonexistent}/log" -type f -name 'addax_*.log' -mmin -5 \
        -exec cp {} "$dest/addax-log/" \; 2>/dev/null || true

    log "  evidence: $dest"
}
