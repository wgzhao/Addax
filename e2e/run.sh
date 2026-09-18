#!/usr/bin/env bash
#
# Addax plugin E2E runner.
#
# Drives the packaged distribution (bin/addax.sh + the plugin/ tree) against
# throwaway databases, using the same job config files a user would write.
#
#   ./e2e/run.sh                          # every case
#   ./e2e/run.sh --dbs mysql '040*'       # only cases whose DBS is a subset of the filter
#   ./e2e/run.sh --list                   # what would run
#   ./e2e/run.sh --update-expect '060*'   # rewrite goldens (review the diff!)
#
# Adding a case means adding a directory under cases/ -- nothing here needs editing.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd -P)"
# shellcheck source=lib/common.sh
. "${SCRIPT_DIR}/lib/common.sh"
# shellcheck source=lib/db.sh
. "${SCRIPT_DIR}/lib/db.sh"
# shellcheck source=lib/assert.sh
. "${SCRIPT_DIR}/lib/assert.sh"
# shellcheck source=lib/addax.sh
. "${SCRIPT_DIR}/lib/addax.sh"
# shellcheck source=lib/runtime.sh
. "${SCRIPT_DIR}/lib/runtime.sh"

usage() {
    cat <<'EOF'
Usage: run.sh [options] [case-glob ...]

Options:
  -d, --dbs <list>     databases available this run (comma or space separated).
                       A case runs only if every database it needs is listed.
                       Default: no filtering -- every case runs.
  -l, --list           list the cases that would run, then exit
      --update-expect  rewrite expect*.txt from actual output (review the diff!)
  -h, --help           this message

Environment:
  ADDAX_HOME           distribution to test (default: newest ../target/addax-*)
  E2E_WORK_DIR         where per-case logs and evidence go
                       (default: ${TMPDIR:-/tmp}/addax-e2e)
  E2E_JOB_TIMEOUT      per-job timeout in seconds, when `timeout` is available
EOF
}

DB_FILTER=""
LIST_ONLY=0
CASE_GLOBS=()

while [ $# -gt 0 ]; do
    case "$1" in
        -d|--dbs) DB_FILTER="$(printf '%s' "$2" | tr ',' ' ')"; shift 2 ;;
        -l|--list) LIST_ONLY=1; shift ;;
        --update-expect) E2E_UPDATE_EXPECT=1; shift ;;
        -h|--help) usage; exit 0 ;;
        -*) die "unknown option: $1 (try --help)" ;;
        *) CASE_GLOBS[${#CASE_GLOBS[@]}]="$1"; shift ;;
    esac
done
export E2E_UPDATE_EXPECT="${E2E_UPDATE_EXPECT:-0}"

# --- case selection ----------------------------------------------------------

case_matches_glob() { # case_name
    [ ${#CASE_GLOBS[@]} -gt 0 ] || return 0
    local g
    for g in "${CASE_GLOBS[@]}"; do
        # shellcheck disable=SC2254 # glob is intentional
        case "$1" in $g) return 0 ;; esac
    done
    return 1
}

case_dbs_available() { # case_dbs
    [ -n "$DB_FILTER" ] || return 0
    local db
    for db in $1; do
        case " ${DB_FILTER} " in
            *" ${db} "*) ;;
            *) return 1 ;;
        esac
    done
    return 0
}

select_cases() { # prints one case directory per line
    local dir name dbs
    for dir in "${E2E_CASES_DIR}"/*/; do
        [ -d "$dir" ] || continue
        dir="${dir%/}"
        name="$(basename "$dir")"
        case_matches_glob "$name" || continue
        dbs="$(read_case_env "$dir" DBS)"
        case_dbs_available "$dbs" || continue
        printf '%s\n' "$dir"
    done
}

# --- running one case --------------------------------------------------------

verify_case() { # case_dir
    local case_dir="$1" f db golden actual found=0

    if [ -f "$case_dir/verify.sh" ]; then
        ( . "$case_dir/verify.sh" ) || die "verify.sh failed"
        return 0
    fi

    for f in "$case_dir"/verify.*.sql; do
        [ -e "$f" ] || continue
        found=1
        db="$(basename "$f" .sql | sed -e 's/^verify\.//')"
        db_container "$db" >/dev/null || die "verify.$db.sql names an unknown database"

        golden="$case_dir/expect.$db.txt"
        [ -f "$golden" ] || golden="$case_dir/expect.txt"
        [ -f "$golden" ] || die "no golden file for verify.$db.sql (expected expect.$db.txt or expect.txt)"

        actual="$E2E_CASE_WORK/actual/verify.$db.txt"
        db_query_normalized "$db" "$(cat "$f")" >"$actual"

        if [ "$E2E_UPDATE_EXPECT" = 1 ]; then
            cp "$actual" "$golden"
            log "  updated golden: $(basename "$golden")"
        else
            assert_file_matches "$actual" "$golden" \
                || die "verify.$db.sql output does not match $(basename "$golden")"
        fi
    done

    [ "$found" -eq 1 ] || die "case has no verify.sh and no verify.<db>.sql"
    return 0
}

run_case() { # case_dir -- runs in a subshell, so a failure cannot take the suite down
    local case_dir="$1" name db job
    name="$(basename "$case_dir")"

    E2E_CASE_WORK="${E2E_WORK_DIR}/${name}"
    rm -rf "$E2E_CASE_WORK"
    mkdir -p "$E2E_CASE_WORK/logs" "$E2E_CASE_WORK/out" "$E2E_CASE_WORK/actual"
    export E2E_CASE_WORK
    export E2E_CASE_OUT="${E2E_CASE_WORK}/out"
    export CASE_DIR="$case_dir"
    export CASE_NAME="$name"

    [ -f "$case_dir/case.env" ] ||
        die "$case_dir has no case.env. Every case directory needs one (at minimum
DBS=...); a directory without it is not a case."

    cp "$case_dir/case.env" "$E2E_CASE_WORK/case.env"

    # shellcheck source=/dev/null
    . "$case_dir/case.env"
    # declare, not populate: an embedded database (DuckDB, SQLite) is a file the case
    # builds itself, so DBS is legitimately empty and no container is involved
    : "${DBS?case.env must define DBS}"
    SRC_DB="${SRC_DB:-}"
    DST_DB="${DST_DB:-}"
    export DBS SRC_DB DST_DB

    log_step "${name}   [DBS=${DBS}]"

    for db in $DBS; do
        db_container "$db" >/dev/null || die "case.env lists an unknown database: $db"
        db_reset "$db"
    done

    if [ -f "$case_dir/setup.sh" ]; then
        ( . "$case_dir/setup.sh" ) || die "setup.sh failed"
    fi

    build_addax_params
    # Written here rather than at evidence-collection time: ADDAX_PARAMS only exists
    # inside this subshell, and the resolved values are the first thing you need when
    # a job fails (the ${placeholder} substitution happens in memory, never on disk).
    printf 'ADDAX_HOME=%s\nADDAX_PARAMS=%s\n' "$ADDAX_HOME" "$ADDAX_PARAMS" \
        >"$E2E_CASE_WORK/addax-params.txt"

    for job in "$case_dir"/job*.json; do
        [ -e "$job" ] || continue
        run_addax_job "$job" "$E2E_CASE_WORK/logs" || return 1
    done

    verify_case "$case_dir"
}

# --- main --------------------------------------------------------------------

# Listing needs neither a runtime nor a distribution -- it is the one command that
# works before anything has been started or built.
if [ "$LIST_ONLY" = 1 ]; then
    select_cases | while IFS= read -r dir; do
        printf '%-34s DBS=%s  %s\n' "$(basename "$dir")" \
            "$(read_case_env "$dir" DBS)" "$(describe_verify "$dir")"
    done
    exit 0
fi

set -a
# shellcheck source=db.env
. "${SCRIPT_DIR}/db.env"
set +a

# After db.env (which supplies the container names) and before any JDBC URL is
# built: the container runtime reaches its databases at the container's own
# address rather than through a published port.
rt_detect
rt_apply_endpoints

ADDAX_HOME="$(resolve_addax_home)" || die "no Addax distribution found.
Build one first:
  mvn -B -T 1C clean package -DskipTests -Dgpg.skip=true
  mvn -B -T 1 package -Pdistribution -DskipTests -Dgpg.skip=true
or point ADDAX_HOME at an existing distribution."
ADDAX_HOME="$(cd "$ADDAX_HOME" && pwd -P)"
export ADDAX_HOME

mkdir -p "$E2E_WORK_DIR"

for db in $DB_FILTER; do
    db_container "$db" >/dev/null || die "--dbs lists an unknown database: $db"
    db_ready "$db" || die "$db is not reachable. Start the databases first:
  ./e2e/start-db.sh --dbs ${DB_FILTER// /,}"
done

CASE_LIST_FILE="${E2E_WORK_DIR}/cases.txt"
select_cases >"$CASE_LIST_FILE"

if [ ! -s "$CASE_LIST_FILE" ]; then
    die "no cases selected (globs: ${CASE_GLOBS[*]:-<none>}, dbs: ${DB_FILTER:-<any>})"
fi

log "ADDAX_HOME: $ADDAX_HOME"
log "runtime:    $E2E_RUNTIME"
log "work dir:   $E2E_WORK_DIR"

# The case list is read from fd 3, not fd 0: anything a case runs (a database client,
# a job reader) may read stdin, and on fd 0 that would truncate the loop after the
# first case with no error anywhere.
while IFS= read -r case_dir <&3; do
    name="$(basename "$case_dir")"
    expected_fail="$(read_case_env "$case_dir" EXPECTED_FAIL)"
    start=$SECONDS
    result=PASS
    if ! ( run_case "$case_dir" ); then
        if [ -n "$expected_fail" ]; then result=XFAIL; else result=FAIL; fi
    elif [ -n "$expected_fail" ]; then
        result=XPASS
    fi
    elapsed=$((SECONDS - start))

    if [ "$result" = XFAIL ]; then
        log "  XFAIL (known defect): ${expected_fail}"
    elif [ "$result" = XPASS ]; then
        log "  XPASS: this case is marked EXPECTED_FAIL but passed -- the defect is fixed,"
        log "         remove EXPECTED_FAIL from ${name}/case.env so the fix stays protected."
        gh_error "$name" "expected failure now passes -- remove the EXPECTED_FAIL marker"
    fi

    if [ "$result" = FAIL ]; then
        gh_error "$name" "E2E case failed"
        ( collect_evidence "$name" "$case_dir" ) || warn "could not collect evidence for $name"
    fi
    record_result "$name" "$result" "$elapsed" "$(describe_verify "$case_dir")"
done 3<"$CASE_LIST_FILE"

print_summary
