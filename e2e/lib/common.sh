# shellcheck shell=bash
#
# Shared helpers: logging, CI annotations, case discovery, result reporting.
# Sourced by run.sh and by every case verify script.
#
# Every script in e2e/ must stay bash 3.2 compatible -- that is what ships with macOS,
# and the suite has to run on a developer laptop as well as on Ubuntu CI (bash 5).
# That rules out: associative arrays, mapfile/readarray, ${var,,}, and `local -n`.

set -euo pipefail

E2E_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
E2E_DIR="$(dirname "$E2E_LIB_DIR")"
E2E_CASES_DIR="${E2E_DIR}/cases"
E2E_FIXTURES_DIR="${E2E_DIR}/fixtures"
E2E_WORK_DIR="${E2E_WORK_DIR:-${TMPDIR:-/tmp}/addax-e2e}"

log() { printf '%s\n' "$*"; }

log_step() { printf '\n=== %s\n' "$*"; }

warn() { printf 'WARNING: %s\n' "$*" >&2; }

die() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}

# Emit a GitHub Actions error annotation so the failure shows up on the PR page,
# not only in the raw log. No-op outside CI.
gh_error() { # title, message
    if [ -n "${GITHUB_ACTIONS:-}" ]; then
        printf '::error title=%s::%s\n' "$1" "$2"
    fi
}

gh_notice() {
    if [ -n "${GITHUB_ACTIONS:-}" ]; then
        printf '::notice title=%s::%s\n' "$1" "$2"
    fi
}

# Read one variable out of a case.env without leaking the rest into the caller.
read_case_env() { # case_dir, var_name
    (
        # shellcheck source=/dev/null
        . "$1/case.env"
        eval "printf '%s' \"\${$2:-}\""
    )
}

# Human-readable description of how a case proves itself, for the summary table.
describe_verify() { # case_dir
    if [ -f "$1/verify.sh" ]; then
        printf 'verify.sh'
    else
        local dbs="" f
        for f in "$1"/verify.*.sql; do
            [ -e "$f" ] || continue
            dbs="${dbs}${dbs:+,}$(basename "$f" .sql | sed -e 's/^verify\.//')"
        done
        printf 'sql(%s)' "$dbs"
    fi
}

# --- result bookkeeping ------------------------------------------------------
# Indexed arrays only; `${arr[@]}` on an empty array is an unbound-variable error
# under `set -u` in bash 3.2, so every loop guards on the length first.

CASE_NAMES=()
CASE_RESULTS=()
CASE_SECONDS=()
CASE_VERIFIES=()

record_result() { # name, result, seconds, verify
    CASE_NAMES[${#CASE_NAMES[@]}]="$1"
    CASE_RESULTS[${#CASE_RESULTS[@]}]="$2"
    CASE_SECONDS[${#CASE_SECONDS[@]}]="$3"
    CASE_VERIFIES[${#CASE_VERIFIES[@]}]="$4"
}

print_summary() {
    local total=${#CASE_NAMES[@]} passed=0 failed=0 xfailed=0 xpassed=0 i name

    if [ "$total" -eq 0 ]; then
        log "no cases ran"
        return 0
    fi

    printf '\n%-34s %-16s %4s %s\n' 'CASE' 'VERIFY' 'TIME' 'RESULT'
    printf '%s\n' '-------------------------------------------------------------------------'
    for ((i = 0; i < total; i++)); do
        case "${CASE_RESULTS[$i]}" in
            PASS) passed=$((passed + 1)) ;;
            XFAIL) xfailed=$((xfailed + 1)) ;;
            XPASS) xpassed=$((xpassed + 1)) ;;
            *) failed=$((failed + 1)) ;;
        esac
        name="${CASE_NAMES[$i]}"
        [ ${#name} -gt 34 ] && name="${name:0:31}..."
        printf '%-34s %-16s %3ss %s\n' \
            "$name" "${CASE_VERIFIES[$i]}" "${CASE_SECONDS[$i]}" "${CASE_RESULTS[$i]}"
    done
    printf '%s\n' '-------------------------------------------------------------------------'
    printf '%d run, %d passed, %d failed' "$total" "$passed" "$failed"
    [ "$xfailed" -gt 0 ] && printf ', %d expected-failure' "$xfailed"
    [ "$xpassed" -gt 0 ] && printf ', %d unexpected-pass' "$xpassed"
    printf '\n'

    if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
        write_step_summary "$total" "$passed" "$failed" "$xfailed" "$xpassed"
    fi

    # A case that starts passing while still marked as an expected failure is a
    # failure too: the bug is gone and the marker has to go with it.
    [ "$failed" -eq 0 ] && [ "$xpassed" -eq 0 ]
}

# The E2E job is not a required check, so the step summary is the main way a
# reviewer notices a red case without opening the log.
write_step_summary() { # total, passed, failed, xfailed, xpassed
    {
        printf '## Addax E2E\n\n'
        if [ "$3" -eq 0 ] && [ "$5" -eq 0 ]; then
            printf '**%d** cases passed' "$2"
            if [ "$4" -gt 0 ]; then
                printf ' (**%d** expected failure%s, all still failing)' "$4" \
                    "$([ "$4" -eq 1 ] && printf '' || printf 's')"
            fi
            printf '.\n\n'
        else
            printf '**%d of %d** cases failed.\n\n' "$3" "$1"
        fi
        if [ "$5" -gt 0 ]; then
            printf '**%d** case(s) marked as an expected failure now PASS. Remove the\n' "$5"
            printf '`EXPECTED_FAIL` marker from `case.env` so the fix is protected.\n\n'
        fi
        printf '| Case | Verify | Time | Result |\n|---|---|---|---|\n'
        local i
        for ((i = 0; i < $1; i++)); do
            printf '| `%s` | %s | %ss | %s |\n' \
                "${CASE_NAMES[$i]}" "${CASE_VERIFIES[$i]}" "${CASE_SECONDS[$i]}" "${CASE_RESULTS[$i]}"
        done
        printf '\n'
    } >>"$GITHUB_STEP_SUMMARY"
}

require_commands() {
    local missing="" cmd
    for cmd in "$@"; do
        command -v "$cmd" >/dev/null 2>&1 || missing="${missing}${missing:+, }${cmd}"
    done
    [ -z "$missing" ] || die "missing required command(s): $missing"
}
