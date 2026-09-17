#!/usr/bin/env bash
#
# Start the E2E databases and wait until they accept real queries.
#
# Plain `docker run` rather than docker compose: `docker compose` is not available
# on every developer machine, and the readiness check has to be written by hand
# anyway in order to print a useful message on timeout.
#
# Idempotent: an already-running container is reused (run.sh resets all data
# before every case), a stopped one is started, a missing one is created.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd -P)"
# shellcheck source=lib/common.sh
. "${SCRIPT_DIR}/lib/common.sh"
# shellcheck source=lib/db.sh
. "${SCRIPT_DIR}/lib/db.sh"
# shellcheck source=lib/runtime.sh
. "${SCRIPT_DIR}/lib/runtime.sh"

DBS="mysql postgres"
WAIT_TIMEOUT=180

usage() {
    cat <<'EOF'
Usage: start-db.sh [--dbs mysql,postgres] [--timeout seconds]

Starts the databases the E2E suite needs and waits for them to become ready.
EOF
}

while [ $# -gt 0 ]; do
    case "$1" in
        -d|--dbs) DBS="$(printf '%s' "$2" | tr ',' ' ')"; shift 2 ;;
        --timeout) WAIT_TIMEOUT="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) die "unknown option: $1 (try --help)" ;;
    esac
done

set -a
# shellcheck source=db.env
. "${SCRIPT_DIR}/db.env"
set +a

rt_detect
log "container runtime: $E2E_RUNTIME"

start_one() { # db
    local db="$1" container image
    container="$(db_container "$db")" || die "unknown database: $db"
    image="$(db_image "$db")" || die "unknown database: $db"

    if rt_is_running "$container"; then
        log "${db}: container ${container} already running"
    elif rt_exists "$container"; then
        log "${db}: starting existing container ${container}"
        rt start "$container" >/dev/null
    else
        log "${db}: creating container ${container} from ${image}"
        case "$db" in
            mysql)
                # $(rt_publish_flag ...) is unquoted on purpose: it is either the
                # two words `-p host:port:3306` or nothing at all (see runtime.sh).
                # shellcheck disable=SC2046
                rt run -d --name "$container" \
                    -e MYSQL_ROOT_PASSWORD="$MYSQL_PASSWORD" \
                    -e MYSQL_DATABASE="$MYSQL_DB" \
                    $(rt_publish_flag 3306) \
                    "$image" >/dev/null
                ;;
            postgres)
                # shellcheck disable=SC2046
                rt run -d --name "$container" \
                    -e POSTGRES_USER="$POSTGRES_USER" \
                    -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
                    -e POSTGRES_DB="$POSTGRES_DB" \
                    $(rt_publish_flag 5432) \
                    "$image" >/dev/null
                ;;
        esac
    fi

    db_wait "$db" "$WAIT_TIMEOUT" || die "$db is not ready (see container log above)"
}

for db in $DBS; do
    start_one "$db"
done

log "databases ready: $DBS"
