#!/usr/bin/env bash
#
# Remove the E2E database containers. Data is thrown away on purpose -- every case
# resets the fixture before it runs, so nothing here is worth preserving.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd -P)"
# shellcheck source=lib/common.sh
. "${SCRIPT_DIR}/lib/common.sh"
# shellcheck source=lib/db.sh
. "${SCRIPT_DIR}/lib/db.sh"
# shellcheck source=lib/runtime.sh
. "${SCRIPT_DIR}/lib/runtime.sh"

DBS="mysql postgres"

while [ $# -gt 0 ]; do
    case "$1" in
        -d|--dbs) DBS="$(printf '%s' "$2" | tr ',' ' ')"; shift 2 ;;
        -h|--help) log "Usage: stop-db.sh [--dbs mysql,postgres]"; exit 0 ;;
        *) die "unknown option: $1" ;;
    esac
done

set -a
# shellcheck source=db.env
. "${SCRIPT_DIR}/db.env"
set +a

rt_detect

for db in $DBS; do
    container="$(db_container "$db")" || die "unknown database: $db"
    if rt_exists "$container"; then
        rt rm -f "$container" >/dev/null
        log "${db}: removed ${container}"
    else
        log "${db}: no container ${container}"
    fi
done
