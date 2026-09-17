# shellcheck shell=bash
#
# Database client helpers. Everything goes through the client binaries inside the
# containers, so the runner (and a developer laptop) needs nothing but Docker.
#
# Values are passed as argv or stdin, never interpolated into a shell string, so
# data containing quotes or backslashes round-trips intact.

db_container() { # db key -> container name (non-zero if the key is unknown)
    case "$1" in
        mysql) printf '%s' "$MYSQL_CONTAINER" ;;
        postgres) printf '%s' "$POSTGRES_CONTAINER" ;;
        *) return 1 ;;
    esac
}

db_image() { # db key -> image reference
    case "$1" in
        mysql) printf '%s' "$MYSQL_IMAGE" ;;
        postgres) printf '%s' "$POSTGRES_IMAGE" ;;
        *) return 1 ;;
    esac
}

# Built on demand rather than in db.env: the host can change after db.env is
# sourced (Apple's container runtime substitutes the container address), and an
# explicitly exported *_JDBC_URL must still win over the derived one. No query
# string on purpose -- see the `-p` minefield note in lib/addax.sh.
db_jdbc_url() { # db
    case "$1" in
        mysql) printf '%s' "${MYSQL_JDBC_URL:-jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DB}}" ;;
        postgres) printf '%s' "${POSTGRES_JDBC_URL:-jdbc:postgresql://${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}}" ;;
        *) return 1 ;;
    esac
}

# Strip CR (the containers can emit them when tty handling varies) and drop empty
# lines. Deliberately does NOT trim trailing spaces: a trailing space in a value is
# real data, and silently trimming it would hide a whole class of writer bugs.
normalize() {
    tr -d '\r' | sed -e '/^[[:space:]]*$/d'
}

# A real query with the real credentials -- `mysqladmin ping` answers 0 even when
# authentication fails, which would let a broken password through as "ready".
db_ready() {
    local db="$1"
    case "$db" in
        mysql)
            rt exec -e MYSQL_PWD="$MYSQL_PASSWORD" "$MYSQL_CONTAINER" \
                mysql -h127.0.0.1 -u"$MYSQL_USER" -N -B -e 'select 1' >/dev/null 2>&1 </dev/null
            ;;
        postgres)
            rt exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$POSTGRES_CONTAINER" \
                psql -h127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -A -q -c 'select 1' >/dev/null 2>&1 </dev/null
            ;;
        *) return 1 ;;
    esac
}

db_wait() { # db, timeout seconds
    local db="$1" timeout="${2:-120}" waited=0 container
    container="$(db_container "$db")" || return 1
    printf 'waiting for %s' "$db"
    while [ "$waited" -lt "$timeout" ]; do
        if db_ready "$db"; then
            printf ' ready (%ss)\n' "$waited"
            return 0
        fi
        printf '.'
        sleep 2
        waited=$((waited + 2))
    done
    printf ' TIMEOUT\n'
    printf 'last 50 log lines from %s:\n' "$container" >&2
    rt_logs "$container" 50 >&2 2>&1 || true
    return 1
}

db_exec() { # db, sql (statement does not need to return rows)
    local db="$1" sql="$2"
    case "$db" in
        mysql)
            rt exec -e MYSQL_PWD="$MYSQL_PASSWORD" "$MYSQL_CONTAINER" \
                mysql -h127.0.0.1 -u"$MYSQL_USER" --default-character-set=utf8mb4 "$MYSQL_DB" -e "$sql" </dev/null
            ;;
        postgres)
            rt exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$POSTGRES_CONTAINER" \
                psql -h127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -q -c "$sql" </dev/null
            ;;
        *) return 1 ;;
    esac
}

db_load() { # db, sql file
    local db="$1" file="$2"
    case "$db" in
        mysql)
            rt exec -i -e MYSQL_PWD="$MYSQL_PASSWORD" "$MYSQL_CONTAINER" \
                mysql -h127.0.0.1 -u"$MYSQL_USER" --default-character-set=utf8mb4 "$MYSQL_DB" <"$file"
            ;;
        postgres)
            rt exec -i -e PGPASSWORD="$POSTGRES_PASSWORD" "$POSTGRES_CONTAINER" \
                psql -h127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -q -f - <"$file"
            ;;
        *) return 1 ;;
    esac
}

# One row per line, one column per query (verify queries build a single delimited
# column with CONCAT_WS / concat_ws so the comparison is a plain line diff).
#
# Note the </dev/null: only db_load attaches stdin. A client started with -i and no
# redirection inherits fd 0 from the caller, and in a `while read` loop that means
# it silently eats the runner's case list.
db_query() { # db, sql
    local db="$1" sql="$2"
    case "$db" in
        mysql)
            rt exec -e MYSQL_PWD="$MYSQL_PASSWORD" "$MYSQL_CONTAINER" \
                mysql -h127.0.0.1 -u"$MYSQL_USER" --default-character-set=utf8mb4 \
                -N -B --raw -e "$sql" "$MYSQL_DB" </dev/null
            ;;
        postgres)
            rt exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$POSTGRES_CONTAINER" \
                psql -h127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
                -t -A -q -v ON_ERROR_STOP=1 -c "$sql" </dev/null
            ;;
        *) return 1 ;;
    esac
    return 0
}

db_query_normalized() { # db, sql
    db_query "$1" "$2" | normalize
}

# Reset to the shared fixture. schema.sql drops and recreates every table, so this
# is idempotent from a cold container or a dirty one left over from the last run,
# and a fixture edit takes effect on the next run without a manual teardown.
db_reset() { # db
    local db="$1"
    local dir="$E2E_FIXTURES_DIR/$db"
    [ -d "$dir" ] || die "no fixture directory for database '$db' (expected $dir)"
    db_load "$db" "$dir/schema.sql"
    db_load "$db" "$dir/seed.sql"
}
