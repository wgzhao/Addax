# shellcheck shell=bash
#
# Container runtime abstraction.
#
# CI runs on GitHub-hosted Ubuntu runners, which have Docker. A developer laptop
# may have Docker Desktop, OrbStack, or Apple's own `container` tool -- and on a
# machine with no Docker daemon at all, `container` is the one that works.
#
# The CLIs agree on `run -d --name -e -p`, `exec -i -e`, `start` and `rm -f`, so
# `rt` is a plain passthrough for those. They disagree on log tailing and on
# listing/filtering, which is why the wrappers below exist.
#
# Override detection with E2E_RUNTIME=docker|container.

E2E_RUNTIME="${E2E_RUNTIME:-}"

# `rt` is not a name for `docker`; it is whichever runtime was selected.
rt() {
    case "$E2E_RUNTIME" in
        docker) docker "$@" ;;
        container) container "$@" ;;
        *) die "no container runtime selected (internal error)" ;;
    esac
}

rt_usable() { # runtime name -> 0 when it can actually run containers right now
    case "$1" in
        docker)
            command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1
            ;;
        container)
            # The CLI alone is not enough: the background apiserver has to be up,
            # otherwise every command fails with an opaque bootstrap error.
            command -v container >/dev/null 2>&1 &&
                container system status 2>/dev/null | grep -q 'apiserver is running'
            ;;
        *)
            return 1
            ;;
    esac
}

rt_detect() {
    if [ -n "$E2E_RUNTIME" ]; then
        rt_usable "$E2E_RUNTIME" || die "E2E_RUNTIME=$E2E_RUNTIME is not usable.
Docker:    start Docker Desktop / OrbStack / colima
container: container system start"
        return 0
    fi

    # Docker first: it is what CI uses, so a machine that can run both should test
    # the same way CI does. `docker info` (not `command -v`) because a CLI with no
    # daemon behind it is a very common state on macOS.
    if rt_usable docker; then
        E2E_RUNTIME=docker
    elif rt_usable container; then
        E2E_RUNTIME=container
    else
        cat >&2 <<'EOF'
ERROR: no usable container runtime found.

Install or start one of:
  - Docker Desktop / OrbStack / colima   (what CI uses)
  - Apple's `container`                  (container system start)

Then re-run, or select explicitly with E2E_RUNTIME=docker|container.
EOF
        exit 1
    fi
}

rt_logs() { # container, lines
    case "$E2E_RUNTIME" in
        docker) docker logs --tail "$2" "$1" ;;
        container) container logs -n "$2" "$1" ;;
    esac
}

# Names of containers, running or not. Docker can filter server side; Apple's CLI
# cannot (and `container inspect` exits 0 even for a name that does not exist, so
# it is useless as an existence test).
rt_all_names() {
    case "$E2E_RUNTIME" in
        docker) docker ps -a --format '{{.Names}}' ;;
        container) container ls -a --format json 2>/dev/null |
            grep -o '"id":"[^"]*"' | cut -d'"' -f4 || true ;;
    esac
}

rt_running_names() {
    case "$E2E_RUNTIME" in
        docker) docker ps --format '{{.Names}}' ;;
        container) container ls --format json 2>/dev/null |
            grep -o '"id":"[^"]*"' | cut -d'"' -f4 || true ;;
    esac
}

rt_exists() { # container
    rt_all_names | grep -qx -- "$1"
}

rt_container_ip() { # container -> the address the host can reach it on, or nothing
    case "$E2E_RUNTIME" in
        docker) return 1 ;;
        container)
            # The JSON escapes the CIDR separator ("192.168.64.3\/24"), so match the
            # address itself rather than trying to strip the suffix off the raw field.
            container inspect "$1" 2>/dev/null |
                grep -o '"ipv4Address":"[0-9.]*' | head -n 1 | cut -d'"' -f4
            ;;
    esac
}

# Where the suite should point a JDBC client.
#
# Docker publishes container ports on the host, so everything talks to 127.0.0.1.
#
# Apple's `container` (v0.8.0) publishes a port that accepts a TCP connection and
# then silently drops it -- `nc -z` reports the port open while no protocol ever
# gets through, for both `-p 13306:3306` and `-p 127.0.0.1:13306:3306`. The
# container's own address on the vmnet bridge is directly routable though, and
# works, so on that runtime the suite skips publishing entirely and connects there
# on the port the server actually listens on.
#
# Must run before db.env is sourced: db.env derives the JDBC URLs from these.
rt_apply_endpoints() {
    [ "$E2E_RUNTIME" = container ] || return 0

    # An explicitly configured host wins: the substitution only applies while the
    # value is still the loopback default, so pointing the suite at your own
    # database keeps working on every runtime.
    local ip
    if [ -z "${MYSQL_HOST:-}" ] || [ "${MYSQL_HOST}" = '127.0.0.1' ]; then
        ip="$(rt_container_ip "$MYSQL_CONTAINER" || true)"
        if [ -n "$ip" ]; then
            export MYSQL_HOST="$ip"
            export MYSQL_PORT=3306
        fi
    fi
    if [ -z "${POSTGRES_HOST:-}" ] || [ "${POSTGRES_HOST}" = '127.0.0.1' ]; then
        ip="$(rt_container_ip "$POSTGRES_CONTAINER" || true)"
        if [ -n "$ip" ]; then
            export POSTGRES_HOST="$ip"
            export POSTGRES_PORT=5432
        fi
    fi
}

# Empty for runtimes that publish properly, so the caller's `$(...)` disappears.
rt_publish_flag() { # container port
    [ "$E2E_RUNTIME" = docker ] || return 0
    case "$1" in
        3306) printf -- '-p %s:%s:3306' "$MYSQL_HOST" "$MYSQL_PORT" ;;
        5432) printf -- '-p %s:%s:5432' "$POSTGRES_HOST" "$POSTGRES_PORT" ;;
    esac
}

rt_is_running() { # container
    rt_running_names | grep -qx -- "$1"
}
