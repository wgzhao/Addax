# shellcheck shell=bash
#
# Assertions for case verify scripts. Each returns non-zero on failure and prints
# a readable reason; case scripts run under `set -e`, so the first failure ends the
# case and run.sh records it as FAIL.

assert_eq() { # actual, expected, what
    if [ "$1" != "$2" ]; then
        printf 'ASSERT FAILED: %s\n  actual:   [%s]\n  expected: [%s]\n' "$3" "$1" "$2" >&2
        return 1
    fi
}

assert_file_exists() { # file, what
    if [ ! -f "$1" ]; then
        printf 'ASSERT FAILED: %s: file does not exist: %s\n' "$2" "$1" >&2
        return 1
    fi
}

# Byte-exact comparison against a golden file. LC_ALL=C so the diff cannot be
# affected by the host locale.
assert_file_matches() { # actual_file, golden_file
    local actual="$1" golden="$2"
    if [ "${E2E_UPDATE_EXPECT:-0}" = 1 ]; then
        cp "$actual" "$golden" || return 1
        log "  updated golden: $(basename "$golden")"
        return 0
    fi
    if [ ! -f "$golden" ]; then
        printf 'ASSERT FAILED: golden file not found: %s\n' "$golden" >&2
        return 1
    fi
    if ! LC_ALL=C diff -u "$golden" "$actual"; then
        printf 'ASSERT FAILED: output differs from %s (- expected, + actual)\n' "$golden" >&2
        return 1
    fi
}

assert_files_equal() { # file_a, file_b, what
    if ! LC_ALL=C diff -u "$1" "$2"; then
        printf 'ASSERT FAILED: %s: %s and %s differ\n' "$3" "$1" "$2" >&2
        return 1
    fi
}

assert_contains() { # file, pattern, what
    if ! LC_ALL=C grep -q -- "$2" "$1"; then
        printf 'ASSERT FAILED: %s: %s does not contain [%s]\n' "$3" "$1" "$2" >&2
        return 1
    fi
}

# Run a query and compare with a golden file. Capture is kept next to the case
# work dir so a failure leaves the actual output behind as evidence.
assert_sql_matches() { # db, sql, golden_file
    local db="$1" sql="$2" golden="$3"
    local actual="${E2E_CASE_WORK}/actual/$(basename "$golden")"
    db_query_normalized "$db" "$sql" >"$actual"

    if [ "${E2E_UPDATE_EXPECT:-0}" = 1 ]; then
        cp "$actual" "$golden"
        log "  updated golden: $(basename "$golden")"
        return 0
    fi

    assert_file_matches "$actual" "$golden"
}

# The same query against two databases must produce the same rows.
assert_sql_equal() { # db_a, sql_a, db_b, sql_b, what
    local a="${E2E_CASE_WORK}/actual/$(printf '%s' "$1" | tr '/' '_').txt"
    local b="${E2E_CASE_WORK}/actual/$(printf '%s' "$3" | tr '/' '_').txt"
    db_query_normalized "$1" "$2" >"$a"
    db_query_normalized "$3" "$4" >"$b"
    assert_files_equal "$a" "$b" "$5"
}

assert_file_count() { # directory, glob, expected count, what
    local dir="$1" pattern="$2" want="$3" what="$4" got=0 f
    for f in "$dir"/$pattern; do
        [ -e "$f" ] || continue
        got=$((got + 1))
    done
    if [ "$got" -ne "$want" ]; then
        printf 'ASSERT FAILED: %s: expected %s file(s) matching %s in %s, found %s\n' \
            "$what" "$want" "$pattern" "$dir" "$got" >&2
        ls -la "$dir" >&2 || true
        return 1
    fi
}

# Concatenate every file matching a glob, sorted by name, and compare with a
# golden file. Used for file writers that split their output across part files.
assert_dir_matches() { # directory, glob, golden_file
    local dir="$1" pattern="$2" golden="$3"
    local actual="${E2E_CASE_WORK}/actual/$(basename "$golden")"
    : >"$actual"
    local f
    for f in $(LC_ALL=C ls -1 "$dir"/$pattern 2>/dev/null | sort); do
        cat "$f" >>"$actual"
    done

    if [ "${E2E_UPDATE_EXPECT:-0}" = 1 ]; then
        cp "$actual" "$golden"
        log "  updated golden: $(basename "$golden")"
        return 0
    fi

    assert_file_matches "$actual" "$golden"
}
