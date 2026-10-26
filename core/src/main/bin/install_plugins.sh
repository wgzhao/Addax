#!/bin/bash
#
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
#

# install plugins from Maven repositories

set -u
set -o pipefail

SCRIPT_PATH="$({
    cd -- "$(dirname "$0")" >/dev/null 2>&1
    pwd -P
})"

ADDAX_HOME="$(dirname "$SCRIPT_PATH")"
VERSION=""
OFFLINE=0
FORCE=0
MAVEN_CMD=""
JAR_CMD=""
UNZIP_CMD=""
MAVEN_BOOTSTRAP_VERSION="3.9.9"
MAVEN_QUIET=1
MAVEN_STRICT=0
if [ -t 2 ]; then
    MAVEN_QUIET=0
fi

PLUGINS=()
INSTALLED=0
SKIPPED=0
FAILED=0
FAILED_LIST=""

STAGE_DIR=""
TMP_DIR=""
MAVEN_TMP_DIR=""
LOCK_DIR=""
LOCK_OWNED=0
REPLACE_TARGET=""
REPLACE_BACKUP=""

err() {
    printf '%s\n' "$*" >&2
}

warn() {
    printf 'Warning: %s\n' "$*" >&2
}

usage() {
    cat <<EOF
Usage: $0 [-o|--offline] [-f|--force] [-v version] <plugin1 plugin2 ...>
Options:
  -o, --offline  Run in offline mode and disable Maven bootstrap download
  -f, --force    Reinstall even when the same version is already installed
  -v version     Plugin version (default: detected from the addax-core jar)
  -h, --help     Show this help
Examples:
  $0 mysqlreader mysqlwriter
  $0 -v 6.2.0 hdfswriter
EOF
}

help() {
    usage
    exit 0
}

usage_error() {
    err "Error: $*"
    usage >&2
    exit 1
}

command_exists() {
    command -v "$1" >/dev/null 2>&1
}

create_tmpdir() {
    if command_exists mktemp; then
        mktemp -d 2>/dev/null && return 0
        mktemp -d -t addax-plugin 2>/dev/null && return 0
    fi
    return 1
}

run_maven() {
    local -a args

    args=()
    if [ "$OFFLINE" -eq 1 ]; then
        args+=(-o)
    fi
    # Strict checksums are safe for released artifacts but not for snapshots
    # or private repositories that do not publish checksums.
    if [ "$MAVEN_STRICT" -eq 1 ] && [ "$OFFLINE" -eq 0 ]; then
        args+=(-C)
    fi
    if [ "$MAVEN_QUIET" -eq 1 ]; then
        args+=(-q)
    fi
    args+=("$@")
    "$MAVEN_CMD" "${args[@]}"
}

run_maven_logged() {
    local log="$1"
    shift

    if [ "$MAVEN_QUIET" -eq 1 ]; then
        run_maven "$@" >"$log" 2>&1
    else
        run_maven "$@" 2>&1 | tee "$log"
    fi
}

download_file() {
    local url="$1"
    local dest="$2"
    local max_time="${3:-600}"

    if command_exists curl; then
        if [ -t 2 ]; then
            curl -fL --progress-bar --retry 3 --retry-delay 2 --connect-timeout 10 --max-time "$max_time" "$url" -o "$dest"
        else
            curl -fsSL --retry 3 --retry-delay 2 --connect-timeout 10 --max-time "$max_time" "$url" -o "$dest"
        fi
    elif command_exists wget; then
        if [ -t 2 ]; then
            wget -O "$dest" --timeout=30 --tries=3 "$url"
        else
            wget -nv -O "$dest" --timeout=30 --tries=3 "$url"
        fi
    else
        err "Neither 'curl' nor 'wget' command found"
        return 1
    fi
}

sha512_of() {
    local file="$1"

    if command_exists sha512sum; then
        sha512sum "$file" | cut -d' ' -f1
    elif command_exists shasum; then
        shasum -a 512 "$file" | cut -d' ' -f1
    else
        return 1
    fi
}

verify_sha512() {
    local archive="$1"
    local checksum_file="$2"
    local expected actual

    expected="$(cut -d' ' -f1 "$checksum_file" 2>/dev/null | tr -d '\r')"
    case "$expected" in
        ''|*[!0-9a-fA-F]*)
            err "Invalid SHA-512 checksum file: $checksum_file"
            return 1
            ;;
    esac
    if [ "${#expected}" -ne 128 ]; then
        err "Invalid SHA-512 checksum length in $checksum_file"
        return 1
    fi

    actual="$(sha512_of "$archive")" || {
        err "No SHA-512 checksum tool found"
        return 1
    }
    actual="$(printf '%s' "$actual" | tr 'A-F' 'a-f')"
    expected="$(printf '%s' "$expected" | tr 'A-F' 'a-f')"

    if [ "$actual" != "$expected" ]; then
        err "SHA-512 checksum mismatch for $archive"
        return 1
    fi
    return 0
}

acquire_lock() {
    local dir="$1"
    local waited=0
    local other_pid

    while :; do
        if mkdir "$dir" 2>/dev/null; then
            LOCK_DIR="$dir"
            LOCK_OWNED=1
            if ! printf '%s\n' "$$" >"${dir}/pid" 2>/dev/null; then
                release_lock
                err "Failed to record lock owner: $dir"
                return 1
            fi
            return 0
        fi

        if [ ! -e "$dir" ]; then
            err "Failed to create lock: $dir"
            return 1
        fi
        if [ ! -d "$dir" ]; then
            err "Lock path exists but is not a directory: $dir"
            return 1
        fi

        if [ -f "${dir}/pid" ]; then
            other_pid="$(cat "${dir}/pid" 2>/dev/null || true)"
            case "$other_pid" in
                ''|*[!0-9]*) ;;
                *)
                    if ! kill -0 "$other_pid" 2>/dev/null; then
                        rm -rf "$dir"
                        continue
                    fi
                    ;;
            esac
        fi

        if [ "$waited" -eq 0 ]; then
            warn "Another installer is running for $(basename "$dir"); waiting..."
        fi
        waited=$((waited + 1))
        if [ "$waited" -ge 300 ]; then
            err "Timed out waiting for lock: $dir"
            return 1
        fi
        sleep 1
    done
}

release_lock() {
    if [ "${LOCK_OWNED:-0}" -eq 1 ] && [ -n "${LOCK_DIR:-}" ]; then
        rm -rf "$LOCK_DIR"
    fi
    LOCK_DIR=""
    LOCK_OWNED=0
}

cleanup_plugin_work() {
    # Restore a plugin moved aside if the process is interrupted mid-swap.
    if [ -n "${REPLACE_BACKUP:-}" ] && [ -e "$REPLACE_BACKUP" ] \
        && [ -n "${REPLACE_TARGET:-}" ] && [ ! -e "$REPLACE_TARGET" ]; then
        mv "$REPLACE_BACKUP" "$REPLACE_TARGET" 2>/dev/null \
            || err "Failed to restore interrupted plugin install from $REPLACE_BACKUP"
    fi
    REPLACE_TARGET=""
    REPLACE_BACKUP=""

    if [ -n "${STAGE_DIR:-}" ]; then
        rm -rf "$STAGE_DIR"
        STAGE_DIR=""
    fi
    if [ -n "${TMP_DIR:-}" ]; then
        rm -rf "$TMP_DIR"
        TMP_DIR=""
    fi
    release_lock
}

cleanup() {
    cleanup_plugin_work
    if [ -n "${MAVEN_TMP_DIR:-}" ]; then
        rm -rf "$MAVEN_TMP_DIR"
        MAVEN_TMP_DIR=""
    fi
    release_lock
}

maven_bootstrap_cleanup() {
    if [ -n "${MAVEN_TMP_DIR:-}" ]; then
        rm -rf "$MAVEN_TMP_DIR"
        MAVEN_TMP_DIR=""
    fi
    release_lock
}

check_java() {
    if command_exists java; then
        return 0
    fi
    if [ -n "${JAVA_HOME:-}" ] && [ -x "${JAVA_HOME}/bin/java" ]; then
        return 0
    fi
    err "Java not found; install a JDK or set JAVA_HOME"
    return 1
}

resolve_archive_tool() {
    if command_exists jar; then
        JAR_CMD="jar"
    elif [ -n "${JAVA_HOME:-}" ] && [ -x "${JAVA_HOME}/bin/jar" ]; then
        JAR_CMD="${JAVA_HOME}/bin/jar"
    else
        JAR_CMD=""
    fi

    if command_exists unzip; then
        UNZIP_CMD="unzip"
    else
        UNZIP_CMD=""
    fi

    if [ -z "$JAR_CMD" ] && [ -z "$UNZIP_CMD" ]; then
        err "Neither 'jar' nor 'unzip' command found"
        return 1
    fi
    return 0
}

get_version() {
    local ng=0
    local jar
    local base
    local version
    local -a candidates
    local -a valid

    shopt -q nullglob && ng=1
    shopt -s nullglob
    candidates=("${ADDAX_HOME}/lib/addax-core-"*.jar)
    [ "$ng" -eq 0 ] && shopt -u nullglob

    if [ "${#candidates[@]}" -eq 0 ]; then
        err "No addax-core jar found in ${ADDAX_HOME}/lib"
        return 1
    fi

    valid=()
    for jar in "${candidates[@]}"; do
        base="$(basename "$jar")"
        case "$base" in
            *-sources.jar|*-javadoc.jar|*-tests.jar)
                continue
                ;;
        esac
        valid+=("$jar")
    done

    if [ "${#valid[@]}" -eq 0 ]; then
        err "No usable addax-core jar found in ${ADDAX_HOME}/lib"
        return 1
    fi
    if [ "${#valid[@]}" -gt 1 ]; then
        err "Multiple addax-core jars found; keep exactly one:"
        for jar in "${valid[@]}"; do
            err "  $jar"
        done
        return 1
    fi

    base="$(basename "${valid[0]}")"
    version="${base%.jar}"
    version="${version#addax-core-}"
    if [ -z "$version" ]; then
        err "Failed to parse Addax version from ${valid[0]}"
        return 1
    fi
    printf '%s\n' "$version"
}

validate_plugin() {
    local plugin="$1"

    case "$plugin" in
        ''|*[!a-z0-9_-]*)
            err "Invalid plugin name '$plugin': expected a lowercase artifact ending in 'reader' or 'writer'"
            return 1
            ;;
    esac
    case "$plugin" in
        [a-z0-9]*)
            ;;
        *)
            err "Invalid plugin name '$plugin': expected a lowercase artifact ending in 'reader' or 'writer'"
            return 1
            ;;
    esac
    case "$plugin" in
        *reader|*writer)
            return 0
            ;;
        *)
            err "Invalid plugin name '$plugin': expected a lowercase artifact ending in 'reader' or 'writer'"
            return 1
            ;;
    esac
}

validate_version() {
    local version="$1"

    case "$version" in
        ''|.*|*..*|*[!A-Za-z0-9._-]*)
            err "Invalid plugin version '$version'"
            return 1
            ;;
    esac
    case "$version" in
        [A-Za-z0-9]*)
            return 0
            ;;
        *)
            err "Invalid plugin version '$version'"
            return 1
            ;;
    esac
}

plugin_is_complete() {
    local dir="$1"
    local plugin="$2"
    local jar

    [ -s "${dir}/plugin.json" ] || return 1
    # Older installers used lib/ by mistake; force a reinstall into libs/.
    [ -d "${dir}/lib" ] && return 1

    for jar in "${dir}/${plugin}"-*.jar; do
        if [ -f "$jar" ]; then
            return 0
        fi
    done
    return 1
}

plugin_installed_version() {
    local dir="$1"
    local plugin="$2"
    local jar
    local name

    for jar in "${dir}/${plugin}"-*.jar; do
        [ -f "$jar" ] || continue
        name="$(basename "$jar")"
        name="${name%.jar}"
        printf '%s\n' "${name#${plugin}-}"
        return 0
    done
    return 1
}

extract_zip_entry() {
    local jar="$1"
    local entry="$2"
    local dest="$3"
    local tmp="${dest}.tmp.$$"

    rm -f "$tmp"
    if [ -n "$UNZIP_CMD" ] \
        && "$UNZIP_CMD" -p "$jar" "$entry" >"$tmp" 2>/dev/null \
        && [ -s "$tmp" ]; then
        mv "$tmp" "$dest"
        return 0
    fi
    rm -f "$tmp"
    return 1
}

extract_plugin_metadata() {
    local plugin="$1"
    local dir="$2"
    local jar=""
    local candidate

    for candidate in "${dir}/${plugin}"-*.jar; do
        if [ -f "$candidate" ]; then
            jar="$candidate"
            break
        fi
    done
    if [ -z "$jar" ]; then
        err "Cannot find plugin jar file in $dir"
        return 1
    fi

    # Extract to temporary files first so an unzip failure cannot leave
    # a zero-byte metadata file behind.
    extract_zip_entry "$jar" plugin.json "${dir}/plugin.json" || true
    extract_zip_entry "$jar" plugin_job_template.json "${dir}/plugin_job_template.json" || true

    if [ ! -s "${dir}/plugin.json" ] || [ ! -s "${dir}/plugin_job_template.json" ]; then
        if [ -n "$JAR_CMD" ]; then
            ( cd "$dir" && "$JAR_CMD" xf "$jar" plugin.json plugin_job_template.json >/dev/null 2>&1 ) || true
        fi
    fi

    if [ ! -s "${dir}/plugin.json" ]; then
        err "Missing or empty plugin.json in $jar"
        return 1
    fi
    if [ ! -s "${dir}/plugin_job_template.json" ]; then
        warn "plugin_job_template.json is missing or empty in $jar; template-based generation will not work for $plugin"
        rm -f "${dir}/plugin_job_template.json"
    fi
    return 0
}

bootstrap_maven() {
    local maven_version
    local archive_name
    local cache_root
    local install_dir
    local tmpdir=""
    local extracted
    local archive
    local checksum_file
    local old_dir=""
    local verified=0
    local url
    local -a urls

    maven_version="$MAVEN_BOOTSTRAP_VERSION"
    archive_name="apache-maven-${maven_version}-bin.tar.gz"
    cache_root="${ADDAX_HOME}/.cache"
    install_dir="${cache_root}/apache-maven-${maven_version}"

    if [ -x "${install_dir}/bin/mvn" ]; then
        if "${install_dir}/bin/mvn" -v >/dev/null 2>&1; then
            MAVEN_CMD="${install_dir}/bin/mvn"
            return 0
        fi
        warn "Cached Maven at ${install_dir} is not runnable; reinstalling"
    fi

    mkdir -p "$cache_root" || {
        err "Failed to create cache directory: $cache_root"
        return 1
    }
    if ! acquire_lock "${cache_root}/.apache-maven-${maven_version}.lock"; then
        return 1
    fi

    if [ -x "${install_dir}/bin/mvn" ] && "${install_dir}/bin/mvn" -v >/dev/null 2>&1; then
        MAVEN_CMD="${install_dir}/bin/mvn"
        release_lock
        return 0
    fi

    command_exists tar || {
        err "'tar' command not found, cannot bootstrap Maven"
        release_lock
        return 1
    }
    if ! command_exists sha512sum && ! command_exists shasum; then
        err "Neither 'sha512sum' nor 'shasum' found; cannot verify the Maven download"
        release_lock
        return 1
    fi

    tmpdir="$(mktemp -d "${cache_root}/.maven-download.XXXXXX")" || {
        err "Failed to create temporary directory for Maven bootstrap"
        release_lock
        return 1
    }
    MAVEN_TMP_DIR="$tmpdir"
    archive="${tmpdir}/${archive_name}"
    checksum_file="${archive}.sha512"

    # Every mirror must publish the matching .sha512 next to the archive.
    # ADDAX_MAVEN_DIST_URL overrides the list for restricted networks.
    if [ -n "${ADDAX_MAVEN_DIST_URL:-}" ]; then
        urls=("$ADDAX_MAVEN_DIST_URL")
    else
        urls=(
            "https://archive.apache.org/dist/maven/maven-3/${maven_version}/binaries/${archive_name}"
            "https://maven.aliyun.com/repository/public/org/apache/maven/apache-maven/${maven_version}/${archive_name}"
            "https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/${maven_version}/${archive_name}"
        )
    fi

    for url in "${urls[@]}"; do
        rm -f "$archive" "$checksum_file"
        echo "Downloading Maven ${maven_version} from ${url}"
        if ! download_file "$url" "$archive" 600; then
            warn "Failed to download Maven from $url"
            continue
        fi
        if ! download_file "${url}.sha512" "$checksum_file" 30; then
            warn "Failed to download checksum from ${url}.sha512"
            continue
        fi
        if verify_sha512 "$archive" "$checksum_file"; then
            verified=1
            break
        fi
        warn "Checksum verification failed for $url"
    done

    if [ "$verified" -ne 1 ]; then
        err "Failed to download a verified Maven distribution"
        maven_bootstrap_cleanup
        return 1
    fi

    tar -xzf "$archive" -C "$tmpdir" || {
        err "Failed to extract Maven archive"
        maven_bootstrap_cleanup
        return 1
    }
    extracted="${tmpdir}/apache-maven-${maven_version}"
    if [ ! -x "${extracted}/bin/mvn" ]; then
        err "Maven archive has an unexpected layout"
        maven_bootstrap_cleanup
        return 1
    fi
    if ! "${extracted}/bin/mvn" -v >/dev/null 2>&1; then
        err "Bootstrapped Maven is not runnable; check Java and JAVA_HOME"
        maven_bootstrap_cleanup
        return 1
    fi

    if [ -e "$install_dir" ]; then
        old_dir="${install_dir}.old.$$"
        rm -rf "$old_dir"
        if ! mv "$install_dir" "$old_dir"; then
            err "Failed to move existing Maven aside"
            maven_bootstrap_cleanup
            return 1
        fi
    fi
    if ! mv "$extracted" "$install_dir"; then
        err "Failed to install bootstrapped Maven"
        if [ -n "$old_dir" ] && [ -e "$old_dir" ]; then
            mv "$old_dir" "$install_dir" \
                || err "Failed to restore previous Maven from $old_dir"
        fi
        maven_bootstrap_cleanup
        return 1
    fi
    if [ -n "$old_dir" ]; then
        rm -rf "$old_dir" || warn "Failed to remove old Maven directory $old_dir"
    fi

    MAVEN_CMD="${install_dir}/bin/mvn"
    maven_bootstrap_cleanup
    return 0
}

resolve_maven_cmd() {
    local cached_mvn

    if command_exists mvn; then
        if mvn -v >/dev/null 2>&1; then
            MAVEN_CMD="mvn"
            return 0
        fi
        warn "'mvn' was found but is not runnable; looking for a wrapper or bootstrapped Maven"
    fi

    if [ -x "${ADDAX_HOME}/mvnw" ]; then
        MAVEN_CMD="${ADDAX_HOME}/mvnw"
        return 0
    fi
    if [ -f "${ADDAX_HOME}/mvnw" ]; then
        chmod +x "${ADDAX_HOME}/mvnw" >/dev/null 2>&1 || true
        if [ -x "${ADDAX_HOME}/mvnw" ]; then
            MAVEN_CMD="${ADDAX_HOME}/mvnw"
            return 0
        fi
        warn "Found ${ADDAX_HOME}/mvnw but cannot make it executable"
    fi

    cached_mvn="${ADDAX_HOME}/.cache/apache-maven-${MAVEN_BOOTSTRAP_VERSION}/bin/mvn"
    if [ -x "$cached_mvn" ] && "$cached_mvn" -v >/dev/null 2>&1; then
        MAVEN_CMD="$cached_mvn"
        return 0
    fi

    if [ "$OFFLINE" -eq 1 ]; then
        err "Offline mode enabled and no usable Maven found"
        return 1
    fi

    bootstrap_maven
}

ensure_build_tools() {
    if [ -n "$MAVEN_CMD" ] && { [ -n "$JAR_CMD" ] || [ -n "$UNZIP_CMD" ]; }; then
        return 0
    fi

    command_exists mktemp || {
        err "'mktemp' command not found"
        return 1
    }
    check_java || return 1
    resolve_archive_tool || return 1
    resolve_maven_cmd || {
        err "No usable Maven found (tried: mvn, ${ADDAX_HOME}/mvnw, bootstrap download)"
        return 1
    }
    return 0
}

download_plugin() {
    local plugin="$1"
    local grp
    local plugin_root
    local plugin_dir
    local stage_dir
    local tmpdir
    local pom_file
    local maven_log
    local installed_version
    local backup_dir=""
    local main_jar=""
    local target_jar=""
    local candidate

    echo "Installing plugin ${plugin}:${VERSION}"

    case "$plugin" in
        *reader)
            grp="reader"
            ;;
        *writer)
            grp="writer"
            ;;
        *)
            err "Invalid plugin name $plugin"
            return 1
            ;;
    esac

    plugin_root="${ADDAX_HOME}/plugin/${grp}"
    plugin_dir="${plugin_root}/${plugin}"

    mkdir -p "$plugin_root" || {
        err "Failed to create plugin root directory: $plugin_root"
        return 1
    }

    # Skip before resolving Maven so an already-installed plugin needs no
    # network access or Java at all.
    if [ -d "$plugin_dir" ] && plugin_is_complete "$plugin_dir" "$plugin"; then
        installed_version="$(plugin_installed_version "$plugin_dir" "$plugin" || true)"
        if [ "$FORCE" -eq 0 ] && [ -n "$installed_version" ] && [ "$installed_version" = "$VERSION" ]; then
            echo "Plugin ${plugin} ${VERSION} is already installed, skipping"
            SKIPPED=$((SKIPPED + 1))
            return 0
        fi
    fi

    # Resolve Maven before taking the plugin lock; bootstrap can take minutes
    # and must not be serialized behind (or deadlock with) plugin locks.
    if ! ensure_build_tools; then
        return 1
    fi

    if ! acquire_lock "${plugin_root}/.${plugin}.lock"; then
        return 1
    fi

    if [ -d "$plugin_dir" ] && plugin_is_complete "$plugin_dir" "$plugin"; then
        installed_version="$(plugin_installed_version "$plugin_dir" "$plugin" || true)"
        if [ "$FORCE" -eq 0 ] && [ -n "$installed_version" ] && [ "$installed_version" = "$VERSION" ]; then
            echo "Plugin ${plugin} ${VERSION} is already installed, skipping"
            SKIPPED=$((SKIPPED + 1))
            release_lock
            return 0
        fi
        echo "Reinstalling ${plugin} (installed: ${installed_version:-unknown}, requested: ${VERSION})"
    elif [ -d "$plugin_dir" ]; then
        echo "Reinstalling ${plugin} (existing directory is incomplete or uses the legacy lib/ layout)"
    fi

    stage_dir="$(mktemp -d "${plugin_root}/.${plugin}.install.XXXXXX")" || {
        err "Failed to create staging directory for plugin $plugin"
        release_lock
        return 1
    }
    STAGE_DIR="$stage_dir"
    mkdir -p "${stage_dir}/libs" || {
        err "Failed to create dependency directory for plugin $plugin"
        cleanup_plugin_work
        return 1
    }

    tmpdir="$(create_tmpdir)" || {
        err "Failed to create temporary directory for plugin $plugin"
        cleanup_plugin_work
        return 1
    }
    TMP_DIR="$tmpdir"
    pom_file="${tmpdir}/pom.xml"
    maven_log="${tmpdir}/maven.log"

    cat >"$pom_file" <<EOF
<project>
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.wgzhao.addax.installer</groupId>
  <artifactId>plugin-installer</artifactId>
  <version>1.0-SNAPSHOT</version>
  <dependencies>
    <dependency>
      <groupId>com.wgzhao.addax</groupId>
      <artifactId>${plugin}</artifactId>
      <version>${VERSION}</version>
    </dependency>
  </dependencies>
</project>
EOF
    if [ ! -s "$pom_file" ]; then
        err "Failed to write Maven POM for plugin $plugin"
        cleanup_plugin_work
        return 1
    fi

    echo "Resolving ${plugin}:${VERSION} and its dependencies..."
    if ! ( cd "$tmpdir" && run_maven_logged "$maven_log" -B -f "$pom_file" \
        dependency:copy dependency:copy-dependencies \
        -Dartifact="com.wgzhao.addax:${plugin}:${VERSION}" \
        -DexcludeGroupIds=com.wgzhao.addax \
        -DincludeScope=runtime \
        -DoutputDirectory="${stage_dir}/libs" ); then
        err "Failed to download plugin ${plugin}:${VERSION} and its dependencies from Maven"
        [ -s "$maven_log" ] && tail -n 20 "$maven_log" >&2
        cleanup_plugin_work
        return 1
    fi

    # Both goals write into libs/ so one Maven JVM resolves the graph. Move
    # the main artifact back to the plugin root to keep the official layout.
    for candidate in "${stage_dir}/libs/${plugin}"-*.jar; do
        if [ -f "$candidate" ]; then
            main_jar="$candidate"
            break
        fi
    done
    if [ -z "$main_jar" ]; then
        err "Maven did not copy the main jar for plugin $plugin"
        cleanup_plugin_work
        return 1
    fi

    # Snapshot artifacts are copied under timestamped names; normalize the
    # layout so version checks and addax.sh gen see a stable file name.
    target_jar="${stage_dir}/${plugin}-${VERSION}.jar"
    if [ "$main_jar" != "$target_jar" ]; then
        if ! mv "$main_jar" "$target_jar"; then
            err "Failed to normalize the main jar name for plugin $plugin"
            cleanup_plugin_work
            return 1
        fi
    fi

    if ! extract_plugin_metadata "$plugin" "$stage_dir"; then
        cleanup_plugin_work
        return 1
    fi
    if ! plugin_is_complete "$stage_dir" "$plugin"; then
        err "Plugin $plugin is incomplete after download"
        cleanup_plugin_work
        return 1
    fi

    if [ -e "$plugin_dir" ]; then
        backup_dir="${plugin_dir}.bak.$$"
        REPLACE_TARGET="$plugin_dir"
        REPLACE_BACKUP="$backup_dir"
        if ! mv "$plugin_dir" "$backup_dir"; then
            REPLACE_TARGET=""
            REPLACE_BACKUP=""
            err "Failed to move existing plugin $plugin aside"
            cleanup_plugin_work
            return 1
        fi
    fi

    if ! mv "$stage_dir" "$plugin_dir"; then
        err "Failed to install plugin $plugin"
        if [ -n "$backup_dir" ] && [ -e "$backup_dir" ]; then
            if mv "$backup_dir" "$plugin_dir"; then
                REPLACE_TARGET=""
                REPLACE_BACKUP=""
            else
                err "Failed to restore previous plugin $plugin from $backup_dir"
            fi
        fi
        cleanup_plugin_work
        return 1
    fi
    STAGE_DIR=""
    if [ -n "$backup_dir" ]; then
        rm -rf "$backup_dir" || warn "Failed to remove backup directory $backup_dir"
        REPLACE_TARGET=""
        REPLACE_BACKUP=""
    fi

    cleanup_plugin_work
    INSTALLED=$((INSTALLED + 1))
    echo "Plugin ${plugin} ${VERSION} installed successfully"
    return 0
}

# -------------- main -----------
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

while [ $# -gt 0 ]; do
    case "$1" in
        -v)
            if [ $# -lt 2 ]; then
                usage_error "Option -v requires an argument"
            fi
            case "$2" in
                ''|-*)
                    usage_error "Option -v requires a version, got '$2'"
                    ;;
            esac
            VERSION="$2"
            shift 2
            ;;
        -o|--offline)
            OFFLINE=1
            shift
            ;;
        -f|--force)
            FORCE=1
            shift
            ;;
        -h|--help)
            help
            ;;
        --)
            shift
            while [ $# -gt 0 ]; do
                PLUGINS+=("$1")
                shift
            done
            ;;
        -*)
            usage_error "Unknown option: $1"
            ;;
        *)
            PLUGINS+=("$1")
            shift
            ;;
    esac
done

if [ "${#PLUGINS[@]}" -lt 1 ]; then
    usage_error "At least one plugin is required"
fi
for plugin in "${PLUGINS[@]}"; do
    validate_plugin "$plugin" || exit 1
done

if [ ! -d "${ADDAX_HOME}/lib" ]; then
    err "This script must run from an installed Addax distribution's bin/ directory (ADDAX_HOME=${ADDAX_HOME})"
    exit 1
fi

if [ -z "$VERSION" ]; then
    VERSION="$(get_version)" || exit 1
    echo "Using detected Addax version: $VERSION"
else
    validate_version "$VERSION" || exit 1
    detected_version="$(get_version 2>/dev/null || true)"
    if [ -n "$detected_version" ] && [ "$detected_version" != "$VERSION" ]; then
        warn "Plugin version ${VERSION} differs from detected Addax core version ${detected_version}"
    fi
fi
validate_version "$VERSION" || exit 1

case "$VERSION" in
    *-SNAPSHOT*)
        MAVEN_STRICT=0
        if [ "$OFFLINE" -eq 0 ]; then
            warn "Version ${VERSION} is a snapshot; Maven Central hosts releases only. Use a release version or configure a snapshot repository."
        fi
        ;;
    *)
        MAVEN_STRICT=1
        ;;
esac

if [ "$OFFLINE" -eq 1 ]; then
    echo "Offline mode enabled"
fi

for plugin in "${PLUGINS[@]}"; do
    if ! download_plugin "$plugin"; then
        FAILED=$((FAILED + 1))
        FAILED_LIST="${FAILED_LIST} ${plugin}"
    fi
done

echo "Summary: installed ${INSTALLED}, skipped ${SKIPPED}, failed ${FAILED}"
if [ "$FAILED" -gt 0 ]; then
    err "Failed plugins:${FAILED_LIST}"
    exit 1
fi

exit 0
