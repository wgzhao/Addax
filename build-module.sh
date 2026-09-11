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

# compile specify module(s) and copy to specify directory

set -e  # Exit on any error

# gpg signing binds to the verify phase and needs a keyring; it only matters for
# formal releases, so skip it for local installs
export MAVEN_OPTS="-Dmaven.test.skip=true -Dmaven.javadoc.skip=true -Dmaven.source.skip=true -Dgpg.skip=true "

# Set default directories if not provided
if [ -z "$SRC_DIR" ]; then
  # resolve the script's own location so any clone of the repo works out of the box
  SRC_DIR=$(cd "$(dirname "$0")" && pwd)
  echo "Using default source directory: $SRC_DIR"
fi

if [ -z "$ADDAX_HOME" ]; then
   ADDAX_HOME=/opt/app/addax
   echo "Using default Addax home: $ADDAX_HOME"
fi

# Create a missing ADDAX_HOME without asking (non-interactive installs)
ASSUME_YES=${ASSUME_YES:-false}

# Get project version; the root pom has no <parent>, so the first <version> is ours.
# Resolve against SRC_DIR so the script also works when invoked from any directory.
version=$(awk -F'[<>]' '/<version>/ {print $3; exit}' "$SRC_DIR/pom.xml")
if [ -z "$version" ]; then
    echo "Error: cannot determine project version from $SRC_DIR/pom.xml"
    exit 1
fi

# ---------------------------------------------------------------------------
# deployment target helpers
#
# ADDAX_HOME is the Addax home path, ADDAX_TARGET is the rsync target pointing
# at it: the path itself for a local install, "host:path" when REMOTE_HOST is set.
# ---------------------------------------------------------------------------

is_remote() {
    [ -n "$REMOTE_HOST" ]
}

# Run a shell command on the deployment target.
target_exec() {
    if is_remote; then
        ssh "$REMOTE_HOST" "$1"
    else
        bash -c "$1"
    fi
}

# Check whether a directory exists on the deployment target.
target_dir_exists() {
    if is_remote; then
        # ssh reports transport problems with 255, any other status is the remote
        # test result, so the status has to be kept before it can be inspected
        local rc=0
        ssh "$REMOTE_HOST" "test -d '$1'" || rc=$?
        if [ "$rc" -eq 255 ]; then
            echo "Error: cannot run commands on $REMOTE_HOST (check ssh access and host key)" >&2
            exit 1
        fi
        return "$rc"
    fi
    # a local install has no transport error to distinguish, the test result is
    # the answer
    [ -d "$1" ]
}

# Check whether a directory on the deployment target is writable, a read-only
# install dir would otherwise only fail deep inside rsync
target_dir_writable() {
    if is_remote; then
        ssh "$REMOTE_HOST" "test -w '$1'" 2>/dev/null
    else
        [ -w "$1" ]
    fi
}

# Ask before creating a missing directory; without a terminal (ci, cron) create it
# directly instead of hanging on a read that never gets an answer.
confirm_create() {
    if [ "$ASSUME_YES" = true ]; then
        return 0
    fi
    if [ ! -t 0 ]; then
        echo "$1 does not exist, creating it"
        return 0
    fi
    local answer
    read -r -p "$1 does not exist on ${REMOTE_HOST:-the local host}. Create it? [Y/n] " answer
    case "$answer" in
        [Nn]*) return 1 ;;
        *) return 0 ;;
    esac
}

# Create a directory on the deployment target without asking, -p also creates the
# whole parent chain which is what a first-time install needs
ensure_target_dir() {
    target_dir_exists "$1" && return 0
    target_exec "mkdir -p '$1'" || {
        echo "Failed to create $1 on ${REMOTE_HOST:-the local host}" >&2
        return 1
    }
}

# Make sure the Addax home exists on the deployment target. A fresh host usually
# has no /opt/app/addax at all. Only the home itself is worth asking about, the
# directories below it are part of its layout and are created without a prompt.
ensure_home_dir() {
    local home=$1
    if target_dir_exists "$home"; then
        if ! target_dir_writable "$home"; then
            echo "Error: $home on ${REMOTE_HOST:-the local host} is not writable" >&2
            return 1
        fi
        return 0
    fi
    if ! confirm_create "$home"; then
        echo "Aborted: $home does not exist on the deployment target" >&2
        return 1
    fi
    ensure_target_dir "$home" || return 1
    echo "Created $home on ${REMOTE_HOST:-the local host}"
}

# Sync a path into the Addax home, recreating the directory layout below it.
#
# The staging root contains the layout Addax expects (bin/, lib/,
# plugin/<type>/<name>), the second argument is the path to sync relative to that
# root. rsync -R (relative) rebuilds the same path below the home on the receiving
# side and creates every missing directory on the way, while a plain rsync only
# creates the last component of the destination path. That is what makes
# plugin/writer/<name> and lib land correctly on a first-time remote install.
sync_into_home() {
    local staging_root=$1
    local rel_source=$2
    shift 2
    if [ ! -e "$staging_root/$rel_source" ]; then
        echo "Error: $staging_root/$rel_source is missing, nothing to deploy" >&2
        return 1
    fi
    # the ./ prefix keeps the implied path relative to the staging root instead of
    # baking the absolute build directory into the remote path
    (cd "$staging_root" && rsync -aR "$@" "./${rel_source}" "${ADDAX_TARGET}/") || {
        echo "Error: rsync of $rel_source to ${ADDAX_TARGET} failed" >&2
        return 1
    }
}

# Mirror a directory into the Addax home, removing files that are gone from the
# source. -R cannot be combined with --delete here: the implied parent directories
# become part of the transfer and --delete then wipes every sibling plugin, so the
# destination directory is created first and the sync stays inside it.
sync_dir_into_home() {
    local staging_root=$1
    local rel_dir=$2
    if [ ! -d "$staging_root/$rel_dir" ]; then
        echo "Error: $staging_root/$rel_dir is missing, nothing to deploy" >&2
        return 1
    fi
    ensure_target_dir "$ADDAX_HOME/$rel_dir" || return 1
    rsync -az --delete "$staging_root/$rel_dir/" "$ADDAX_TARGET/$rel_dir/" || {
        echo "Error: rsync of $rel_dir to ${ADDAX_TARGET}/$rel_dir failed" >&2
        return 1
    }
}

# Remove jars left over from a previous version, they would sit next to the new
# one on the classpath otherwise.
remove_stale_jars() {
    local dir=$1
    if ! target_dir_exists "$dir"; then
        return 0
    fi
    # -not is used instead of ! so the command also works when run through ssh
    target_exec "find '$dir' -maxdepth 1 -name '$2' -not -name '$3' -delete" 2>/dev/null || true
}

function build_base() {
    echo "Building base components..."
    cd "$SRC_DIR" || return 1
    # install (not package) so downstream single-module builds resolve these SNAPSHOTs from the local repo
    mvn clean install -q -B -pl :addax-core,:addax-rdbms,:addax-storage -am || {
        echo "Base build failed! Check dependencies and try again."
        return 1
    }

    # the assembly output dir is named after the artifactId (addax-core-<version>)
    # and mirrors the Addax home layout; rsync creates the sub directories it
    # transfers on its own, only the two destination roots have to exist
    ensure_target_dir "$ADDAX_HOME/lib" || return 1
    rsync -az "$SRC_DIR/core/target/addax-core-${version}/." "${ADDAX_TARGET}/" || {
        echo "Failed to deploy core files to ${ADDAX_TARGET}" >&2
        return 1
    }
    # list source paths explicitly: two brace groups would expand to a cross product
    rsync -azv "$SRC_DIR/lib/addax-rdbms/target/addax-rdbms-${version}/lib/." \
        "${ADDAX_TARGET}/lib/" || return 1
    rsync -azv "$SRC_DIR/lib/addax-storage/target/addax-storage-${version}/lib/." \
        "${ADDAX_TARGET}/lib/" || return 1
    echo "Base build completed successfully"
}

# Parse options and module names (options come first, modules after)
usage() {
    echo "Usage: $0 [-y] [-s] module_name1 [module_name2 ...]"
    echo "  module_name: reader/writer plugin artifact id (e.g. streamreader, mysqlwriter),"
    echo "               or addax-core | server | addax-rdbms | addax-storage"
    echo "  -y:          create a missing ADDAX_HOME without asking"
    echo "  -s:          sync only the module jar instead of the whole plugin directory"
    echo "Env overrides: SRC_DIR, ADDAX_HOME, REMOTE_HOST, ASSUME_YES"
}

SYNC_JAR_ONLY=false
while getopts ":hsy" opt; do
    case "$opt" in
        y) ASSUME_YES=true ;;
        s) SYNC_JAR_ONLY=true ;;
        h) usage; exit 0 ;;
        *) echo "Error: unknown option -$OPTARG" >&2; usage; exit 1 ;;
    esac
done
shift $((OPTIND - 1))
MODULES=("$@")

if [ ${#MODULES[@]} -eq 0 ]; then
    usage
    exit 1
fi

# Handle remote host case
ADDAX_TARGET="${ADDAX_HOME}"
if [ -n "${REMOTE_HOST}" ]; then
    echo "The building module(s) will upload to ${REMOTE_HOST}"
    ADDAX_TARGET="${REMOTE_HOST}:${ADDAX_HOME}"
fi

# Create necessary directories and build base if needed
ensure_home_dir "$ADDAX_HOME" || exit 1

if ! target_dir_exists "$ADDAX_HOME/bin"; then
    echo "Binary directory not found, building base components first"
    build_base || exit 1
fi

# Function to build and deploy a single module
build_module() {
    local MODULE_NAME=$1
    local SYNC_JAR_ONLY=$2

    echo "Building module: $MODULE_NAME"
    cd "$SRC_DIR" || return 1
    # avoid -am on the happy path: it rebuilds every upstream module on each iteration.
    # fall back to -am only when the module's SNAPSHOT deps are missing or stale.
    mvn clean install -B -q -pl :$MODULE_NAME || {
        echo "Direct build failed for $MODULE_NAME, retrying with upstream modules (-am)..."
        mvn clean install -B -q -pl :$MODULE_NAME -am || {
            echo "Failed to build $MODULE_NAME (rerun without -q to see the error)"
            return 1
        }
    }

    # Handle special modules
    if [ "$MODULE_NAME" == "addax-core" ]; then
        echo "Deploying core module..."
        sync_into_home "$SRC_DIR/core/target/${MODULE_NAME}-${version}" \
            "lib/${MODULE_NAME}-${version}.jar" -z || return 1
        echo "Core module deployed successfully"
        return 0
    fi

    if [ "$MODULE_NAME" == "server" ]; then
        echo "Deploying server module..."
        sync_into_home "$SRC_DIR/server/target/${MODULE_NAME}-${version}" \
            "lib/${MODULE_NAME}-${version}.jar" -z || return 1
        echo "Server module deployed successfully"
        return 0
    fi

    if [ "$MODULE_NAME" == "addax-rdbms" ] || [ "$MODULE_NAME" == "addax-storage" ]; then
        echo "Deploying $MODULE_NAME module..."
        sync_into_home "$SRC_DIR/lib/${MODULE_NAME}/target/${MODULE_NAME}-${version}" \
            "lib/${MODULE_NAME}-${version}.jar" -z || return 1
        echo "$MODULE_NAME module deployed successfully"
        return 0
    fi

    # Determine if it's a reader or writer plugin
    if [[ $MODULE_NAME =~ .*"reader" ]]; then
        MODULE_DIR=plugin/reader
    elif [[ $MODULE_NAME =~ .*"writer" ]]; then
        MODULE_DIR=plugin/writer
    else
        echo "Error: Module name must end with 'reader' or 'writer'"
        return 1
    fi

    local STAGING_ROOT="$SRC_DIR/$MODULE_DIR/$MODULE_NAME/target/${MODULE_NAME}-${version}"

    # Deploy module
    if [ "$SYNC_JAR_ONLY" = true ]; then
        echo "Deploying only the jar file for $MODULE_NAME..."
        remove_stale_jars "$ADDAX_HOME/$MODULE_DIR/$MODULE_NAME" \
            "${MODULE_NAME}-*.jar" "${MODULE_NAME}-${version}.jar"
        sync_into_home "$STAGING_ROOT" \
            "$MODULE_DIR/$MODULE_NAME/${MODULE_NAME}-${version}.jar" -z || return 1
    else
        echo "Deploying complete module directory for $MODULE_NAME..."
        # sync into the module's own dir so --delete never removes sibling plugins
        sync_dir_into_home "$STAGING_ROOT" "$MODULE_DIR/$MODULE_NAME" || return 1
    fi

    echo "Module $MODULE_NAME deployed successfully"
    return 0
}

# Build each module
FAILED=0
for MODULE_NAME in "${MODULES[@]}"; do
    echo "========================================="
    echo "Processing module: $MODULE_NAME"
    echo "========================================="
    build_module "$MODULE_NAME" "$SYNC_JAR_ONLY" || {
        echo "Failed to process module $MODULE_NAME" >&2
        # Continue with other modules even if one fails
        FAILED=$((FAILED + 1))
    }
done

if [ "$FAILED" -gt 0 ]; then
    echo "$FAILED module(s) failed" >&2
    exit 1
fi

echo "All specified modules processed"
