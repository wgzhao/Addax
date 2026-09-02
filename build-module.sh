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
# if build for remote host, then skip all path exists check
SKIP_CHECK=0

function build_base() {
    echo "Building base components..."
    cd "$SRC_DIR"
    # install (not package) so downstream single-module builds resolve these SNAPSHOTs from the local repo
    mvn clean install -q -B -pl :addax-core,:addax-rdbms,:addax-storage -am || {
        echo "Base build failed! Check dependencies and try again."
        exit 1
    }

    # Ensure target directories exist
    mkdir -p "${ADDAX_HOME}/lib"

    # assembly output dir is named after the artifactId (addax-core-<version>)
    rsync -a core/target/addax-core-${version}/* "${ADDAX_HOME}"
    # list source paths explicitly: two brace groups would expand to a cross product
    rsync -azv lib/addax-rdbms/target/addax-rdbms-${version}/lib/* \
        lib/addax-storage/target/addax-storage-${version}/lib/* "${ADDAX_HOME}/lib/"
    echo "Base build completed successfully"
}

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

# Get project version; the root pom has no <parent>, so the first <version> is ours.
# Resolve against SRC_DIR so the script also works when invoked from any directory.
version=$(awk -F'[<>]' '/<version>/ {print $3; exit}' "$SRC_DIR/pom.xml")
if [ -z "$version" ]; then
    echo "Error: cannot determine project version from $SRC_DIR/pom.xml"
    exit 1
fi

# Parse options and module names (options come first, modules after)
usage() {
    echo "Usage: $0 [-s] module_name1 [module_name2 ...]"
    echo "  module_name: reader/writer plugin artifact id (e.g. streamreader, mysqlwriter),"
    echo "               or addax-core | server | addax-rdbms | addax-storage"
    echo "  -s:          sync only the module jar instead of the whole plugin directory"
    echo "Env overrides: SRC_DIR, ADDAX_HOME, REMOTE_HOST"
}

SYNC_JAR_ONLY=false
while getopts ":hs" opt; do
    case "$opt" in
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
if [ -n "${REMOTE_HOST}" ]; then
    echo "The building module(s) will upload to ${REMOTE_HOST}"
    ADDAX_HOME="${REMOTE_HOST}:${ADDAX_HOME}"
    SKIP_CHECK=1
fi

# Create necessary directories and build base if needed
if [ $SKIP_CHECK -eq 0 ]; then
    if [ ! -d "$ADDAX_HOME" ]; then
        mkdir -p "$ADDAX_HOME" || { echo "Failed to create $ADDAX_HOME"; exit 1; }
    fi

    if [ ! -d "$ADDAX_HOME/bin" ]; then
        echo "Binary directory not found, building base components first"
        build_base
    fi
fi

# Function to build and deploy a single module
build_module() {
    local MODULE_NAME=$1
    local SYNC_JAR_ONLY=$2

    echo "Building module: $MODULE_NAME"
    cd "$SRC_DIR"
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
        rsync -av core/target/${MODULE_NAME}-${version}.jar ${ADDAX_HOME}/lib/
        echo "Core module deployed successfully"
        return 0
    fi

    if [ "$MODULE_NAME" == "server" ]; then
        echo "Deploying server module..."
        rsync -av server/target/${MODULE_NAME}-${version}.jar ${ADDAX_HOME}/lib/
        echo "Server module deployed successfully"
        return 0
    fi

    if [ "$MODULE_NAME" == "addax-rdbms" ] || [ "$MODULE_NAME" == "addax-storage" ]; then
        echo "Deploying $MODULE_NAME module..."
        rsync -av lib/${MODULE_NAME}/target/${MODULE_NAME}-${version}.jar ${ADDAX_HOME}/lib/
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

    # Create target directory if needed
    if [ $SKIP_CHECK -eq 0 ]; then
        if [ ! -d "$ADDAX_HOME/$MODULE_DIR" ]; then
            mkdir -p "$ADDAX_HOME/$MODULE_DIR" || {
                echo "Failed to create $ADDAX_HOME/$MODULE_DIR";
                return 1;
            }
        fi
    fi

    # Deploy module
    if [ "$SYNC_JAR_ONLY" = true ]; then
        echo "Deploying only the jar file for $MODULE_NAME..."
        if [ $SKIP_CHECK -eq 0 ]; then
            # a stale jar of a previous version would land on the classpath next to the new one
            find "$ADDAX_HOME/$MODULE_DIR/$MODULE_NAME" -maxdepth 1 \
                -name "${MODULE_NAME}-*.jar" ! -name "${MODULE_NAME}-${version}.jar" -delete 2>/dev/null || true
        fi
        rsync -avz $MODULE_DIR/$MODULE_NAME/target/${MODULE_NAME}-${version}/$MODULE_DIR/${MODULE_NAME}/${MODULE_NAME}-${version}.jar \
                $ADDAX_HOME/$MODULE_DIR/${MODULE_NAME}/
    else
        echo "Deploying complete module directory for $MODULE_NAME..."
        # sync into the module's own dir so --delete never removes sibling plugins
        rsync -avz --delete $MODULE_DIR/$MODULE_NAME/target/${MODULE_NAME}-${version}/$MODULE_DIR/${MODULE_NAME}/ \
            $ADDAX_HOME/$MODULE_DIR/${MODULE_NAME}/
    fi

    echo "Module $MODULE_NAME deployed successfully"
    return 0
}

# Build each module
for MODULE_NAME in "${MODULES[@]}"; do
    echo "========================================="
    echo "Processing module: $MODULE_NAME"
    echo "========================================="
    build_module "$MODULE_NAME" "$SYNC_JAR_ONLY" || {
        echo "Failed to process module $MODULE_NAME"
        # Continue with other modules even if one fails
    }
done

echo "All specified modules processed"
