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

# Get project version
version=$(head -n25 pom.xml | awk -F'[<>]' '/<version>/ {print $3; exit}')
export MAVEN_OPTS="-Dmaven.test.skip=true -Dmaven.javadoc.skip=true -Dmaven.source.skip=true "
# if build for remote host, then skip all path exists check
SKIP_CHECK=0

function build_base() {
    echo "Building base components..."
    cd "$SRC_DIR"
    mvn clean package -q -B -pl :addax-core,:addax-rdbms,:addax-storage -am || {
        echo "Base build failed! Check dependencies and try again."
        exit 1
    }

    # Ensure target directories exist
    mkdir -p "${ADDAX_HOME}/lib"

    rsync -a core/target/addax-${version}/* "${ADDAX_HOME}"
    rsync -azv lib/addax-{rdbms,storage}/target/addax-{rdbms,storage}-${version}/lib/* "${ADDAX_HOME}/lib/"
    echo "Base build completed successfully"
}

# Set default directories if not provided
if [ -z "$SRC_DIR" ]; then
  SRC_DIR=$HOME/code/personal/Addax
  echo "Using default source directory: $SRC_DIR"
fi

if [ -z "$ADDAX_HOME" ]; then
   ADDAX_HOME=/opt/app/addax
   echo "Using default Addax home: $ADDAX_HOME"
fi

# Check if we have any arguments
if [ $# -eq 0 ]; then
    echo "Usage: $0 module_name1 [module_name2 ...] [s]"
    echo "       module_names: One or more module names to build"
    echo "       s: Optional flag to sync only the jar file instead of the whole directory"
    exit 1
fi

# Check if last argument is 's' flag
SYNC_JAR_ONLY=false
MODULES=()
for arg in "$@"; do
    if [ "$arg" = "s" ]; then
        SYNC_JAR_ONLY=true
    else
        MODULES+=("$arg")
    fi
done

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
    mvn clean package -B -q -pl :$MODULE_NAME -am || {
        echo "Failed to build $MODULE_NAME"
        return 1
    }

    # Handle special modules
    if [ "$MODULE_NAME" == "addax-core" ]; then
        echo "Deploying core module..."
        rsync -av core/target/${MODULE_NAME}-${version}.jar ${ADDAX_HOME}/lib/
        echo "Core module deployed successfully"
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
        rsync -avz $MODULE_DIR/$MODULE_NAME/target/${MODULE_NAME}-${version}/$MODULE_DIR/${MODULE_NAME}/${MODULE_NAME}-${version}.jar \
                $ADDAX_HOME/$MODULE_DIR/${MODULE_NAME}/
    else
        echo "Deploying complete module directory for $MODULE_NAME..."
        rsync -avz --delete $MODULE_DIR/$MODULE_NAME/target/${MODULE_NAME}-${version}/$MODULE_DIR/${MODULE_NAME} \
            $ADDAX_HOME/$MODULE_DIR/
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