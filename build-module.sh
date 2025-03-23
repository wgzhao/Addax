#!/bin/bash
# compile specify module and copy to specify directory

set -e  # Exit on any error

# Get project version
version=$(head -n10 pom.xml | awk -F'[<>]' '/<version>/ {print $3; exit}')
mvn_opts="-Dmaven.test.skip=true -Dmaven.javadoc.skip=true -Dmaven.source.skip=true --quiet"
# if build for remote host, then skip all path exists check
SKIP_CHECK=0

function build_base() {
    echo "Building base components..."
    cd "$SRC_DIR"
    mvn package -B $mvn_opts $missing_deps_opts -pl :addax-core,:addax-rdbms,:addax-storage -am || {
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

if [ -z "$1" ]; then
    echo "Usage: $0 module_name [s]"
    echo "       module_name: Name of the module to build"
    echo "       s: Optional flag to sync only the jar file instead of the whole directory"
    exit 1
fi

# Handle remote host case
if [ -n "${REMOTE_HOST}" ]; then
    echo "The building module will upload to ${REMOTE_HOST}"
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

MODULE_NAME=$1

# Build the specified module
echo "Building module: $MODULE_NAME"
cd "$SRC_DIR"
mvn package -B $mvn_opts $missing_deps_opts -pl :$MODULE_NAME -am || {
    echo "Failed to build $MODULE_NAME"
    exit 1
}

# Handle special modules
if [ "$MODULE_NAME" == "addax-core" ]; then
    echo "Deploying core module..."
    rsync -av core/target/${MODULE_NAME}-${version}.jar ${ADDAX_HOME}/lib/
    echo "Core module deployed successfully"
    exit 0
fi

if [ "$MODULE_NAME" == "addax-rdbms" ] || [ "$MODULE_NAME" == "addax-storage" ]; then
    echo "Deploying $MODULE_NAME module..."
    rsync -av lib/${MODULE_NAME}/target/${MODULE_NAME}-${version}.jar ${ADDAX_HOME}/lib/
    echo "$MODULE_NAME module deployed successfully"
    exit 0
fi

# Determine if it's a reader or writer plugin
if [[ $MODULE_NAME =~ .*"reader" ]]; then
    MODULE_DIR=plugin/reader
elif [[ $MODULE_NAME =~ .*"writer" ]]; then
    MODULE_DIR=plugin/writer
else
    echo "Error: Module name must end with 'reader' or 'writer'"
    exit 1
fi

# Create target directory if needed
if [ $SKIP_CHECK -eq 0 ]; then
    if [ ! -d "$ADDAX_HOME/$MODULE_DIR" ]; then
        mkdir -p "$ADDAX_HOME/$MODULE_DIR" || {
            echo "Failed to create $ADDAX_HOME/$MODULE_DIR";
            exit 1;
        }
    fi
fi

# Deploy module
if [ -n "$2" ] && [ "$2" = "s" ]; then
    echo "Deploying only the jar file for $MODULE_NAME..."
    rsync -avz $MODULE_DIR/$MODULE_NAME/target/${MODULE_NAME}-${version}/$MODULE_DIR/${MODULE_NAME}/${MODULE_NAME}-${version}.jar \
            $ADDAX_HOME/$MODULE_DIR/${MODULE_NAME}/
else
    echo "Deploying complete module directory for $MODULE_NAME..."
    rsync -avz $MODULE_DIR/$MODULE_NAME/target/${MODULE_NAME}-${version}/$MODULE_DIR/${MODULE_NAME} \
        $ADDAX_HOME/$MODULE_DIR/
fi

echo "Module $MODULE_NAME deployed successfully"