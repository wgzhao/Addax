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

# install plugins from maven central repository

SCRIPT_PATH="$(
    cd -- "$(dirname "$0")" >/dev/null 2>&1
    pwd -P
)"

ADDAX_HOME=$(dirname $SCRIPT_PATH)
VERSION=""

get_version() {
    core_jar=$(ls -w1 ${ADDAX_HOME}/lib/addax-core-*.jar)
    if [ -z "$core_jar" ]; then
        echo "Unknown"
        exit 1
    else
        echo $(basename ${core_jar%%\.jar}) | cut -c12-
    fi
}

which mvn >/dev/null 2>&1 || {
    echo "mvn command not found"
    exit 1
}

help() {
    echo "Usage: $0 [-v version] <plugin1 plugin2 ...>"
    echo "Options:"
    echo "  -v version    Specify plugin version to install (default: detected from addax-core jar)"
    echo "Example: $0 mysqlreader mysqlwriter"
    echo "         $0 -v 1.0.8 mysqlreader mysqlwriter"
    exit 1
}

extract_plugin_template() {
    plugin=$1
    plugin_dir=$2
    jar_file=$(ls ${plugin_dir}/${plugin}-*.jar)
    if [ -z "$jar_file" ]; then
        echo "Warning: Cannot find plugin jar file in ${plugin_dir}"
        return 1
    fi

    # Extract the resource files from the jar file
    for f in plugin.json plugin_job_template.json
    do
        unzip -p "$jar_file" ${f} > "${plugin_dir}/${f}" 2>/dev/null
        if [ $? -ne 0 ]; then
            echo "Warning: No ${f} found in $jar_file"
            return 1
        fi
    done
    return 0
}

download_plugin() {
    plugin=$1
    echo "Installing plugin $plugin:$VERSION"
    # split plugin into reader plugin or writer plugin via last characters
    grp=${plugin: -6}
    if [ "$grp" != "reader" ] && [ "$grp" != "writer" ]; then
        echo "Invalid plugin name $plugin, must end with 'reader' or 'writer'"
        return 1
    fi

    # plugin already exists?
    if [ -d ${ADDAX_HOME}/plugin/${grp}/${plugin} ]; then
        echo "Warning: Plugin $plugin already exists, skipping"
        return 0
    fi

    # Create plugin directory structure
    mkdir -p ${ADDAX_HOME}/plugin/${grp}/${plugin}/lib
    plugin_dir=${ADDAX_HOME}/plugin/${grp}/${plugin}

    # Download the main plugin jar
    mvn -B -q dependency:copy -Dartifact=com.wgzhao.addax:$plugin:${VERSION} -DoutputDirectory=${plugin_dir}
    if [ $? -gt 0 ]; then
        echo "Failed to install plugin $plugin"
        rm -rf ${plugin_dir}
        return 1
    fi

    # Create temporary pom.xml to download dependencies
    cat > pom.xml << EOF
<project>
  <modelVersion>4.0.0</modelVersion>
  <groupId>temp</groupId>
  <artifactId>temp</artifactId>
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

    # Download dependencies
    mvn -B -q dependency:copy-dependencies \
        -Dartifact=com.wgzhao.addax:${plugin}:${VERSION} \
        -DexcludeGroupIds=com.wgzhao.addax,org.apache.commons \
        -DoutputDirectory=${plugin_dir}/lib

    if [ $? -gt 0 ]; then
        echo "Failed to download dependencies for plugin $plugin"
        rm -rf ${plugin_dir}
        rm -f pom.xml
        return 1
    fi

    # Extract plugin template configuration
    extract_plugin_template $plugin $plugin_dir

    # Remove temporary pom.xml
    rm -f pom.xml
    echo "Plugin $plugin installed successfully"
    return 0
}

# -------------- main -----------
# Parse command line options
while getopts "v:h" opt; do
    case $opt in
        v)
            VERSION="${OPTARG}"
            ;;
        h|*)
            help
            ;;
    esac
done
shift $((OPTIND-1))

# If no version specified, detect from addax-core jar
if [ -z "$VERSION" ]; then
    VERSION=$(get_version)
    echo "Using detected Addax version: $VERSION"
fi

if [ $# -lt 1 ]; then
    help
fi

failures=0
for plugin in "$@"; do
    download_plugin "$plugin" || ((failures++))
done

if [ $failures -gt 0 ]; then
    echo "Warning: $failures plugin(s) failed to install"
    exit 1
fi

exit 0