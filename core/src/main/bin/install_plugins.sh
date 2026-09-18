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

set -u
set -o pipefail

SCRIPT_PATH="$({
    cd -- "$(dirname "$0")" >/dev/null 2>&1
    pwd -P
})"

ADDAX_HOME="$(dirname "$SCRIPT_PATH")"
VERSION=""
OFFLINE=0
MAVEN_CMD=""
JAR_CMD=""
UNZIP_CMD=""
MAVEN_BOOTSTRAP_VERSION="3.9.9"

help() {
    echo "Usage: $0 [-o|--offline] [-v version] <plugin1 plugin2 ...>"
    echo "Options:"
    echo "  -o, --offline Run in offline mode and disable Maven bootstrap download"
    echo "  -v version    Specify plugin version to install (default: detected from addax-core jar)"
    echo "Example: $0 mysqlreader mysqlwriter"
    echo "         $0 -v 1.0.8 mysqlreader mysqlwriter"
    echo "         $0 --offline mysqlreader"
    exit 1
}

command_exists() {
    command -v "$1" >/dev/null 2>&1
}

create_tmpdir() {
    local tmpdir
    tmpdir="$(mktemp -d 2>/dev/null)" || tmpdir="$(mktemp -d -t addax-plugin)"
    echo "$tmpdir"
}

run_maven() {
    if [ "$OFFLINE" -eq 1 ]; then
        "$MAVEN_CMD" -o "$@"
    else
        "$MAVEN_CMD" "$@"
    fi
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
        echo "Neither 'jar' nor 'unzip' command found"
        return 1
    fi
    return 0
}

bootstrap_maven() {
    local maven_version archive_name maven_url cache_root install_dir tmpdir

    maven_version="$MAVEN_BOOTSTRAP_VERSION"
    archive_name="apache-maven-${maven_version}-bin.tar.gz"
    maven_url="https://archive.apache.org/dist/maven/maven-3/${maven_version}/binaries/${archive_name}"
    cache_root="${ADDAX_HOME}/.cache"
    install_dir="${cache_root}/apache-maven-${maven_version}"

    if [ -x "${install_dir}/bin/mvn" ]; then
        MAVEN_CMD="${install_dir}/bin/mvn"
        return 0
    fi

    command_exists tar || {
        echo "'tar' command not found, cannot bootstrap Maven"
        return 1
    }

    tmpdir="$(create_tmpdir)" || {
        echo "Failed to create temporary directory for Maven bootstrap"
        return 1
    }

    mkdir -p "$cache_root" || {
        echo "Failed to create cache directory: $cache_root"
        rm -rf "$tmpdir"
        return 1
    }

    if command_exists curl; then
        curl -fsSL "$maven_url" -o "${tmpdir}/${archive_name}" || {
            echo "Failed to download Maven from $maven_url"
            rm -rf "$tmpdir"
            return 1
        }
    elif command_exists wget; then
        wget -qO "${tmpdir}/${archive_name}" "$maven_url" || {
            echo "Failed to download Maven from $maven_url"
            rm -rf "$tmpdir"
            return 1
        }
    else
        echo "Neither 'curl' nor 'wget' command found, cannot bootstrap Maven"
        rm -rf "$tmpdir"
        return 1
    fi

    tar -xzf "${tmpdir}/${archive_name}" -C "$tmpdir" || {
        echo "Failed to extract Maven archive"
        rm -rf "$tmpdir"
        return 1
    }

    rm -rf "$install_dir"
    mv "${tmpdir}/apache-maven-${maven_version}" "$install_dir" || {
        echo "Failed to install bootstrapped Maven"
        rm -rf "$tmpdir"
        return 1
    }

    rm -rf "$tmpdir"
    MAVEN_CMD="${install_dir}/bin/mvn"
    return 0
}

resolve_maven_cmd() {
    local cached_mvn

    if command_exists mvn; then
        MAVEN_CMD="mvn"
        return 0
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
    fi

    cached_mvn="${ADDAX_HOME}/.cache/apache-maven-${MAVEN_BOOTSTRAP_VERSION}/bin/mvn"
    if [ -x "$cached_mvn" ]; then
        MAVEN_CMD="$cached_mvn"
        return 0
    fi

    if [ "$OFFLINE" -eq 1 ]; then
        echo "Offline mode enabled, skip downloading Maven"
        return 1
    fi

    bootstrap_maven
}

get_version() {
    local pattern jar_file jar_name
    pattern="${ADDAX_HOME}/lib/addax-core-*.jar"

    for jar_file in $pattern; do
        if [ -f "$jar_file" ]; then
            jar_name="$(basename "$jar_file")"
            jar_name="${jar_name%.jar}"
            echo "${jar_name#addax-core-}"
            return 0
        fi
    done

    echo "Unknown"
    return 1
}

extract_plugin_template() {
    local plugin="$1"
    local plugin_dir="$2"
    local jar_file=""

    for jar_file in "${plugin_dir}/${plugin}"-*.jar; do
        [ -f "$jar_file" ] && break
    done

    if [ -z "$jar_file" ] || [ ! -f "$jar_file" ]; then
        echo "Warning: Cannot find plugin jar file in ${plugin_dir}"
        return 1
    fi

    # Keep runtime metadata outside jar for direct loading by engine.
    if [ -n "$JAR_CMD" ]; then
        (
            cd "$plugin_dir" || exit 1
            "$JAR_CMD" xf "$jar_file" plugin.json plugin_job_template.json >/dev/null 2>&1
        )
    fi

    if [ ! -f "${plugin_dir}/plugin.json" ] || [ ! -f "${plugin_dir}/plugin_job_template.json" ]; then
        if [ -n "$UNZIP_CMD" ]; then
            "$UNZIP_CMD" -p "$jar_file" plugin.json >"${plugin_dir}/plugin.json" 2>/dev/null || true
            "$UNZIP_CMD" -p "$jar_file" plugin_job_template.json >"${plugin_dir}/plugin_job_template.json" 2>/dev/null || true
        fi
    fi

    if [ ! -f "${plugin_dir}/plugin.json" ] || [ ! -f "${plugin_dir}/plugin_job_template.json" ]; then
        echo "Warning: Missing plugin metadata in $jar_file"
        return 1
    fi

    return 0
}

download_plugin() {
    local plugin="$1"
    local grp plugin_root plugin_dir stage_dir tmpdir pom_file

    echo "Installing plugin $plugin:$VERSION"

    case "$plugin" in
        *reader)
            grp="reader"
            ;;
        *writer)
            grp="writer"
            ;;
        *)
            echo "Invalid plugin name $plugin, must end with 'reader' or 'writer'"
            return 1
            ;;
    esac

    plugin_root="${ADDAX_HOME}/plugin/${grp}"
    plugin_dir="${plugin_root}/${plugin}"

    if [ -d "$plugin_dir" ]; then
        if [ -f "${plugin_dir}/plugin.json" ] && [ -d "${plugin_dir}/lib" ]; then
            echo "Warning: Plugin $plugin already exists, skipping"
            return 0
        fi
        echo "Warning: Found incomplete plugin directory for $plugin, reinstalling"
        rm -rf "$plugin_dir"
    fi

    mkdir -p "$plugin_root" || {
        echo "Failed to create plugin root directory: $plugin_root"
        return 1
    }

    stage_dir="${plugin_root}/.${plugin}.install.$$"
    rm -rf "$stage_dir"
    mkdir -p "${stage_dir}/lib" || {
        echo "Failed to create staging directory for plugin $plugin"
        return 1
    }

    tmpdir="$(create_tmpdir)" || {
        echo "Failed to create temporary directory"
        rm -rf "$stage_dir"
        return 1
    }

    # Download main plugin jar.
    run_maven -B -q dependency:copy \
        -Dartifact="com.wgzhao.addax:${plugin}:${VERSION}" \
        -DoutputDirectory="$stage_dir"
    if [ $? -ne 0 ]; then
        echo "Failed to install plugin $plugin"
        rm -rf "$stage_dir" "$tmpdir"
        return 1
    fi

    pom_file="${tmpdir}/pom.xml"
    cat >"$pom_file" <<EOF
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

    # Use isolated pom to avoid polluting caller's working directory.
    run_maven -B -q -f "$pom_file" dependency:copy-dependencies \
        -DexcludeGroupIds=com.wgzhao.addax,org.apache.commons \
        -DoutputDirectory="${stage_dir}/lib"

    if [ $? -ne 0 ]; then
        echo "Failed to download dependencies for plugin $plugin"
        rm -rf "$stage_dir" "$tmpdir"
        return 1
    fi

    extract_plugin_template "$plugin" "$stage_dir"
    if [ $? -ne 0 ]; then
        echo "Failed to extract plugin metadata for plugin $plugin"
        rm -rf "$stage_dir" "$tmpdir"
        return 1
    fi

    mv "$stage_dir" "$plugin_dir" || {
        echo "Failed to finalize plugin $plugin"
        rm -rf "$stage_dir" "$tmpdir"
        return 1
    }

    rm -rf "$tmpdir"
    echo "Plugin $plugin installed successfully"
    return 0
}

# -------------- main -----------
PLUGINS=()
while [ $# -gt 0 ]; do
    case "$1" in
        -v)
            if [ $# -lt 2 ]; then
                echo "Option -v requires an argument"
                help
            fi
            VERSION="$2"
            shift 2
            ;;
        -o|--offline)
            OFFLINE=1
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
            echo "Unknown option: $1"
            help
            ;;
        *)
            PLUGINS+=("$1")
            shift
            ;;
    esac
done

command_exists mktemp || {
    echo "mktemp command not found"
    exit 1
}

resolve_archive_tool || exit 1

resolve_maven_cmd || {
    echo "No usable Maven found (tried: mvn, ${ADDAX_HOME}/mvnw, bootstrap download)"
    exit 1
}

# If no version specified, detect from addax-core jar
if [ -z "$VERSION" ]; then
    VERSION="$(get_version)"
    if [ "$VERSION" = "Unknown" ] || [ -z "$VERSION" ]; then
        echo "Failed to detect Addax version from ${ADDAX_HOME}/lib/addax-core-*.jar"
        exit 1
    fi
    echo "Using detected Addax version: $VERSION"
fi

if [ "${#PLUGINS[@]}" -lt 1 ]; then
    help
fi

if [ "$OFFLINE" -eq 1 ]; then
    echo "Offline mode enabled"
fi

failures=0
for plugin in "${PLUGINS[@]}"; do
    download_plugin "$plugin" || failures=$((failures + 1))
done

if [ "$failures" -gt 0 ]; then
    echo "Warning: $failures plugin(s) failed to install"
    exit 1
fi

exit 0
