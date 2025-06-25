#!/bin/sh
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
# Addax command line script
# Author wgzhao<wgzhao@gmail.com>
# Created at 2021-07-22

# ------------------------------ Constants ----------------------------------------
SCRIPT_PATH="$(
    cd -- "$(dirname "$0")" >/dev/null 2>&1
    pwd -P
)"
ADDAX_HOME=$(dirname "$SCRIPT_PATH")
DEBUG_PORT=9999

CLASS_PATH=".:/etc/hbase/conf:${ADDAX_HOME}/lib/*"
LOGBACK_FILE="${ADDAX_HOME}/conf/logback.xml"
CORE_JSON="${ADDAX_HOME}/conf/core.json"

DEFAULT_PROPERTY_CONF="-Dfile.encoding=UTF-8 -Djava.security.egd=file:///dev/urandom -Daddax.home=${ADDAX_HOME} -Dlogback.configurationFile=${LOGBACK_FILE}"
REMOTE_DEBUG_CONFIG="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,address=0.0.0.0:${DEBUG_PORT}"
JAVA_OPTS="$JAVA_OPTS -XX:+UseZGC -XX:MaxGCPauseMillis=200 -XX:InitiatingHeapOccupancyPercent=75"

# ------------------------------ Global Variables ---------------------------------
CUST_JVM=""
LOG_DIR="${ADDAX_HOME}/log"
DEBUG=0
LOG_LEVEL="info"
JOB_FILE=""
LOG_FILE=""
TMPDIR=""

# ------------------------------ Functions ----------------------------------------

# Print usage information
usage() {
    cat <<-EOF
Usage: $(basename "$0") [options] job-url-or-path

Options:
  -h, --help                  This help text
  -v, --version               Show version number and quit
  -j, --jvm <jvm parameters>  Set extra java jvm parameters if necessary.
  -p, --params <parameter>    Set job parameter, e.g., -p"-DtableName=your-table-name".
  -l, --logdir <log directory> The directory where logs are written to
  -d, --debug                 Enable remote debug mode.
  -L, --loglevel <log level>  Set log level (e.g., debug, info, warn, error, all).

Usage: $(basename "$0") gen [options]

  Generate job template file
Options:
  -r specify reader plugin name
  -w specify writer plugin name
  -l [r|w] list all reader/writer plugin names
EOF
    exit 1
}

# Print version information
print_version() {
    echo -n "Addax version: "
    core_jar=$(ls -w1 "${ADDAX_HOME}"/lib/addax-core-*.jar 2>/dev/null)
    if [ -z "$core_jar" ]; then
        echo "Unknown"
    else
        echo "$(basename "${core_jar%%\.jar}")" | cut -c12-
    fi
    exit 0
}

# Cleanup temporary files
cleanup() {
    if [ -n "$TMPDIR" ] && [ -d "$TMPDIR" ]; then
        rm -rf "$TMPDIR"
    fi
}
trap cleanup EXIT

# Parse job file (local or remote)
parse_job_file() {
    case "$JOB_FILE" in
        [hH][tT][tT][pP]://* | [hH][tT][tT][pP][sS]://*)
            if ! command -v curl >/dev/null 2>&1; then
                echo "Error: curl command not found, cannot download job file"
                exit 1
            fi
            TMPDIR=$(mktemp -d /tmp/addax.XXXXXX)
            JOB_NAME=$(basename "${JOB_FILE}")
            if ! curl -sS -f -o "$TMPDIR/$JOB_NAME" "${JOB_FILE}" 2>/dev/null; then
                echo "Error: Failed to download job file from ${JOB_FILE}"
                exit 1
            fi
            JOB_FILE=$(ls -w1 "${TMPDIR}/"*)
            ;;
    esac

    if [ ! -f "${JOB_FILE}" ]; then
        echo "Error: The job file '${JOB_FILE}' does not exist"
        exit 2
    fi

    if [ ! -r "${JOB_FILE}" ]; then
        echo "Error: The job file '${JOB_FILE}' is not readable"
        exit 3
    fi
}

# Generate log file name
gen_log_file() {
    if [ ! -d "${LOG_DIR}" ]; then
        mkdir -p "${LOG_DIR}" || {
            echo "Error: Failed to create log directory ${LOG_DIR}"
            exit 1
        }
    fi

    if [ ! -w "${LOG_DIR}" ]; then
        echo "Error: Log directory ${LOG_DIR} is not writable"
        exit 1
    fi

    job_name=$(basename "$JOB_FILE")
    job_escaped_name=$(echo "${job_name%\.*}" | tr '.' '_')
    curr_time=$(date +"%Y%m%d_%H%M%S")
    pid=$$
    LOG_FILE="addax_${job_escaped_name}_${curr_time}_${pid}.log"
}

# check the jdk version
get_jdk_version() {
    # get major version
    local version_string=$(java -version 2>&1 | head -n 1 | grep -o -E '\"[^\"]+\"' | sed 's/"//g')
    if echo "$version_string" | grep -q '^1\.'; then
        # old version (like JDK 1.8)
        echo ${version_string#1.} | cut -d '.' -f 1
    else
        # new version (like JDK 9+)
        echo $version_string | cut -d '.' -f 1
    fi
}

check_jdk_version() {
    local jdk_version=$(get_jdk_version)
    if [ "$jdk_version" -lt 17 ]; then
        echo "Error: JDK version $jdk_version is not supported. Please use JDK 17 or higher."
        exit 1
    fi
}

# Generate job template JSON
generate_json() {
    LIST_PLUGINS=0
    READER=""
    WRITER=""

    while getopts 'r:w:lh' option; do
        case "$option" in
            r) READER="${OPTARG}" ;;
            w) WRITER="${OPTARG}" ;;
            l) LIST_PLUGINS=1 ;;
            h|*)
                echo "Usage: $0 gen [options]"
                echo "Options:"
                echo -e "\t -r specify reader plugin name"
                echo -e "\t -w specify writer plugin name"
                echo -e "\t -l list all reader/writer plugin names"
                echo -e "\t -h Print Usage and exit"
                exit 0
                ;;
        esac
    done

    if [ ${LIST_PLUGINS} -eq 1 ]; then
        list_all_plugin_names
        exit 0
    fi

    if [ -z "${READER}" ] || [ -z "${WRITER}" ]; then
        echo "Error: Both -r (reader) and -w (writer) are required arguments"
        exit 2
    fi

    READER_TEMPLATE="${ADDAX_HOME}/plugin/reader/${READER}/plugin_job_template.json"
    if [ ! -f "${READER_TEMPLATE}" ]; then
        echo "Error: Reader plugin ${READER} does not exist or has not been installed yet"
        echo "Available reader plugins:"
        for i in "${ADDAX_HOME}"/plugin/reader/*; do
            if [ -d "$i" ]; then
                echo "  $(basename "${i}")"
            fi
        done
        exit 3
    fi

    WRITER_TEMPLATE="${ADDAX_HOME}/plugin/writer/${WRITER}/plugin_job_template.json"
    if [ ! -f "${WRITER_TEMPLATE}" ]; then
        echo "Error: Writer plugin ${WRITER} does not exist or has not been installed yet"
        echo "Available writer plugins:"
        for i in "${ADDAX_HOME}"/plugin/writer/*; do
            if [ -d "$i" ]; then
                echo "  $(basename "${i}")"
            fi
        done
        exit 3
    fi

    reader_content=$(sed 's/^/      /' "${READER_TEMPLATE}")
    writer_content=$(sed 's/^/      /' "${WRITER_TEMPLATE}")

    printf '{
      "job": {
        "setting": {
          "speed": {
            "byte": -1,
            "channel": 1
          }
        },
        "content": {
          "reader": %s,
          "writer": %s
        }
      }
    }\n' "$reader_content" "$writer_content"
}

# List all plugin names
list_all_plugin_names() {
    echo "Reader Plugins:"
    for i in "${ADDAX_HOME}"/plugin/reader/*
    do
        if [ -d "$i" ]; then
            echo "  $(basename "${i}")"
        fi
    done
    echo
    echo "Writer Plugins:"
    for i in "${ADDAX_HOME}"/plugin/writer/*
    do
        if [ -d "$i" ]; then
            echo "  $(basename "${i}")"
        fi
    done
}

# ------------------------------ Main Logic ---------------------------------------

[ $# -eq 0 ] && usage

if [ "$1" = "gen" ]; then
    shift 1
    generate_json "$@"
    exit 0
fi

OS=$(uname -s)
if [ "$OS" = "Linux" ]; then
    PARSED_ARGUMENTS=$(getopt -a -n 'addax' -o hj:p:l:vL:d -l help,jvm:,params:,logdir:,version,loglevel:,debug -- "$@")
    if [ $? -ne 0 ]; then
        echo "Error: Failed to parse arguments"
        exit 65
    fi
    eval set -- "$PARSED_ARGUMENTS"

    while true; do
        case "$1" in
            -h|--help)
                usage
                ;;
            -j|--jvm)
                CUST_JVM="$2"
                shift 2
                ;;
            -p|--params)
                PARAMS="$2"
                shift 2
                ;;
            -l|--logdir)
                LOG_DIR="$2"
                shift 2
                ;;
            -d|--debug)
                DEBUG=1
                shift
                ;;
            -v|--version)
                print_version
                ;;
            -L|--loglevel)
                LOG_LEVEL="$2"
                shift 2
                ;;
            --)
                shift
                break
                ;;
            *)
                usage
                ;;
        esac
    done
else
    while getopts 'hj:p:l:vdL:' option; do
        case "$option" in
            h) usage ;;
            j) CUST_JVM="${OPTARG}" ;;
            p) PARAMS="${OPTARG}" ;;
            l) LOG_DIR="${OPTARG}" ;;
            v) print_version ;;
            d) DEBUG=1 ;;
            L) LOG_LEVEL="${OPTARG}" ;;
            ?) usage ;;
        esac
    done
    shift $((OPTIND - 1))
fi

if [ $# -eq 0 ]; then
    echo "Error: Job file is required"
    usage
fi

JOB_FILE="${1}"
check_jdk_version
parse_job_file
gen_log_file

PARAMS=" ${DEFAULT_PROPERTY_CONF} -Dloglevel=${LOG_LEVEL} -Daddax.log=${LOG_DIR} -Dlog.file.name=${LOG_FILE} ${PARAMS}"
cmd="java -server ${DEFAULT_JVM} -classpath ${CLASS_PATH} $JAVA_OPTS ${CUST_JVM} ${PARAMS}"
[ ${DEBUG} -eq 1 ] && cmd="${cmd} ${REMOTE_DEBUG_CONFIG}"

sh -c "${cmd} com.wgzhao.addax.core.Engine -job ${JOB_FILE}"
exit $?