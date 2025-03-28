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
# Addax command line script
# Author wgzhao<wgzhao@gmail.com>
# Created at 2021-07-22

# ------------------------------ constant ----------------------------------------
SCRIPT_PATH="$(
    cd -- "$(dirname "$0")" >/dev/null 2>&1
    pwd -P
)"

ADDAX_HOME=$(dirname "$SCRIPT_PATH")

DEBUG_PORT=9999
if [ -z "${ADDAX_HOME}" ]; then
    echo "Error: Cannot determine ADDAX_HOME directory"
    exit 2
fi

CLASS_PATH=".:/etc/hbase/conf:${ADDAX_HOME}/lib/*"
LOGBACK_FILE="${ADDAX_HOME}/conf/logback.xml"
CORE_JSON="${ADDAX_HOME}/conf/core.json"

if [ ! -f "$CORE_JSON" ]; then
    echo "Error: core.json not found at ${CORE_JSON}"
    exit 2
fi

CORE_JVM=$(grep '"jvm":' "${CORE_JSON}" | cut -d: -f2 | tr -d ',"')
DEFAULT_JVM="$CORE_JVM -XX:+HeapDumpOnOutOfMemoryError -XX:+ExitOnOutOfMemoryError -XX:HeapDumpPath=${ADDAX_HOME}"
DEFAULT_PROPERTY_CONF="-Dfile.encoding=UTF-8 -Djava.security.egd=file:///dev/urandom -Daddax.home=${ADDAX_HOME} \
                        -Dlogback.configurationFile=${LOGBACK_FILE}"
ENGINE_COMMAND="java -server ${DEFAULT_JVM} ${DEFAULT_PROPERTY_CONF} -classpath ${CLASS_PATH}"
REMOTE_DEBUG_CONFIG="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,address=0.0.0.0:${DEBUG_PORT}"

# ------------------------- global variables ---------------------------
CUST_JVM=""
LOG_DIR="${ADDAX_HOME}/log"
DEBUG=0
LOG_LEVEL="info"
JOB_FILE=""
LOG_FILE=""
TMPDIR=""

# ---------------------------- base function --------------------------
usage() {
    cat <<-EOF
Usage: $(basename "$0") [options] job-url-or-path

Options:
  -h, --help                  This help text
  -v, --version               Show version number and quit
  -j, --jvm <jvm parameters>  Set extra java jvm parameters if necessary.
  -p, --params <parameter>    Set job parameter, eg: the item 'tableName' which you want to specify by command,
                              you can use pass -p"-DtableName=your-table-name".
                              If you want multiple parameters, you can pass
                              -p"-DtableName=your-table-name -DcolumnName=your-column-name".
                              Note: you should configure tableName with \${tableName} in your job.
  -l, --logdir <log directory> The directory where logs are written to
  -d, --debug                 Set to remote debug mode.
  -L, --loglevel <log level>  Set log level such as: debug, info, warn, error, all etc.

Usage: $(basename "$0") gen [options]

  Generate job template file
Options:
  -r specify reader plugin name
  -w specify writer plugin name
  -l [r|w] list all reader/writer plugin names
EOF
    exit 1
}

# Clean up temporary directory on exit
cleanup() {
    if [ -n "$TMPDIR" ] && [ -d "$TMPDIR" ]; then
        rm -rf "$TMPDIR"
    fi
}

# Register the cleanup function to be called on exit
trap cleanup EXIT

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

parse_job_file() {
    # check the job file is local file or url?
    case "$JOB_FILE" in
        http*)
            # check curl command exists or not
            if ! command -v curl >/dev/null 2>&1; then
                echo "Error: curl command not found, cannot download job file"
                exit 1
            fi

            # download it first
            TMPDIR=$(mktemp -d /tmp/addax.XXXXXX)
            JOB_NAME=$(basename "${JOB_FILE}")
            if ! curl -sS -f -o "$TMPDIR/$JOB_NAME" "${JOB_FILE}" 2>/dev/null; then
                echo "Error: Download job file failed, check the URL: ${JOB_FILE}"
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

gen_log_file() {
    # Create log directory if it doesn't exist
    if [ ! -d "${LOG_DIR}" ]; then
        mkdir -p "${LOG_DIR}" || {
            echo "Error: Failed to create log directory ${LOG_DIR}"
            exit 1
        }
    fi

    # Check if LOG_DIR is writable
    if [ ! -w "${LOG_DIR}" ]; then
        echo "Error: Log directory ${LOG_DIR} is not writable"
        exit 1
    fi

    # Combine log file name
    job_name=$(basename "$JOB_FILE")
    job_escaped_name=$(echo "${job_name%\.*}" | tr '.' '_')
    curr_time=$(date +"%Y%m%d_%H%M%S")
    pid=$$
    LOG_FILE="addax_${job_escaped_name}_${curr_time}_${pid}.log"
}

# ---------------------------- generate job template file ---------------

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

# ------------------------------------ main -----------------------------

[ $# -eq 0 ] && usage

if [ "$1" = "gen" ]; then
    shift 1
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

    # Check if specified reader plugin exists
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

    # Check if specified writer plugin exists
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

    # Combine reader and writer plugin templates
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

    exit 0
fi

# OS detect and argument parsing
if command -v getopt >/dev/null 2>&1; then
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
    # Fallback for systems without getopt (like macOS)
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

# Check job file is provided
if [ $# -eq 0 ]; then
    echo "Error: Job file is required"
    usage
fi

JOB_FILE="${1}"
parse_job_file
gen_log_file

# Combine command
cmd="${ENGINE_COMMAND} ${CUST_JVM} ${PARAMS} -Dloglevel=${LOG_LEVEL} -Daddax.log=${LOG_DIR} -Dlog.file.name=${LOG_FILE}"

if [ ${DEBUG} -eq 1 ]; then
    cmd="${cmd} ${REMOTE_DEBUG_CONFIG}"
    echo "Debug mode enabled on port ${DEBUG_PORT}"
fi

# Attach main class
cmd="${cmd} com.wgzhao.addax.core.Engine -job ${JOB_FILE}"

# Execute the command
sh -c "${cmd}"
exit $?