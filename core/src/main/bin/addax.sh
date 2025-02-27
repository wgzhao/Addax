#!/bin/sh
# Addax command line script
# Author wgzhao<wgzhao@gmail.com>
# Created at 2021-07-22

# ------------------------------ constant ----------------------------------------
SCRIPT_PATH="$(
    cd -- "$(dirname "$0")" >/dev/null 2>&1
    pwd -P
)"

ADDAX_HOME=$(dirname $SCRIPT_PATH)

DEBUG_PORT=9999
if [ -z "${ADDAX_HOME}" ]; then
    exit 2
fi

CLASS_PATH=".:/etc/hbase/conf:${ADDAX_HOME}/lib/*"
LOGBACK_FILE="${ADDAX_HOME}/conf/logback.xml"
DEFAULT_JVM="-Xms64m -Xmx2g -XX:+HeapDumpOnOutOfMemoryError -XX:+ExitOnOutOfMemoryError -XX:HeapDumpPath=${ADDAX_HOME}"
DEFAULT_PROPERTY_CONF="-Dfile.encoding=UTF-8 -Djava.security.egd=file:///dev/urandom -Daddax.home=${ADDAX_HOME} \
                        -Dlogback.configurationFile=${LOGBACK_FILE} "
ENGINE_COMMAND="java -server ${DEFAULT_JVM} ${DEFAULT_PROPERTY_CONF} -classpath ${CLASS_PATH}  "
REMOTE_DEBUG_CONFIG="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,address=0.0.0.0:${DEBUG_PORT}"

# ------------------------- global variables ---------------------------

CUST_JVM=""
LOG_DIR="${ADDAX_HOME}/log"
DEBUG=0
LOG_LEVEL="info"
JOB_FILE=
LOG_FILE=

# ---------------------------- base function --------------------------
usage() {
    cat <<-EOF
    Usage: $(basename $0) [options] job-url-or-path

    Options:
    -h, --help                  This help text
    -v, --version               Show version number and quit
    -j, --jvm <jvm parameters>  Set extra java jvm parameters if necessary.
    -p, --params <parameter>    Set job parameter, eg: the item 'tableName' which you want to specify by command,
                                you can use pass -p"-DtableName=your-table-name".
                                If you want to multiple parameters, you can pass
                                -p"-DtableName=your-table-name -DcolumnName=your-column-name".
                                Note: you should config in you job tableName with \${tableName}.
    -l, --logdir <log directory> the directory which log writes to
    -d, --debug                 Set to remote debug mode.
    -L, --loglevel <log level>  Set log level such as: debug, info, warn, error, all etc.


    Usage: $(basename $0) gen [options]

      generate job template file
    Options:
    -r specify reader plugin name
    -w specify writer plugin name
    -l [r|w] list all reader/writer plugin names
EOF
    exit 1
}

print_version() {
    echo -n -e "Addax version "
    core_jar=$(ls -w1 ${ADDAX_HOME}/lib/addax-core-*.jar)
    if [ -z "$core_jar" ]; then
        echo "Unknown"
    else
        echo $(basename ${core_jar%%\.jar}) | cut -c12-
    fi
    exit 0
}

parse_job_file() {
    # check the job file is local file or url ?
    case "$JOB_FILE" in
        http*)
        # check curl command is exists or not
        # if curl does not exists, the addax engine will download it first
        which curl >/dev/null 2>&1 || return
        # download it first
        TMPDIR=$(mktemp -d /tmp/addax.XXXXXX)
        JOB_NAME=$(basename ${JOB_FILE})
        curl -sS -f -o $TMPDIR/$JOB_NAME ${JOB_FILE} 2>/dev/null
        if [ $? -ne 0 ]; then
            echo "Download job file, failed check the url: ${JOB_FILE}"
            exit 1
        fi
        JOB_FILE=$(ls -w1 ${TMPDIR}/*)
        ;;
    esac

    if [ ! -f ${JOB_FILE} ]; then
        echo "The job file '${JOB_FILE}' does not exists"
        exit 2
    fi

    if [ ! -r ${JOB_FILE} ]; then
        echo "The job file '${JOB_FILE}' is unreadable"
        exit 3
    fi
    # end check job file
}

gen_log_file() {
    # ---------------- combine log file name
    job_name=$(basename $JOB_FILE)
    job_escaped_name=$(echo ${job_name%\.*} | tr '.' '_')
    curr_time=$(date +"%Y%m%d_%H%M%S")
    pid=$$
    LOG_FILE="addax_${job_escaped_name}_${curr_time}_${pid}.log"
    # --------------- end combine
}

# ---------------------------- generate job template file ---------------

sub_main() {
    shift
    if [ $# -lt 1 ]; then
        sub_help
    fi
    # ---------- parse gen options -------------
    echo "$*"
    while getopts 'r:w:l:h' option; do
        case "$option" in
        r) READER=${OPTARG} ;;
        w) WRITER=${OPTARG} ;;
        l) TYPE=${OPTARG} ;;
        *) sub_help ;;
        esac
    done

    if [ "x$READER" = "x" -o "x$WRITER" = "x" ]; then
        echo "-r/-w required a argument"
        exit 2
    fi
}

sub_help() {
    echo "Usage: $0 gen [options]"
    echo "Options:"
    echo -e "\t -r specify reader plugin name"
    echo -e "\t -w specify writer plugin name"
    echo -e "\t -l [r|w] list all reader/writer plugin names"
    echo -e "\t -h Print Usage and exit"
    exit 0
}

list_all_plugin_names() {
    echo  "Reader Plugins:"
    for i in plugin/reader/*
    do
        echo " $(basename ${i})"
    done
    echo
    echo "Reader Plugins:"
    for i in plugin/writer/*
    do
        echo  "  $(basename ${i})"
    done
}

# ------------------------------------ main -----------------------------

[ $# -eq 0 ] && usage

if [ "$1" = "gen" ]; then

    shift 1
    LIST_PLUGINS=0
    while getopts 'r:w:lh' option; do
        case "$option" in
        r) READER=${OPTARG} ;;
        w) WRITER=${OPTARG} ;;
        l) LIST_PLUGINS=1 ;;
        *) sub_help ;;
        esac
    done
    if [ ${LIST_PLUGINS} -eq 1 ]; then
        list_all_plugin_names
        exit 0
    fi

    if [ "x${READER}" = "x" -o "x${WRITER}" = "x" ]; then
        echo "-r/-w required a argument"
        exit 2
    fi
    # specified reader plugin is exists or not ?
    if [ ! -f plugin/reader/${READER}/plugin_job_template.json ]; then
        echo "Reader plugin ${READER} DOES NOT exists or has not installed yet "
        exit 3
    fi
    if [ ! -f plugin/writer/${WRITER}/plugin_job_template.json ]; then
        echo  "Reader plugin ${WRITER} DOES NOT exists or has not installed yet"
        exit 3
    fi
    # correct identified reader and writer plugin
    reader_content=$(cat plugin/reader/${READER}/plugin_job_template.json | sed 's/^/      /')
    writer_content=$(cat plugin/writer/${WRITER}/plugin_job_template.json | sed 's/^/      /')
    #combine reader and writer plugin
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

# OS detect
os=$(uname -s)
has_get_opt=$(which getopt 2>/dev/null)
if [ "${os}" = "Darwin" -o -z "${has_get_opt}" ]; then
    while getopts 'hj:p:l:vdL:' option; do
        case "$option" in
        h) usage ;;
        j) CUST_JVM=${OPTARG} ;;
        p) PARAMS=${OPTARG} ;;
        l) LOG_DIR=${OPTARG} ;;
        v) print_version ;;
        d) DEBUG=1 ;;
        L) LOG_LEVEL=${OPTARG} ;;
        ?) usage ;;
        esac
    done
    shift $((OPTIND - 1))
else
    PARSED_ARGUMENTS=$(getopt -a -n 'addax' -o hj:p:l:vL:d -l help,jvm:,params:,logdir:,version,loglevel:,debug -- "$@")
    if [ $? -ne 0 ]; then
        echo "Terminating...." >&2
        exit 65
    fi
    eval set -- "$PARSED_ARGUMENTS"

    while true; do
        case "$1" in
        -h | --help)
            usage
            ;;
        -j | --jvm)
            CUST_JVM="$2"
            shift 2
            ;;
        -p | --params)
            PARAMS="$2"
            shift 2
            ;;
        -l | --logdir)
            LOG_DIR="$2"
            shift 2
            ;;
        -d | --debug)
            DEBUG=1
            shift
            ;;
        -v | --version)
            print_version
            shift
            ;;
        -L | --loglevel)
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
fi

# check job file
if [ $# -eq 0 ]; then
    echo "The job file is required"
    usage
fi

JOB_FILE=${1}
parse_job_file
gen_log_file

# combine command
cmd="${ENGINE_COMMAND} ${CUST_JVM} ${PARAMS} -Dloglevel=${LOG_LEVEL} -Daddax.log=${LOG_DIR} -Dlog.file.name=${LOG_FILE}"

if [ ${DEBUG} -eq 1 ]; then
    cmd="${cmd} ${REMOTE_DEBUG_CONFIG}"
fi

# attach main class
cmd="${cmd} com.wgzhao.addax.core.Engine -job ${JOB_FILE} "

# run it
sh -c "${cmd}"
