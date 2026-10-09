#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

#
# addax-server.sh - start/stop the minimal Addax server (JDK HttpServer based)
# Usage:
#   addax-server.sh start [-p <parallel>] [--port <port>] [--bind <address>]
#                         [--max-tasks <n>] [--daemon]
#   addax-server.sh stop
#

set -euo pipefail

SCRIPT_PATH="$(
    cd -- "$(dirname "$0")" >/dev/null 2>&1
    pwd -P
)"
ADDAX_HOME=$(dirname "$SCRIPT_PATH")

PID_FILE="$ADDAX_HOME/addax-server.pid"
OUT_FILE="$ADDAX_HOME/addax-server.out"


start_server() {
  # the server reads this variable itself as well; here it only has to survive the explicit
  # -p below, which would otherwise override it
  PARALLEL="${ADDAX_SERVER_PARALLEL:-30}"
  PORT=10601
  DAEMON=0
  BIND=""
  MAX_TASKS=""
  EXTRA=()
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -p|--parallel)
        PARALLEL="$2"; shift 2;;
      --port)
        PORT="$2"; shift 2;;
      --bind)
        BIND="$2"; shift 2;;
      --max-tasks)
        MAX_TASKS="$2"; shift 2;;
      --daemon)
        DAEMON=1; shift;;
      *)
        # let the server validate and report anything it does not know either
        EXTRA+=("$1"); shift;;
    esac
  done

  # without logback.configurationFile the logback defaults are used, which is a debug console
  # dump; a long running server wants its log in a file under the addax home
  PROPS=(-Djava.security.manager=allow
         -Dfile.encoding=UTF-8
         -Djava.security.egd=file:///dev/urandom
         -Daddax.home="${ADDAX_HOME}"
         -Dlogback.configurationFile="${ADDAX_HOME}/conf/logback.xml"
         -Daddax.log="${ADDAX_HOME}/log"
         -Dlog.file.name=addax-server.log
         -Dloglevel="${LOG_LEVEL:-info}")

  # in daemon mode the log file is the only output worth keeping, so the console copy is
  # turned off to keep addax-server.out small
  if [[ $DAEMON -eq 1 ]]; then
    PROPS+=(-Dconsole.enabled=false)
  fi

  CMD=(java -server ${JAVA_OPTS:-} "${PROPS[@]}" -cp "lib/*" com.wgzhao.addax.server.AddaxServer --port "$PORT" -p "$PARALLEL")
  [[ -n "$BIND" ]] && CMD+=(--bind "$BIND")
  [[ -n "$MAX_TASKS" ]] && CMD+=(--max-tasks "$MAX_TASKS")
  CMD+=(${EXTRA[@]+"${EXTRA[@]}"})

  if [[ $DAEMON -eq 1 ]]; then
    nohup "${CMD[@]}" > "$OUT_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    echo "Addax server started in background (PID: $(cat $PID_FILE)). Logs: $OUT_FILE"
  else
    "${CMD[@]}"
  fi
}

stop_server() {
  if [[ -f "$PID_FILE" ]]; then
    PID=$(cat "$PID_FILE")
    if kill "$PID" >/dev/null 2>&1; then
      echo "Stopped process $PID"
      rm -f "$PID_FILE"
    else
      echo "Failed to stop process $PID (it may no longer be running)";
      rm -f "$PID_FILE" || true
    fi
  else
    echo "PID file not found: $PID_FILE. Server may not be running."
  fi
}

cd $ADDAX_HOME || exit 1

case "${1:-}" in
  start)
    shift || true
    start_server "$@"
    ;;
  stop)
    stop_server
    ;;
  *)
    echo "Usage: $0 {start [-p <parallel>] [--port <port>] [--bind <address>] [--max-tasks <n>] [--daemon] | stop}"
    exit 1
    ;;
esac
