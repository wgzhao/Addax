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
#   addax-server.sh start [-p <parallel>] [--port <port>] [--daemon]
#   addax-server.sh stop
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR/../../.."
PID_FILE="$SCRIPT_DIR/addax-server.pid"
OUT_FILE="$SCRIPT_DIR/addax-server.out"

find_jar() {
  # pattern is a glob relative to project root, use find to locate it
  pattern="$1"
  find "$PROJECT_ROOT" -name "$pattern" -type f -print | head -n 1
}

start_server() {
  PARALLEL=30
  PORT=8080
  DAEMON=0
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -p|--parallel)
        PARALLEL="$2"; shift 2;;
      --port)
        PORT="$2"; shift 2;;
      --daemon)
        DAEMON=1; shift;;
      *)
        shift;;
    esac
  done

  # Try expected build output locations first
  SERVER_JAR="$PROJECT_ROOT/server/target/server-*.jar"
  CORE_JAR="$PROJECT_ROOT/core/target/addax-core-*.jar"

  # Resolve actual files (globs)
  SERVER_JAR_RESOLVED=$(ls $SERVER_JAR 2>/dev/null || true)
  CORE_JAR_RESOLVED=$(ls $CORE_JAR 2>/dev/null || true)

  # Fallback to find if not found
  if [[ -z "$SERVER_JAR_RESOLVED" ]]; then
    SERVER_JAR_RESOLVED=$(find_jar "server-*.jar")
  fi
  if [[ -z "$CORE_JAR_RESOLVED" ]]; then
    CORE_JAR_RESOLVED=$(find_jar "addax-core-*.jar")
  fi

  if [[ -z "$SERVER_JAR_RESOLVED" || -z "$CORE_JAR_RESOLVED" ]]; then
    echo "Error: cannot find server or core jars. Build the project first: mvn -DskipTests clean package -pl :server,:core -am"
    exit 1
  fi

  CMD=(java -cp "$SERVER_JAR_RESOLVED:$CORE_JAR_RESOLVED" com.wgzhao.addax.server.ServerApplication --port "$PORT" -p "$PARALLEL")

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

case "${1:-}" in
  start)
    shift || true
    start_server "$@"
    ;;
  stop)
    stop_server
    ;;
  *)
    echo "Usage: $0 {start [-p <parallel>] [--port <port>] [--daemon] | stop}"
    exit 1
    ;;
esac
