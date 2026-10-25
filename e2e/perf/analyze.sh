#!/usr/bin/env bash
#
# Turn the JFR files from a baseline run into text reports.
#
#   ./e2e/perf/analyze.sh [results-dir]
#
# Defaults to the newest e2e/perf/results/*. The reports are written under
# <results-dir>/analysis/ and are intentionally plain text: they are evidence,
# not a golden file.

set -euo pipefail

PERF_DIR="$(cd "$(dirname "$0")" && pwd -P)"
RESULTS_ROOT="$PERF_DIR/results"

if [ $# -gt 0 ]; then
    DIR="$1"
else
    DIR="$(ls -d "$RESULTS_ROOT"/*/ 2>/dev/null | sort | tail -1)"
    DIR="${DIR%/}"
fi
[ -d "$DIR/jfr" ] || { echo "no jfr directory under $DIR" >&2; exit 1; }

if [ -x /usr/libexec/java_home ]; then
    JAVA_HOME="$(/usr/libexec/java_home 2>/dev/null || true)"
fi
if [ -z "${JAVA_HOME:-}" ]; then
    JAVA_BIN="$(command -v java)"
    JAVA_HOME="$(cd "$(dirname "$JAVA_BIN")/.." && pwd -P)"
fi
JFR="$JAVA_HOME/bin/jfr"
[ -x "$JFR" ] || { echo "jfr tool not found under $JAVA_HOME/bin" >&2; exit 1; }

OUT="$DIR/analysis"
mkdir -p "$OUT"

for f in "$DIR"/jfr/*.jfr; do
    [ -e "$f" ] || continue
    base="$(basename "$f" .jfr)"
    echo "analyzing $base"
    "$JFR" view --width 200 allocation-by-class "$f" >"$OUT/$base-allocation-by-class.txt" 2>&1 || true
    "$JFR" view --width 240 allocation-by-site "$f" >"$OUT/$base-allocation-by-site.txt" 2>&1 || true
    "$JFR" view --width 240 hot-methods "$f" >"$OUT/$base-hot-methods.txt" 2>&1 || true
    "$JFR" view gc-pauses "$f" >"$OUT/$base-gc-pauses.txt" 2>&1 || true
    "$JFR" view contention-by-site "$f" >"$OUT/$base-contention.txt" 2>&1 || true
done

echo "reports: $OUT"
