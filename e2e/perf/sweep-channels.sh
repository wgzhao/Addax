#!/usr/bin/env bash
#
# Channel-count sweep for one scenario. Each point is a --quick baseline run
# (one measured repetition, no warmup/JFR); use run-baseline.sh for the
# statistically useful A/B after the knee is known.
#
#   ./e2e/perf/sweep-channels.sh --case mysql_hdfs_text --channels "1 2 4 8 16"

set -euo pipefail

PERF_DIR="$(cd "$(dirname "$0")" && pwd -P)"
CASE="mysql_hdfs_text"
CHANNELS="1 2 4 8 16"
HOME_ARG=""
ROWS=""
HEAP=""

while [ $# -gt 0 ]; do
    case "$1" in
        --case) CASE="$2"; shift 2 ;;
        --channels) CHANNELS="$2"; shift 2 ;;
        --home) HOME_ARG="$2"; shift 2 ;;
        --rows) ROWS="$2"; shift 2 ;;
        --heap) HEAP="$2"; shift 2 ;;
        -h|--help)
            sed -n '2,10p' "$0"
            exit 0
            ;;
        *) echo "unknown option: $1" >&2; exit 1 ;;
    esac
done

for ch in $CHANNELS; do
    out="$PERF_DIR/results/channel-${ch}-${CASE}"
    args=(--quick --channel "$ch" --case "$CASE" --out "$out")
    [ -n "$HOME_ARG" ] && args+=(--home "$HOME_ARG")
    [ -n "$ROWS" ] && args+=(--rows "$ROWS")
    [ -n "$HEAP" ] && args+=(--heap "$HEAP")
    echo "=== channel $ch"
    "$PERF_DIR/run-baseline.sh" "${args[@]}"
done

echo
echo "results:"
for ch in $CHANNELS; do
    out="$PERF_DIR/results/channel-${ch}-${CASE}/baseline.md"
    [ -f "$out" ] && echo "  $out"
done
