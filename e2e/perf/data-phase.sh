#!/usr/bin/env bash
#
# Report the data phase of a run: the interval from the first reader starting
# to the last writer finishing. External wall time also carries JVM/Addax
# startup and the scheduler's completion poll (core.container.job.sleepInterval,
# 3s by default), which makes short runs look much slower and noisy.
#
#   ./e2e/perf/data-phase.sh e2e/perf/results/<run>
#   ./e2e/perf/data-phase.sh path/to/addax_*.log
#
# Prints one tab-separated line per log: label, data_phase_s, wait_reader_s,
# wait_writer_s. Wait times come from Addax's final summary.

set -euo pipefail

if [ $# -eq 0 ]; then
    echo "usage: $0 <results-dir|log>..." >&2
    exit 1
fi

logs=()
for arg in "$@"; do
    if [ -d "$arg" ]; then
        while IFS= read -r f; do logs+=("$f"); done < <(find "$arg" -type f -name 'addax_*.log' | sort)
    else
        logs+=("$arg")
    fi
done

python3 - "${logs[@]}" <<'PY'
import datetime
import os
import re
import sys

stamp = re.compile(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})')
begin = re.compile(r'Begin reading records')
finish = re.compile(r'(Finished reading records|Finish write)')
wait = re.compile(r'Total \d+ records.*All Task WaitWriterTime ([\d.]+)s.*All Task WaitReaderTime ([\d.]+)s')

for path in sys.argv[1:]:
    starts, ends, summary = [], [], None
    for line in open(path, errors='replace'):
        m = stamp.match(line)
        if not m:
            continue
        t = datetime.datetime.strptime(m.group(1), '%Y-%m-%d %H:%M:%S.%f')
        if begin.search(line):
            starts.append(t)
        elif finish.search(line):
            ends.append(t)
        w = wait.search(line)
        if w:
            summary = (w.group(1), w.group(2))
    label = os.path.basename(os.path.dirname(path))
    if starts and ends:
        data = (max(ends) - min(starts)).total_seconds()
        print(f'{label}\t{data:.3f}\t{summary[1] if summary else "-"}\t{summary[0] if summary else "-"}')
    else:
        print(f'{label}\tNA\t-\t-')
PY
