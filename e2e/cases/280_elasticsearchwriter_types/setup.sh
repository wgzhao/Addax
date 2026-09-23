# The cluster is not part of this suite: it is passed in, and the case skips (exit 77)
# instead of failing when there is none to talk to -- see the Requirements section of
# e2e/README.md.
#
#   E2E_ES_ENDPOINT   the cluster to write to   (default: http://127.0.0.1:9200)
#
# The indices of the case are dropped before it runs, so the documents a run reads are the
# ones that run wrote.
ES_ENDPOINT="${E2E_ES_ENDPOINT:-http://127.0.0.1:9200}"

if ! command -v python3 >/dev/null 2>&1; then
    echo "elasticsearch case: python3 is not on PATH, cannot check the cluster" >&2
    exit 77
fi

# A cluster that cannot be reached is a missing dependency of this case, not a defect in
# the writer, so the case asks to be skipped.
if ! python3 - "$ES_ENDPOINT" <<'PY'
import sys, urllib.request
# Direct, like the job: a proxy configured on the machine would otherwise be picked up here
# and turn an unreachable cluster into a wrong answer.
urllib.request.install_opener(urllib.request.build_opener(urllib.request.ProxyHandler({})))
endpoint = sys.argv[1].rstrip("/")
try:
    urllib.request.urlopen(endpoint + "/", timeout=5).read()
except Exception as e:
    print("elasticsearch case: no elasticsearch at %s: %s" % (endpoint, e), file=sys.stderr)
    print("  point E2E_ES_ENDPOINT at a cluster to run this case", file=sys.stderr)
    sys.exit(1)
PY
then
    exit 77
fi

# job.02 reads a file with a null primary key: no reader of the suite produces one, and the
# point of the job is what the writer does with it.
printf 'row-1\tone\n\\N\ttwo\n\\N\tthree\nrow-4\tfour\n' >"$E2E_CASE_WORK/in.txt"

python3 - "$ES_ENDPOINT" <<'PY' || exit 1
import sys, urllib.error, urllib.request
urllib.request.install_opener(urllib.request.build_opener(urllib.request.ProxyHandler({})))
endpoint = sys.argv[1].rstrip("/")
for index in ("addax_e2e_esw_types", "addax_e2e_esw_generated", "addax_e2e_esw_dynamic"):
    req = urllib.request.Request(endpoint + "/" + index, method="DELETE")
    try:
        urllib.request.urlopen(req, timeout=10).read()
    except urllib.error.HTTPError as e:
        if e.code != 404:
            print("cannot drop %s: %s" % (index, e.read()[:200]), file=sys.stderr)
            sys.exit(1)
print("dropped the indices of the case")
PY
