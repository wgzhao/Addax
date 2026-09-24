# The cluster is not part of this suite: it is passed in, and the case skips (exit 77)
# instead of failing when there is none to talk to -- see the Requirements section of
# e2e/README.md.
#
#   E2E_ES_ENDPOINT   the cluster to read from   (default: http://127.0.0.1:9200)
#
# The index is dropped and rebuilt on every run, so the documents of this run are the
# only ones in it. python3 with nothing but the standard library seeds it.
ES_ENDPOINT="${E2E_ES_ENDPOINT:-http://127.0.0.1:9200}"

if ! command -v python3 >/dev/null 2>&1; then
    echo "elasticsearch case: python3 is not on PATH, cannot seed the index" >&2
    exit 77
fi

# A cluster that cannot be reached is a missing dependency of this case, not a defect in
# the reader, so the case asks to be skipped.
if ! python3 - "$ES_ENDPOINT" <<'PY'
import sys, urllib.request
# Direct, like the job: a proxy configured on the machine (macOS system settings, for one)
# would otherwise be picked up here and turn an unreachable cluster into a wrong answer.
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

python3 - "$ES_ENDPOINT" <<'PY' || exit 1
# The documents to read. Every one of them is here for a reason:
#
#   id 1  a negative long below Integer.MIN_VALUE: it used to be truncated by a cast to
#         int on its way into a record (-3000000000 came out as 1294967296)
#   id 2  both Integer bounds, a padded CJK value, a comma and a quote inside an array,
#         and a zero with a sign
#   id 3  an empty string (which is not null), an empty array, a missing object
#   id 4  a quote and a backslash, Long.MAX_VALUE, a float with digits behind it
#   id 5  every requested field null, but the document itself is not: it is read
#   id 6  an integer-valued number beyond the range of a long: it used to fail the whole
#         read with a NumberFormatException
#   id 7  none of the requested fields: nothing can be converted, so it is skipped
#   id 8+ filler, so the scroll needs more than one page
import json, sys, urllib.error, urllib.request

urllib.request.install_opener(urllib.request.build_opener(urllib.request.ProxyHandler({})))

endpoint = sys.argv[1].rstrip("/")
index = "addax_e2e_es"

def request(method, path, body=None, ctype="application/json"):
    data = body.encode() if isinstance(body, str) else body
    req = urllib.request.Request(endpoint + path, data=data, method=method,
                                 headers={"Content-Type": ctype})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()

request("DELETE", "/" + index)
status, body = request("PUT", "/" + index, json.dumps({
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
    "mappings": {"properties": {
        "id": {"type": "long"},
        "name": {"type": "keyword"},
        "qty": {"type": "integer"},
        "price": {"type": "double"},
        "active": {"type": "boolean"},
        "tags": {"type": "keyword"},
        "big_neg": {"type": "long"},
        "big_int": {"type": "double"},
        "meta": {"properties": {"origin": {"type": "keyword"}}},
        "note": {"type": "keyword"},
    }},
}))
if status != 200:
    print("cannot create %s: %s %s" % (index, status, body), file=sys.stderr)
    sys.exit(1)

docs = {
    1: dict(id=1, name="alice", qty=0, price=1.5, active=True, tags=["a", "b"],
            big_neg=-3000000000, meta={"origin": "beijing"}),
    2: dict(id=2, name=" 空间 ", qty=2147483647, price=-0.0, active=False,
            tags=["中文", "a,b"], big_neg=2147483648, meta={"origin": "shanghai"}),
    3: dict(id=3, name="", qty=-2147483648, price=0.0, active=True, tags=[],
            big_neg=-2147483648),
    4: dict(id=4, name='quote"and\\back', qty=1, price=5904.744, active=True, tags=["x"],
            big_neg=9223372036854775807, meta={"origin": "c,d"}),
    5: dict(id=5, name="null-fields"),
    6: dict(id=6, name="beyond-long", big_int=123456789012345678901234567890),
    7: dict(note="only this one"),
}
for i in range(8, 22):
    docs[i] = dict(id=i, name="filler-%02d" % i, qty=i, price=i + 0.5, active=i % 2 == 0,
                   tags=["t%d" % i], big_neg=-i)

payload = "".join(
    json.dumps({"index": {"_index": index, "_id": doc_id}}) + "\n" + json.dumps(doc) + "\n"
    for doc_id, doc in docs.items())
status, body = request("POST", "/_bulk?refresh=true", payload, ctype="application/x-ndjson")
if status != 200 or '"errors":true' in body:
    print("bulk load failed: %s %s" % (status, body[:400]), file=sys.stderr)
    sys.exit(1)
print("seeded %d documents into %s" % (len(docs), index))
PY
