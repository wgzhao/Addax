# What the three jobs are meant to show.
#
# job.01 writes one document of every field type the writer supports, with the values that
# are easy to get wrong: a long beyond the precision of a double (9007199254740993), the
# Integer bounds, a byte, a keyword with CJK and a comma in it, an array, an object, a
# nested object, a geo point, a geo shape, a date column and a date carried as text with a
# format of its own. The index is created from the job's mappings, and the documents are
# read back through them.
#
# The values are read back with python, not with the reader plugin: what is being checked
# is what the writer put in the cluster, and the check should not be able to agree with a
# mistake the reader would make as well.
#
# job.02 writes two documents without a primary key value next to two with one.
# elasticsearch generates an id for those, so nothing is lost; a null primary key used to
# be written as the literal id "null", which put both of them on that one id.
#
# job.03 runs with `dynamic: true`: the writer sends no mappings and elasticsearch maps the
# two fields itself.

ES_ENDPOINT="${E2E_ES_ENDPOINT:-http://127.0.0.1:9200}"
mkdir -p "$E2E_CASE_WORK/actual"

python3 - "$ES_ENDPOINT" "$E2E_CASE_WORK/actual" <<'PY'
import json, sys, urllib.request

urllib.request.install_opener(urllib.request.build_opener(urllib.request.ProxyHandler({})))
endpoint, out = sys.argv[1].rstrip("/"), sys.argv[2]


def get(path):
    with urllib.request.urlopen(endpoint + path, timeout=30) as resp:
        return json.loads(resp.read().decode())


def fail(message):
    print("ASSERT FAILED: " + message, file=sys.stderr)
    sys.exit(1)


def search(index):
    return get("/%s/_search?size=100" % index)["hits"]["hits"]


def value_of(doc, field):
    if field == "_id":  # the primary key column of the job is the document id, not a field
        return doc["_id"]
    value = doc["_source"].get(field)
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    return "<null>" if value is None else str(value)


def project(index, fields, target):
    """One line per document, fields in the order given, nulls as <null>, sorted."""
    lines = ["|".join(value_of(doc, field) for field in fields) for doc in search(index)]
    with open(target, "w") as fh:
        fh.write("".join(line + "\n" for line in sorted(lines)))


project("addax_e2e_esw_types",
        ["_id", "col_keyword", "col_text", "col_long", "col_integer", "col_short", "col_byte",
         "col_double", "col_float", "col_boolean", "col_date", "col_date_str", "col_int_array",
         "col_object", "col_nested", "col_geo_point", "col_geo_shape"],
        out + "/types.txt")
project("addax_e2e_esw_generated", ["value"], out + "/generated.txt")

mapping = get("/addax_e2e_esw_types/_mapping")["addax_e2e_esw_types"]["mappings"]
properties = mapping.get("properties")
if properties is None:
    fail("the mapping of addax_e2e_esw_types carries no properties: %s" % json.dumps(mapping)[:200])
# elasticsearch 7.0 removed the mapping type: the properties sit at the top level
if "addax_e2e_esw_types" in mapping:
    fail("the mapping is wrapped in a type: %s" % json.dumps(mapping)[:200])
for field, expected in (("col_byte", "byte"), ("col_long", "long"), ("col_integer", "integer"),
                        ("col_short", "short"), ("col_double", "double"), ("col_float", "float"),
                        ("col_boolean", "boolean"), ("col_date", "date"), ("col_date_str", "date"),
                        ("col_keyword", "keyword"), ("col_text", "text"),
                        ("col_nested", "nested"), ("col_geo_point", "geo_point"),
                        ("col_geo_shape", "geo_shape"), ("col_int_array", "integer")):
    actual = properties.get(field, {}).get("type")
    if actual != expected:
        fail("the mapping of %s is [%s], expected [%s]" % (field, actual, expected))
# a field of type object is reported through its properties, and elasticsearch leaves the
# default type out: what has to be there is the object itself
if properties.get("col_object", {}).get("type", "object") != "object":
    fail("the mapping of col_object is not an object: %s" % json.dumps(properties.get("col_object")))
# index_options is a string in elasticsearch (docs, freqs, positions, offsets); read as a
# boolean it was sent as false, the whole mapping was rejected and the job went on to write
# into an index elasticsearch mapped on its own
if properties["col_text"].get("index_options") != "offsets":
    fail("the mapping of col_text has index_options [%s], expected [offsets]"
         % properties["col_text"].get("index_options"))
# the quadtree parameters were removed in elasticsearch 6, the writer must not send them
if "precision" in properties["col_geo_shape"] or "tree" in properties["col_geo_shape"]:
    fail("the geo_shape mapping still carries tree or precision: %s" % json.dumps(properties["col_geo_shape"]))

aliases = get("/_cat/aliases/addax_e2e_types_alias?format=json")
if not any(a["index"] == "addax_e2e_esw_types" for a in aliases):
    fail("the alias does not point at addax_e2e_esw_types: %s" % aliases)

documents = search("addax_e2e_esw_generated")
if len(documents) != 4:
    fail("%d documents of the four are in addax_e2e_esw_generated: two records without a "
         "primary key value share the id elasticsearch generates for them" % len(documents))
by_value = {doc["_source"]["value"]: doc["_id"] for doc in documents}
if by_value.get("one") != "row-1" or by_value.get("four") != "row-4":
    fail("the primary key column did not become the document id: %s" % by_value)
generated = [by_value[v] for v in ("two", "three")]
if len(set(generated)) != 2 or None in generated:
    fail("the two records without a primary key value did not each get an id: %s" % generated)

# job.03: elasticsearch mapped the two fields itself, which is what dynamic asks for
dynamic = get("/addax_e2e_esw_dynamic/_mapping")["addax_e2e_esw_dynamic"]["mappings"]["properties"]
if (dynamic["k"]["type"] != "text" or "keyword" not in dynamic["k"].get("fields", {})
        or dynamic["n"]["type"] != "long"):
    fail("the dynamic mapping is not elasticsearch's own: %s" % json.dumps(dynamic))
PY

assert_file_matches "$E2E_CASE_WORK/actual/types.txt" "$CASE_DIR/expect.types.txt"
assert_file_matches "$E2E_CASE_WORK/actual/generated.txt" "$CASE_DIR/expect.generated.txt"

assert_contains "$E2E_CASE_WORK/logs/job.02.console.log" "is empty for a record, elasticsearch generates the id" \
    "a record without a primary key value is reported"
assert_contains "$E2E_CASE_WORK/logs/job.01.console.log" "tree and precision are not supported" \
    "the quadtree parameters of a geo_shape column are reported as ignored"
