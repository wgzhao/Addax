# S3 has no throwaway container in this suite: the objects come from moto, a python S3 test
# double. That makes moto an optional dependency, so the case asks to be skipped (exit 77)
# instead of failing a machine that has no use for it -- see the Requirements section of
# e2e/README.md.
#
#   E2E_MOTO_PYTHON  interpreter that can import moto   (default: python3)
#   E2E_MOTO_PORT    port the test double listens on    (default: 5111)
MOTO_PYTHON="${E2E_MOTO_PYTHON:-python3}"
MOTO_PORT="${E2E_MOTO_PORT:-5111}"
BUCKET="addax-e2e-s3"

if ! command -v "$MOTO_PYTHON" >/dev/null 2>&1 ||
        ! "$MOTO_PYTHON" -c 'import moto' >/dev/null 2>&1; then
    echo "220_s3reader_objects: $MOTO_PYTHON cannot import moto" >&2
    echo "  install it with: pip install 'moto[server]'" >&2
    echo "  or point E2E_MOTO_PYTHON at an interpreter that has it" >&2
    exit 77
fi

moto_listens() {
    "$MOTO_PYTHON" - "$MOTO_PORT" <<'PY' >/dev/null 2>&1
import socket, sys
sock = socket.socket()
sock.settimeout(1)
sys.exit(0 if sock.connect_ex(("127.0.0.1", int(sys.argv[1]))) == 0 else 1)
PY
}

if moto_listens; then
    echo "using the S3 test double already listening on port $MOTO_PORT"
else
    "$MOTO_PYTHON" -m moto.server -p "$MOTO_PORT" -H 127.0.0.1 >"$E2E_CASE_WORK/moto.log" 2>&1 &
    echo "$!" >"$E2E_CASE_WORK/moto.pid"
    for _ in $(seq 1 30); do
        moto_listens && break
        sleep 0.5
    done
    if ! moto_listens; then
        echo "the S3 test double did not come up on port $MOTO_PORT:" >&2
        tail -5 "$E2E_CASE_WORK/moto.log" >&2
        exit 1
    fi
    echo "started the S3 test double on port $MOTO_PORT (pid $(cat "$E2E_CASE_WORK/moto.pid"))"
fi

"$MOTO_PYTHON" - "$MOTO_PORT" "$BUCKET" <<'PY'
# The bucket of the case. Every object is there for a reason:
#
#   data/a.csv, data/b.csv  the objects `data/*.csv` names
#   data/foo1csv            no dot in the name; a dot in the pattern that is handed to the
#                           regular expression unquoted matches any character and reads it
#   data+old/c.csv          a plus sign in the pattern is a literal, not a quantifier
#   data/gz.csv.gz          gzip, to be decompressed without being told the algorithm
#   other/plain.csv         a plain object read in the same job as the gzip one
import gzip
import sys

import boto3

port, bucket = sys.argv[1], sys.argv[2]
s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:%s" % port,
                  aws_access_key_id="test", aws_secret_access_key="test", region_name="us-east-1")
try:
    s3.create_bucket(Bucket=bucket)
except Exception:
    pass

# Empty the bucket: a rerun has to read the objects of this run and nothing else.
while True:
    page = s3.list_objects_v2(Bucket=bucket)
    keys = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
    if not keys:
        break
    s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})

objects = {
    "data/a.csv": b"1,alice\n2,bob\n",
    "data/b.csv": b"3,carol\n",
    "data/foo1csv": b"9,zed\n",
    "data+old/c.csv": b"4,dan\n",
    "data/gz.csv.gz": gzip.compress(b"5,eve\n"),
    "other/plain.csv": b"6,frank\n",
}
for key, body in objects.items():
    s3.put_object(Bucket=bucket, Key=key, Body=body)
print("seeded %d objects in %s" % (len(objects), bucket))
PY
