# hdfswriter needs its target directory to exist, so the two directories it writes
# into are made here. The txt directory the two readers write into is made as well, so
# a failure there is about the read and not about a missing directory.
rm -rf "$E2E_CASE_OUT"
mkdir -p "$E2E_CASE_OUT/parquet" "$E2E_CASE_OUT/orc" "$E2E_CASE_OUT/txt"
