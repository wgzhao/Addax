# The parquet file is a binary format, so instead of asserting its bytes this case
# closes the loop: job 2 reads it back with hdfsreader and lands it as csv, which
# is then compared byte for byte with the golden.
assert_file_count "$E2E_CASE_OUT/parquet" '*.parquet' 1 "hdfswriter PARQUET output"
assert_file_matches "$E2E_CASE_OUT/txt/roundtrip.csv" "$CASE_DIR/expect.txt"
