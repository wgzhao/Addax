assert_file_count "$E2E_CASE_OUT/orc" '*.orc' 1 "hdfswriter ORC output"
assert_file_matches "$E2E_CASE_OUT/txt/roundtrip.csv" "$CASE_DIR/expect.txt"
