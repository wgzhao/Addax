assert_file_count "$E2E_CASE_OUT" 'e2e_src.csv' 1 "txtfilewriter output"
assert_file_matches "$E2E_CASE_OUT/e2e_src.csv" "$CASE_DIR/expect.txt"
