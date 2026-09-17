assert_file_count "$E2E_CASE_OUT" 'load*' 1 "sql format output"
assert_dir_matches "$E2E_CASE_OUT" 'load*' "$CASE_DIR/expect.txt"
