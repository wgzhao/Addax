# Both runs write the same path with the same fileName, so after the second job the
# directory must hold exactly one part file holding only the second run's rows.
# A writer that appends, or that leaves the first file behind, fails both assertions.
assert_file_count "$E2E_CASE_OUT/over" '*.text' 1 "overwrite must leave one file"
assert_dir_matches "$E2E_CASE_OUT/over" '*.text' "$CASE_DIR/expect.txt"
