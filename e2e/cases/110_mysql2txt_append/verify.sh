# job 1 truncates, job 2 appends to the same fileName. The golden therefore holds
# rows 1-3 followed by rows 4-6: a writer that ignores append, or that truncates
# on every run, cannot produce it.
assert_file_count "$E2E_CASE_OUT" 'append_test*' 1 "append output"
assert_dir_matches "$E2E_CASE_OUT" 'append_test*' "$CASE_DIR/expect.txt"
