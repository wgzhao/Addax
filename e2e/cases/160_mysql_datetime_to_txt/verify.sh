# This case pins the file-rendering path, which is a different code path from the
# RDBMS one and has its own trap: a writer-level dateFormat formats every date/time
# column through one SimpleDateFormat, so DATE, TIME and TIMESTAMP all render with
# the same pattern -- including the TIME column, whose value is millis-of-day and so
# comes out anchored at the epoch. The golden records that honestly.
assert_file_count "$E2E_CASE_OUT" 'e2e_src*' 1 "datetime text output"
assert_dir_matches "$E2E_CASE_OUT" 'e2e_src*' "$CASE_DIR/expect.txt"
