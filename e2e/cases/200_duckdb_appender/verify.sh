assert_file_count "$E2E_CASE_OUT" 'duckdb_out.csv' 1 "txtfilewriter output"
assert_file_matches "$E2E_CASE_OUT/duckdb_out.csv" "$CASE_DIR/expect.txt"
# the appender is the whole point of this plugin; a silent fall back to the prepared
# statement would still produce correct data, so assert the path actually taken
assert_contains "$E2E_CASE_WORK/logs/job.01.console.log" "with the DuckDB appender" "appender used for the insert"
