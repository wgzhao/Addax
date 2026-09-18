# Reviewed against 060_mysql2txt_csv, which reads the same fixture into a text file: the
# only difference is the active column, 1/0 there and true/false here, because the target
# is a real BOOLEAN. The price column keeps its DECIMAL(10,2) scale (1.50, not 1.5) and
# row 5 keeps an empty note distinct from the NULL on row 2.
assert_file_matches "$E2E_CASE_OUT/duckdb_out.csv" "$CASE_DIR/expect.txt"
assert_contains "$E2E_CASE_WORK/logs/job.01.console.log" "with the DuckDB appender" "appender used for the insert"
