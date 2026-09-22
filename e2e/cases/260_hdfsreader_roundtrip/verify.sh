# A parquet file and an orc file are binary formats this suite must not reimplement, so
# the round trip is closed instead: jobs 3 and 4 read them back with hdfsreader and land
# them as csv, which is compared byte for byte with the golden of that format. The two
# goldens hold the same values; the array and map columns are spelled with the separator
# of the library that renders each format, which is why there is one golden per format
# rather than one for both.
assert_file_count "$E2E_CASE_OUT/parquet" '*.parquet' 1 "hdfswriter PARQUET output"
assert_file_count "$E2E_CASE_OUT/orc" '*.orc' 1 "hdfswriter ORC output"
assert_file_matches "$E2E_CASE_OUT/txt/from_parquet.csv" "$CASE_DIR/expect.parquet.txt"
assert_file_matches "$E2E_CASE_OUT/txt/from_orc.csv" "$CASE_DIR/expect.orc.txt"

# Jobs 5 and 6 read the same two files with `column: ["*"]`, so the reader derives every
# type from the schema of the file rather than from the job configuration. The rows have
# to come out the same, which is what the explicit column lists above already blessed.
assert_files_equal "$E2E_CASE_OUT/txt/from_parquet_star.csv" "$E2E_CASE_OUT/txt/from_parquet.csv" \
    "the '*' column list must read the same rows as the explicit one (parquet)"
assert_files_equal "$E2E_CASE_OUT/txt/from_orc_star.csv" "$E2E_CASE_OUT/txt/from_orc.csv" \
    "the '*' column list must read the same rows as the explicit one (orc)"
