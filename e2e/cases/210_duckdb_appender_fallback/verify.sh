# the data must be correct even though the appender was refused
assert_file_matches "$E2E_CASE_OUT/fallback_out.csv" "$CASE_DIR/expect.txt"
assert_contains "$E2E_CASE_WORK/logs/job.01.console.log" \
    "do not match the physical order" "appender refused for the reordered column list"
