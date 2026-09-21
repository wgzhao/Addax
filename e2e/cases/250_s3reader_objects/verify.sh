# `data/*.csv` names two objects, and `data/foo1csv` is not one of them: the dot of the
# pattern is a literal, and a pattern read as a regular expression used to read that object
# as well.
assert_file_matches "$E2E_CASE_OUT/pattern.csv" "$CASE_DIR/expect.pattern.txt"

# A plus sign in the pattern is part of the object name, not a quantifier: the object used
# to be listed and then filtered out again, which failed the job with "is not found".
assert_file_matches "$E2E_CASE_OUT/metachar.csv" "$CASE_DIR/expect.metachar.txt"

# The gzip object is decompressed without the job naming the algorithm, and the plain object
# of the same job is read as it is.
assert_file_matches "$E2E_CASE_OUT/mixed.csv" "$CASE_DIR/expect.mixed.txt"
assert_contains "$E2E_CASE_WORK/logs/job.03.console.log" "read from its first bytes" \
    "the gzip object is detected from its own bytes"
