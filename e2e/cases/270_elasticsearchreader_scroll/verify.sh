# What the two jobs are meant to show.
#
# job.01 reads every document of the index through a scroll, with no size in the search
# body: the page size then comes from batchSize (4), and without it elasticsearch would
# answer with 10 documents per page -- which is not wrong, only one round trip per 10
# documents instead of per 4. The documents carry the values that readers get wrong: the
# Integer bounds, a negative long below Integer.MIN_VALUE (truncated by an int cast until
# it was fixed), an integer beyond the range of a long (which failed the whole read), an
# empty string, an empty array, a missing object, a quote and a backslash, Long.MAX_VALUE.
#
# The scroll hands the documents over in the order elasticsearch stores them, so the
# goldens are sorted by id: what is compared is the content, not the order.
sort "$E2E_CASE_OUT/all.csv" >"$E2E_CASE_WORK/actual/all.sorted.csv"
assert_file_matches "$E2E_CASE_WORK/actual/all.sorted.csv" "$CASE_DIR/expect.all.txt"

# 21 documents are in the index and 20 are read: id 7 carries none of the requested
# columns, so there is nothing to convert and no row is written for it. That is worth
# pinning down, it is the difference between a row of empty columns and no row at all.
assert_contains "$E2E_CASE_WORK/logs/job.01.console.log" "read finished: 20 records" \
    "every document that has at least one requested column is read once"
assert_contains "$E2E_CASE_WORK/logs/job.01.console.log" "using scroll page size batchSize=4" \
    "a scroll without a size in the search body pages with batchSize"

# Every document exactly once: a duplicated or dropped page would show up as a repeated
# line, and comparing a sorted copy with a sorted unique copy only works because no value
# of this fixture contains a newline.
assert_eq "$(sort -u "$E2E_CASE_OUT/all.csv" | wc -l | tr -d ' ')" \
    "$(wc -l <"$E2E_CASE_OUT/all.csv" | tr -d ' ')" \
    "each document is read exactly once (distinct rows == rows)"

# job.02 filters on two of the columns and sets the page size in the body, which wins
# over batchSize. It selects the documents with a positive qty that are active: id 4 and
# the even-numbered filler documents.
sort "$E2E_CASE_OUT/filtered.csv" >"$E2E_CASE_WORK/actual/filtered.sorted.csv"
assert_file_matches "$E2E_CASE_WORK/actual/filtered.sorted.csv" "$CASE_DIR/expect.filtered.txt"
assert_contains "$E2E_CASE_WORK/logs/job.02.console.log" "read finished: 8 records" \
    "the filter keeps only the documents it selects"
