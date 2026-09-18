# querySql support plus read volume. Note this does not by itself prove the result set is
# streamed rather than materialized -- that would need enough rows to exhaust the heap,
# which is not something a test suite should do. See the reader's jdbc_stream_results
# suffix in DataBaseType.
assert_eq "$(wc -l <"$E2E_CASE_OUT/big.csv" | tr -d ' ')" 200001 "rows written including the header"
assert_eq "$(sed -n '2p' "$E2E_CASE_OUT/big.csv")" "0,row-0,0.0" "first row"
assert_eq "$(tail -1 "$E2E_CASE_OUT/big.csv")" "199999,row-199999,299998.5" "last row"
