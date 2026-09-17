# hdfswriter always mints <fileName>_<timestamp>_<random>.<filetype>, even at
# channel 1, so the part file is matched by glob and the count is asserted rather
# than the name. The extension follows the lowercase fileType ("TEXT" -> .text).
#
# Hadoop also leaves a hidden .<name>.crc checksum sidecar next to the data file;
# the glob does not match it, which is why the count assertion is 1 and not 2.
assert_file_count "$E2E_CASE_OUT/hdfs_text" '*.text' 1 "hdfswriter TEXT output"
assert_dir_matches "$E2E_CASE_OUT/hdfs_text" '*.text' "$CASE_DIR/expect.txt"
