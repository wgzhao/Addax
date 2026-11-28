# The reader asks for the deprecated com.mysql.jdbc.Driver shim. Connector/J 8.x
# still ships that class, and its static initializer prints one deprecation line to
# stderr. Each plugin is loaded by its own classloader, so the line appears once per
# plugin that actually loads the shim.
#
# The writer configures no driver. If it picks up the reader's choice through shared
# state, its classloader loads the shim too and the line appears twice (or the job
# fails outright when the shim class is missing). Exactly one line therefore proves
# the driver did not cross the plugin boundary.
console="$E2E_CASE_WORK/logs/job.console.log"
[ -f "$console" ] || die "missing job console log: $console"
assert_eq "$(grep -c 'Loading class .com.mysql.jdbc.Driver' "$console" || true)" "1" \
    "the writer must not inherit the reader's driver class"

# The job must have moved the data, not merely survived: preSql removed the two
# stale fixture rows, so count and sums have to match the source exactly.
src_sum="$(db_query mysql "SELECT CONCAT_WS('|', COUNT(*), SUM(id), SUM(qty), SUM(price), SUM(active)) FROM e2e_src")"
dst_sum="$(db_query mysql "SELECT CONCAT_WS('|', COUNT(*), SUM(id), SUM(qty), SUM(price), SUM(active)) FROM e2e_dst")"
assert_eq "$dst_sum" "$src_sum" "all source rows reached the target after the preSql delete"
