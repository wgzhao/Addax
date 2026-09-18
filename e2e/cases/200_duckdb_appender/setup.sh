# DuckDB is embedded, so the destination database and its table are built here; the
# table has to exist before the job starts (see addax_duckdb_sql in lib/addax.sh).
# The table column order matches the writer's column list so the appender is eligible.
printf '1,alice,3,1.25,true\n2,bob,7,2.50,false\n3,carol,11,3.75,true\n' >"$E2E_CASE_OUT/in.csv"
addax_duckdb_sql "$E2E_CASE_OUT/e2e.duckdb" \
    "CREATE TABLE e2e_src (id BIGINT, name VARCHAR, qty INTEGER, price DECIMAL(10,2), active BOOLEAN)"
