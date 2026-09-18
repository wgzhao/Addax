# The DuckDB destination is a file this case builds, so there is no fixture directory
# and no db_reset for it; only the MySQL source comes from the shared fixtures.
addax_duckdb_sql "$E2E_CASE_OUT/e2e.duckdb" \
    "CREATE TABLE e2e_dst (id BIGINT, name VARCHAR, qty INTEGER, price DECIMAL(10,2), active BOOLEAN, note VARCHAR)"
