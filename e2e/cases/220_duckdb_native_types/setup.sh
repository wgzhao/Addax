# One row per DuckDB type that the shared reader cannot map correctly on its own:
# HUGEINT and DECIMAL(38,10) would lose precision through a double, BIT is rejected by
# both getBoolean and getBytes (which drops the whole row as a dirty record), and the
# nested types would stringify with java.util.Object semantics into "{a=1}".
addax_duckdb_sql "$E2E_CASE_OUT/typed.duckdb" \
    "CREATE TABLE typed (h HUGEINT, d DECIMAL(38,10), u UUID, j JSON, l INTEGER[], s STRUCT(a INTEGER, b VARCHAR), m MAP(VARCHAR, INTEGER), bit BIT, ts TIMESTAMP, tz TIMESTAMPTZ, dt DATE, t TIME)"
addax_duckdb_sql "$E2E_CASE_OUT/typed.duckdb" \
    "INSERT INTO typed VALUES (170141183460469231731687303715884105727, 12345678901234567.1234567890, '123e4567-e89b-12d3-a456-426614174000', '{\"k\":\"v\"}', [1,2,3], {'a':1,'b':'x'}, MAP{'p':1,'q':2}, '1010', TIMESTAMP '2024-01-02 03:04:05.123456', TIMESTAMPTZ '2024-01-02 11:04:05+08', DATE '2024-01-02', TIME '03:04:05')"
