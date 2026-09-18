# The writer columns are listed in a different order than the table declares them. The
# appender always writes every column in physical order, so it must refuse to run here
# rather than silently shifting values into neighbouring columns.
# Source is name,id,qty,price,active while the table is (id,name,qty,price,active).
printf 'alice,1,3,1.25,true\nbob,2,7,2.50,false\n' >"$E2E_CASE_OUT/in.csv"
addax_duckdb_sql "$E2E_CASE_OUT/e2e.duckdb" \
    "CREATE TABLE e2e_src (id BIGINT, name VARCHAR, qty INTEGER, price DECIMAL(10,2), active BOOLEAN)"
