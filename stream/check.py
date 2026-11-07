import duckdb

((rows, trips),) = duckdb.sql("select count(*), sum(trips) from 'lake/gold/zone_stream/*.parquet'").fetchall()
print(f"{rows} rows, {trips} trips (expected: 4174 rows, 98664 trips)")
