import json, random, duckdb
from kafka import KafkaProducer

p = KafkaProducer(bootstrap_servers="localhost:9092",
                  value_serializer=lambda v: json.dumps(v, default=str).encode())
rows = duckdb.sql("""select PULocationID, tpep_pickup_datetime, fare_amount
                     from 'lake/silver/trips/*/*.parquet' where tpep_pickup_datetime >= '2024-01-01' and tpep_pickup_datetime < '2024-02-01' limit 100000""").fetchall()
for i, (z, ts, fare) in enumerate(rows):
    msg = {"id": i, "zone": z, "ts": ts, "fare": fare}
    p.send("trips", key=str(i).encode(), value=msg)
    if random.random() < 0.02:  # deliberate duplicate
        p.send("trips", key=str(i).encode(), value=msg)
p.flush()
