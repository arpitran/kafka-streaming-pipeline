"""Read order events from the `orders` topic and print them. Runs until Ctrl-C.

Usage: uv run apps/order_consumer.py
Start it in two Terminal windows to see the group share the 3 partitions.
"""
import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "orders",
    bootstrap_servers="localhost:9092",
    group_id="order-tracker",      # instances with the same group split the partitions
    auto_offset_reset="earliest",  # only used when the group has no committed offset yet
)

print("consumer running, waiting for orders...")
try:
    for msg in consumer:  # polls the broker; offsets are auto-committed every 5 s
        o = json.loads(msg.value)
        print(f"partition {msg.partition}, offset {msg.offset}: {o['user']} ordered {o['quantity']} x {o['item']}")
except KeyboardInterrupt:
    print("stopping consumer")
finally:
    consumer.close()  # commits final offsets and leaves the group cleanly
