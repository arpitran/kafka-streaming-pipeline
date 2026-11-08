"""Send fake food-order events to the `orders` topic, one per second.

Usage: uv run apps/order_producer.py [count]   (default 10)
"""
import json, random, sys, time, uuid
from kafka import KafkaProducer

USERS = ["arpit", "nicole", "lara", "sam"]
ITEMS = ["mushroom pizza", "chicken bowl", "frozen yogurt", "pad thai"]

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    acks="all",  # wait until the broker has stored the event
    key_serializer=str.encode,
    value_serializer=lambda v: json.dumps(v).encode(),
)

count = int(sys.argv[1]) if len(sys.argv) > 1 else 10
try:
    for _ in range(count):
        user = random.choice(USERS)
        order = {"order_id": str(uuid.uuid4()), "user": user,
                 "item": random.choice(ITEMS), "quantity": random.randint(1, 3)}
        # Key = user: all of one user's orders land in the same partition, in order.
        meta = producer.send("orders", key=user, value=order).get(timeout=10)
        print(f"sent {order['quantity']} x {order['item']} for {user} -> partition {meta.partition}, offset {meta.offset}")
        time.sleep(1)
except KeyboardInterrupt:
    pass
finally:
    producer.flush()
    producer.close()
