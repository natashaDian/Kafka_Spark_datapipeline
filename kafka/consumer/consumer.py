from kafka import KafkaConsumer
import json
from collections import defaultdict

#define customer 
consumer = KafkaConsumer(
    "events_topic",
    bootstrap_servers=['localhost:9092'],
    group_id="events_group",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    key_deserializer=lambda k : k.decode("utf-8"),
    value_deserializer=lambda v : json.loads(v.decode("utf-8"))
)

#mencatat jumlah event per user
event_per_user = defaultdict(int)
total_event = 0

print("cunsumer started...")

for message in consumer:
    key = message.key
    value = message.value

    total_event += 1
    event_per_user[key] += 1

    print(
        f"Partition:{message.partition} | "
        f"Key:{key} | "
        f"Value:{value} | "
        f"Total event user ini:{event_per_user[key]} |"
        f"Total event keseluruhan:{total_event}"
    )